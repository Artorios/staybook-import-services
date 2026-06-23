package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// ---------------------------------------------------------------------------
// Конфиг
// ---------------------------------------------------------------------------

const defaultMaxLineSize = 16 * 1024 * 1024 // 16 МБ

type Config struct {
	DatabaseURL string
	DumpsDir    string
	BatchSize   int
	MaxLineSize int
}

func configFromEnv() Config {
	batchSize := 500
	if v := os.Getenv("BATCH_SIZE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			batchSize = n
		}
	}
	maxLineSize := defaultMaxLineSize
	if v := os.Getenv("MAX_LINE_SIZE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			maxLineSize = n
		}
	}
	return Config{
		DatabaseURL: mustEnv("DATABASE_URL"),
		DumpsDir:    mustEnv("DUMPS_DIR"),
		BatchSize:   batchSize,
		MaxLineSize: maxLineSize,
	}
}

// ---------------------------------------------------------------------------
// Модель отеля (только поля, нужные для индексов / быстрого доступа)
// Полный JSON сохраняется as-is в поле data
// ---------------------------------------------------------------------------

type Hotel struct {
	HID     int64           `json:"hid"`
	ID      string          `json:"id"`
	Name    string          `json:"name"`
	Lat     float64         `json:"latitude"`
	Lon     float64         `json:"longitude"`
	Country string          `json:"-"` // извлекается из region
	Deleted bool            `json:"deleted"`
	Region  json.RawMessage `json:"region"`
	Raw     json.RawMessage // весь объект целиком
}

type region struct {
	CountryCode string `json:"country_code"`
}

// ---------------------------------------------------------------------------
// Статистика импорта
// ---------------------------------------------------------------------------

type Stats struct {
	Total   int
	Upsert  int
	Deleted int
	Errors  int
}

func (s Stats) String() string {
	return fmt.Sprintf("total=%d upsert=%d soft_deleted=%d errors=%d",
		s.Total, s.Upsert, s.Deleted, s.Errors)
}

// ---------------------------------------------------------------------------
// Импортер
// ---------------------------------------------------------------------------

type Importer struct {
	pool        *pgxpool.Pool
	batchSize   int
	maxLineSize int
}

func NewImporter(pool *pgxpool.Pool, batchSize, maxLineSize int) *Importer {
	return &Importer{pool: pool, batchSize: batchSize, maxLineSize: maxLineSize}
}

const maxErrorMsgsInDB = 20

// Import читает NDJSON-файл построчно и делает батч-upsert.
// importRunID — ID записи в таблице import_runs.
// progress — опциональный индикатор; nil отключает вывод.
func (imp *Importer) Import(ctx context.Context, r io.Reader, importRunID int64, progress *importProgress) (Stats, []string, error) {
	var stats Stats
	var lineErrors []string
	batch := make([]Hotel, 0, imp.batchSize)
	var batchStartOffset int64

	counter := &byteCounter{r: r}
	reader := bufio.NewReaderSize(counter, imp.maxLineSize)

	for {
		lineOffset := counter.offset
		line, err := reader.ReadSlice('\n')
		if errors.Is(err, bufio.ErrBufferFull) {
			msg := fmt.Sprintf("offset %d: line exceeds %d bytes, skipped", lineOffset, imp.maxLineSize)
			slog.Warn("line too long, skipping", "offset", lineOffset, "max", imp.maxLineSize)
			lineErrors = append(lineErrors, msg)
			stats.Errors++
			if _, discardErr := reader.ReadBytes('\n'); discardErr != nil && !errors.Is(discardErr, io.EOF) {
				return stats, lineErrors, fmt.Errorf("discard oversized line at offset %d: %w", lineOffset, discardErr)
			}
			continue
		}
		if errors.Is(err, io.EOF) {
			if len(line) == 0 {
				break
			}
		} else if err != nil {
			return stats, lineErrors, fmt.Errorf("read at file offset %d: %w", lineOffset, err)
		}

		line = bytes.TrimSuffix(line, []byte{'\n'})
		if len(line) == 0 {
			continue
		}

		var h Hotel
		if err := json.Unmarshal(line, &h); err != nil {
			msg := fmt.Sprintf("offset %d: %v", lineOffset, err)
			slog.Warn("failed to parse line", "offset", lineOffset, "err", err)
			lineErrors = append(lineErrors, msg)
			stats.Errors++
			continue
		}
		h.Raw = bytes.Clone(line)

		// извлекаем country_code из region
		if len(h.Region) > 0 {
			var reg region
			_ = json.Unmarshal(h.Region, &reg)
			h.Country = reg.CountryCode
		}

		if len(batch) == 0 {
			batchStartOffset = lineOffset
		}
		batch = append(batch, h)
		stats.Total++

		if len(batch) >= imp.batchSize {
			n, err := imp.upsertBatch(ctx, batch, importRunID)
			if err != nil {
				return stats, lineErrors, fmt.Errorf("upsert batch at file offset %d: %w", batchStartOffset, err)
			}
			stats.Upsert += n
			batch = batch[:0]

			if progress != nil {
				progress.update(counter.offset, stats)
			}
		}
	}

	// последний неполный батч
	if len(batch) > 0 {
		n, err := imp.upsertBatch(ctx, batch, importRunID)
		if err != nil {
			return stats, lineErrors, fmt.Errorf("upsert final batch at file offset %d: %w", batchStartOffset, err)
		}
		stats.Upsert += n
	}

	if progress != nil {
		progress.finish(counter.offset, stats)
	}

	return stats, lineErrors, nil
}

// upsertBatch вставляет/обновляет батч отелей через pgx CopyFrom + ON CONFLICT.
// Возвращает количество затронутых строк.
func (imp *Importer) upsertBatch(ctx context.Context, hotels []Hotel, importRunID int64) (int, error) {
	// Используем временную таблицу + INSERT ... ON CONFLICT для атомарности
	tx, err := imp.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	// Создаём temp-таблицу для батча
	_, err = tx.Exec(ctx, `
		CREATE TEMP TABLE hotels_import_batch (
			external_id  BIGINT,
			slug         TEXT,
			name         TEXT,
			latitude     DOUBLE PRECISION,
			longitude    DOUBLE PRECISION,
			country_code TEXT,
			deleted      BOOLEAN,
			data         JSONB,
			import_run_id BIGINT
		) ON COMMIT DROP
	`)
	if err != nil {
		return 0, fmt.Errorf("create temp table: %w", err)
	}

	// Быстрая вставка в temp через CopyFrom
	rows := make([][]any, len(hotels))
	for i, h := range hotels {
		rows[i] = []any{
			h.HID, h.ID, h.Name, h.Lat, h.Lon,
			h.Country, h.Deleted, string(h.Raw), importRunID,
		}
	}

	_, err = tx.CopyFrom(ctx,
		pgx.Identifier{"hotels_import_batch"},
		[]string{"external_id", "slug", "name", "latitude", "longitude",
			"country_code", "deleted", "data", "import_run_id"},
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		return 0, fmt.Errorf("copy from: %w", err)
	}

	// Upsert из temp → основная таблица
	tag, err := tx.Exec(ctx, `
		INSERT INTO hotels (external_id, slug, name, latitude, longitude, country_code, deleted_at, data, updated_at)
		SELECT
			b.external_id,
			b.slug,
			b.name,
			b.latitude,
			b.longitude,
			b.country_code,
			CASE WHEN b.deleted THEN NOW() ELSE NULL END,
			b.data,
			NOW()
		FROM hotels_import_batch b
		ON CONFLICT (external_id) DO UPDATE SET
			slug         = EXCLUDED.slug,
			name         = EXCLUDED.name,
			latitude     = EXCLUDED.latitude,
			longitude    = EXCLUDED.longitude,
			country_code = EXCLUDED.country_code,
			deleted_at   = EXCLUDED.deleted_at,
			data         = EXCLUDED.data,
			updated_at   = EXCLUDED.updated_at
		WHERE hotels.data IS DISTINCT FROM EXCLUDED.data
	`)
	if err != nil {
		return 0, fmt.Errorf("upsert: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}

	return int(tag.RowsAffected()), nil
}

// ---------------------------------------------------------------------------
// import_runs
// ---------------------------------------------------------------------------

func createImportRun(ctx context.Context, pool *pgxpool.Pool, importType, filename string) (int64, error) {
	var id int64
	err := pool.QueryRow(ctx, `
		INSERT INTO import_runs (type, filename, status, started_at)
		VALUES ($1, $2, 'running', NOW())
		RETURNING id
	`, importType, filename).Scan(&id)
	return id, err
}

func buildErrorMsg(lineErrors []string, runErr error) string {
	var parts []string
	if len(lineErrors) > 0 {
		n := len(lineErrors)
		limited := lineErrors
		if n > maxErrorMsgsInDB {
			limited = lineErrors[:maxErrorMsgsInDB]
		}
		parts = append(parts, limited...)
		if n > maxErrorMsgsInDB {
			parts = append(parts, fmt.Sprintf("… and %d more line errors", n-maxErrorMsgsInDB))
		}
	}
	if runErr != nil {
		parts = append(parts, runErr.Error())
	}
	return strings.Join(parts, "\n")
}

func finishImportRun(ctx context.Context, pool *pgxpool.Pool, id int64, stats Stats, lineErrors []string, runErr error) {
	status := "success"
	errMsg := buildErrorMsg(lineErrors, runErr)
	if runErr != nil {
		status = "failed"
	}
	_, err := pool.Exec(ctx, `
		UPDATE import_runs SET
			status       = $1,
			finished_at  = NOW(),
			total        = $2,
			upserted     = $3,
			soft_deleted = $4,
			errors       = $5,
			error_msg    = NULLIF($6, '')
		WHERE id = $7
	`, status, stats.Total, stats.Upsert, stats.Deleted, stats.Errors, errMsg, id)
	if err != nil {
		slog.Error("failed to update import_run", "err", err)
	}
}

// ---------------------------------------------------------------------------
// Поиск файла для импорта
// ---------------------------------------------------------------------------

// findLatestDump ищет самый свежий файл вида YYYY-MM-DD_*.json
// с готовым флагом .done в папке dumpsDir.
func findLatestDump(dumpsDir string) (string, error) {
	entries, err := os.ReadDir(dumpsDir)
	if err != nil {
		return "", err
	}

	var latest string
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".json") {
			continue
		}
		doneFile := filepath.Join(dumpsDir, name+".done")
		if _, err := os.Stat(doneFile); errors.Is(err, os.ErrNotExist) {
			slog.Warn("dump exists but .done flag missing, skipping", "file", name)
			continue
		}
		if name > latest {
			latest = name
		}
	}

	if latest == "" {
		return "", fmt.Errorf("no ready dump files found in %s", dumpsDir)
	}
	return filepath.Join(dumpsDir, latest), nil
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	cfg := configFromEnv()
	ctx := context.Background()

	// Подключение к БД
	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		slog.Error("failed to connect to database", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		slog.Error("database ping failed", "err", err)
		os.Exit(1)
	}
	slog.Info("connected to database")

	// Находим файл дампа
	dumpPath, err := findLatestDump(cfg.DumpsDir)
	if err != nil {
		slog.Error("no dump found", "err", err)
		os.Exit(1)
	}
	filename := filepath.Base(dumpPath)
	importType := "incremental_dump"
	if strings.Contains(filename, "_dump_") || strings.HasPrefix(filename, "dump") {
		importType = "full_dump"
	}

	slog.Info("starting import", "file", filename, "type", importType)

	// Создаём запись о запуске
	runID, err := createImportRun(ctx, pool, importType, filename)
	if err != nil {
		slog.Error("failed to create import_run", "err", err)
		os.Exit(1)
	}

	// Открываем файл
	f, err := os.Open(dumpPath)
	if err != nil {
		slog.Error("failed to open dump", "err", err)
		finishImportRun(ctx, pool, runID, Stats{}, nil, err)
		os.Exit(1)
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		slog.Error("failed to stat dump", "err", err)
		finishImportRun(ctx, pool, runID, Stats{}, nil, err)
		os.Exit(1)
	}

	// Импорт
	start := time.Now()
	progress := newImportProgress(fi.Size(), start)
	imp := NewImporter(pool, cfg.BatchSize, cfg.MaxLineSize)
	stats, lineErrors, importErr := imp.Import(ctx, f, runID, progress)

	finishImportRun(ctx, pool, runID, stats, lineErrors, importErr)

	if importErr != nil {
		slog.Error("import failed", "err", importErr, "stats", stats.String())
		os.Exit(1)
	}

	slog.Info("import finished",
		"stats", stats.String(),
		"duration", time.Since(start).Round(time.Second),
	)
}

// ---------------------------------------------------------------------------
// Прогресс импорта (stderr, чтобы не мешать slog)
// ---------------------------------------------------------------------------

const progressBarWidth = 40

type importProgress struct {
	fileSize   int64
	start      time.Time
	lastRender time.Time
}

func newImportProgress(fileSize int64, start time.Time) *importProgress {
	return &importProgress{fileSize: fileSize, start: start}
}

func (p *importProgress) update(bytesRead int64, stats Stats) {
	if time.Since(p.lastRender) < 200*time.Millisecond {
		return
	}
	p.render(bytesRead, stats, false)
}

func (p *importProgress) finish(bytesRead int64, stats Stats) {
	p.render(bytesRead, stats, true)
}

func (p *importProgress) render(bytesRead int64, stats Stats, final bool) {
	p.lastRender = time.Now()
	elapsed := time.Since(p.start)

	var pct float64
	if p.fileSize > 0 {
		pct = float64(bytesRead) / float64(p.fileSize) * 100
		if pct > 100 {
			pct = 100
		}
	}

	filled := int(pct / 100 * progressBarWidth)
	if filled > progressBarWidth {
		filled = progressBarWidth
	}
	bar := "[" + strings.Repeat("█", filled) + strings.Repeat("░", progressBarWidth-filled) + "]"

	var rate string
	if elapsed > 0 {
		rate = fmt.Sprintf("%d/s", int(float64(stats.Total)/elapsed.Seconds()))
	}

	line := fmt.Sprintf("\r%s %5.1f%% | %s hotels | %s/%s | %s elapsed | %s",
		bar, pct, formatInt(stats.Total), formatBytes(bytesRead), formatBytes(p.fileSize),
		formatDuration(elapsed), rate)
	if stats.Errors > 0 {
		line += fmt.Sprintf(" | %d errors", stats.Errors)
	}

	fmt.Fprint(os.Stderr, line)
	if final {
		fmt.Fprintln(os.Stderr)
	}
}

func formatDuration(d time.Duration) string {
	d = d.Round(time.Second)
	h := d / time.Hour
	d -= h * time.Hour
	m := d / time.Minute
	d -= m * time.Minute
	s := d / time.Second
	if h > 0 {
		return fmt.Sprintf("%02d:%02d:%02d", h, m, s)
	}
	return fmt.Sprintf("%02d:%02d", m, s)
}

func formatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

func formatInt(n int) string {
	s := strconv.Itoa(n)
	if len(s) <= 3 {
		return s
	}
	var b strings.Builder
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteRune(c)
	}
	return b.String()
}

// ---------------------------------------------------------------------------
// Утилиты
// ---------------------------------------------------------------------------

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		fmt.Fprintf(os.Stderr, "required env variable %s is not set\n", key)
		os.Exit(1)
	}
	return v
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// byteCounter отслеживает смещение в файле при построчном чтении дампа.
type byteCounter struct {
	r      io.Reader
	offset int64
}

func (c *byteCounter) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.offset += int64(n)
	return n, err
}
