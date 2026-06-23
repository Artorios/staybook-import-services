package main

import (
	"bufio"
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

type Config struct {
	DatabaseURL string
	DumpsDir    string
	BatchSize   int
}

func configFromEnv() Config {
	batchSize := 500
	if v := os.Getenv("BATCH_SIZE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			batchSize = n
		}
	}
	return Config{
		DatabaseURL: mustEnv("DATABASE_URL"),
		DumpsDir:    mustEnv("DUMPS_DIR"),
		BatchSize:   batchSize,
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
	pool      *pgxpool.Pool
	batchSize int
}

func NewImporter(pool *pgxpool.Pool, batchSize int) *Importer {
	return &Importer{pool: pool, batchSize: batchSize}
}

const maxErrorMsgsInDB = 20

// Import читает NDJSON-файл построчно и делает батч-upsert.
// importRunID — ID записи в таблице import_runs.
func (imp *Importer) Import(ctx context.Context, r io.Reader, importRunID int64) (Stats, []string, error) {
	var stats Stats
	var lineErrors []string
	batch := make([]Hotel, 0, imp.batchSize)
	var batchStartOffset int64

	counter := &byteCounter{r: r}
	scanner := bufio.NewScanner(counter)
	scanner.Buffer(make([]byte, 4*1024*1024), 4*1024*1024) // строки до 4 МБ

	for {
		lineOffset := counter.offset
		if !scanner.Scan() {
			break
		}
		line := scanner.Bytes()
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
		h.Raw = line

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

			if stats.Total%10000 == 0 {
				slog.Info("progress", "processed", stats.Total)
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return stats, lineErrors, fmt.Errorf("scanner at file offset %d: %w", counter.offset, err)
	}

	// последний неполный батч
	if len(batch) > 0 {
		n, err := imp.upsertBatch(ctx, batch, importRunID)
		if err != nil {
			return stats, lineErrors, fmt.Errorf("upsert final batch at file offset %d: %w", batchStartOffset, err)
		}
		stats.Upsert += n
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
			h.Country, h.Deleted, h.Raw, importRunID,
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

	// Импорт
	start := time.Now()
	imp := NewImporter(pool, cfg.BatchSize)
	stats, lineErrors, importErr := imp.Import(ctx, f, runID)

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

// byteCounter отслеживает смещение в файле при чтении через bufio.Scanner.
type byteCounter struct {
	r      io.Reader
	offset int64
}

func (c *byteCounter) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.offset += int64(n)
	return n, err
}
