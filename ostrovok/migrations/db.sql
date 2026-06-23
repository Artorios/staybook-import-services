-- Основная таблица отелей
CREATE TABLE IF NOT EXISTS hotels (
    id           BIGSERIAL PRIMARY KEY,
    external_id  BIGINT        NOT NULL,          -- hid из дампа
    slug         TEXT          NOT NULL,          -- id из дампа (строковый)
    name         TEXT          NOT NULL,
    latitude     DOUBLE PRECISION,
    longitude    DOUBLE PRECISION,
    country_code TEXT,
    data         JSONB         NOT NULL,           -- полный JSON отеля
    deleted_at   TIMESTAMPTZ,                      -- soft delete
    created_at   TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ   NOT NULL DEFAULT NOW(),

    CONSTRAINT hotels_external_id_unique UNIQUE (external_id)
);

CREATE INDEX IF NOT EXISTS hotels_country_code_idx ON hotels (country_code);
CREATE INDEX IF NOT EXISTS hotels_deleted_at_idx   ON hotels (deleted_at) WHERE deleted_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS hotels_data_gin_idx     ON hotels USING GIN (data); -- поиск внутри JSONB

-- История запусков импорта
CREATE TABLE IF NOT EXISTS import_runs (
    id           BIGSERIAL PRIMARY KEY,
    type         TEXT          NOT NULL,           -- 'full_dump' | 'incremental_dump'
    filename     TEXT          NOT NULL,
    status       TEXT          NOT NULL DEFAULT 'running', -- 'running' | 'success' | 'failed'
    total        INT           DEFAULT 0,
    upserted     INT           DEFAULT 0,
    soft_deleted INT           DEFAULT 0,
    errors       INT           DEFAULT 0,
    error_msg    TEXT,
    started_at   TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    finished_at  TIMESTAMPTZ
);

-- Лог изменений (заполняется триггером)
CREATE TABLE IF NOT EXISTS hotel_audit_log (
    id            BIGSERIAL PRIMARY KEY,
    hotel_id      BIGINT        NOT NULL REFERENCES hotels(id),
    import_run_id BIGINT        REFERENCES import_runs(id),
    changed_at    TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    old_data      JSONB,
    new_data      JSONB
);

CREATE INDEX IF NOT EXISTS hotel_audit_log_hotel_id_idx ON hotel_audit_log (hotel_id);

-- Триггер: пишем в audit_log только если данные реально изменились
CREATE OR REPLACE FUNCTION hotels_audit_trigger_fn()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'UPDATE' AND OLD.data IS DISTINCT FROM NEW.data THEN
        INSERT INTO hotel_audit_log (hotel_id, changed_at, old_data, new_data)
        VALUES (OLD.id, NOW(), OLD.data, NEW.data);
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS hotels_audit ON hotels;
CREATE TRIGGER hotels_audit
    AFTER UPDATE ON hotels
    FOR EACH ROW EXECUTE FUNCTION hotels_audit_trigger_fn();
