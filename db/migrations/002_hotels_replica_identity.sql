-- Для уже существующих БД (initdb не перезапускался).
-- Debezium + крупный JSONB в TOAST: без FULL на UPDATE приходит __debezium_unavailable_value.
ALTER TABLE hotels REPLICA IDENTITY FULL;
