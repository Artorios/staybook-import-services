#!/bin/bash
set -euo pipefail

# Выполняется после db.sql (docker-entrypoint-initdb.d, порядок по имени файла).
# Переменные DEBEZIUM_* пробрасываются из .env через docker-compose.

: "${DEBEZIUM_USER:?DEBEZIUM_USER is required}"
: "${DEBEZIUM_PASSWORD:?DEBEZIUM_PASSWORD is required}"
: "${DEBEZIUM_PUBLICATION:?DEBEZIUM_PUBLICATION is required}"

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '${DEBEZIUM_USER}') THEN
            EXECUTE format(
                'CREATE USER %I WITH REPLICATION LOGIN PASSWORD %L',
                '${DEBEZIUM_USER}',
                '${DEBEZIUM_PASSWORD}'
            );
        END IF;
    END
    \$\$;

    DO \$\$
    BEGIN
        EXECUTE format('GRANT CONNECT ON DATABASE %I TO %I', current_database(), '${DEBEZIUM_USER}');
    END
    \$\$;

    GRANT USAGE ON SCHEMA public TO "${DEBEZIUM_USER}";
    GRANT SELECT ON ALL TABLES IN SCHEMA public TO "${DEBEZIUM_USER}";
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO "${DEBEZIUM_USER}";

    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_publication WHERE pubname = '${DEBEZIUM_PUBLICATION}') THEN
            EXECUTE format('CREATE PUBLICATION %I FOR ALL TABLES', '${DEBEZIUM_PUBLICATION}');
        END IF;
    END
    \$\$;
EOSQL
