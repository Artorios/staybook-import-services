#!/bin/sh
set -eu

CONNECT_URL="http://debezium-connect:8083"
CONNECTOR_NAME="${DEBEZIUM_CONNECTOR_NAME:-ostrovok-postgres-connector}"

echo "Waiting for Kafka Connect at ${CONNECT_URL}..."
until curl -sf "${CONNECT_URL}/" >/dev/null 2>&1; do
  sleep 3
done

echo "Registering connector ${CONNECTOR_NAME}..."

payload=$(cat <<EOF
{
  "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
  "tasks.max": "1",
  "database.hostname": "postgres",
  "database.port": "${POSTGRES_PORT}",
  "database.user": "${DEBEZIUM_USER}",
  "database.password": "${DEBEZIUM_PASSWORD}",
  "database.dbname": "${POSTGRES_DB}",
  "topic.prefix": "${DEBEZIUM_TOPIC_PREFIX}",
  "plugin.name": "pgoutput",
  "slot.name": "${DEBEZIUM_SLOT_NAME}",
  "publication.name": "${DEBEZIUM_PUBLICATION}",
  "schema.include.list": "public",
  "table.include.list": "public.hotels,public.hotel_audit_log,public.import_runs",
  "key.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "key.converter.schemas.enable": "false",
  "value.converter.schemas.enable": "false",
  "snapshot.mode": "initial",
  "tombstones.on.delete": "false",
  "producer.override.max.request.size": "16777216"
}
EOF
)

status=$(curl -s -o /tmp/connector-response.json -w "%{http_code}" \
  -X PUT "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/config" \
  -H "Content-Type: application/json" \
  -d "${payload}")

if [ "$status" -ge 200 ] && [ "$status" -lt 300 ]; then
  echo "Connector ${CONNECTOR_NAME} registered successfully (HTTP ${status})."
  cat /tmp/connector-response.json
  exit 0
fi

echo "Failed to register connector (HTTP ${status}):"
cat /tmp/connector-response.json
exit 1
