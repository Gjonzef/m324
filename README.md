# Ledger Listener

Event-driven Billing-Listener fuer die Architektur:

`MariaDB -> Debezium -> Kafka (CDC + monthly_billing) -> Python Listener -> ClickHouse`

Der Service schreibt append-only Ledger-Eintraege nach ClickHouse, unterstuetzt Korrekturbuchungen und bietet einen Demo-Invoice-Run fuer die Aggregation.

## Aktueller Funktionsumfang

- Kafka-Consumer fuer Debezium-CDC und `monthly_billing`.
- Debezium-Parser fuer Envelope mit/ohne `payload` Wrapper.
- Verarbeitung von `subscription`-Create-Events (`op=c`) mit Preisanreicherung aus MariaDB.
- Verarbeitung direkter Billing-Events (z. B. `monthly`, `prorata`, `correction`).
- Idempotenz ueber `position_id`.
- Append-only Speicherung in ClickHouse (`billing_ledger` + Detailtabellen).
- Offset-Commit erst nach erfolgreichem ClickHouse-Insert.
- Korrekturbuchungs-Skript (Gegenbuchung + optionale Ersatzbuchung).
- Demo-Invoice-Skript zur Aggregation pro Billing-Periode.
- Test-Suite fuer Parser, Betragslogik, Idempotenz, Korrektur- und Invoice-Verhalten.

## Projektstruktur

```text
config/
ledger_listener/
  clickhouse/client.py
  debezium/parser.py
  events/models.py
  kafka/consumer.py
  mariadb/client.py
  processor/billing.py
  processor/worker.py
  listener.py
  management/commands/runlistener.py
scripts/
  ci/check_kafka.py
  ci/ensure_clickhouse_schema.py
  ci/smoke_test.py
  correction_booking.py
  invoice.py
tests/
manage.py
docker-compose.yml
Dockerfile
pyproject.toml
```

## Voraussetzungen

- Python 3.13+
- `uv`
- Docker + Docker Compose (fuer lokale ClickHouse/Compose-Workflows)
- Externe Kafka/Debezium-Infrastruktur

## Setup

1. Abhaengigkeiten installieren:

```bash
uv sync --dev
```

2. Konfiguration bereitstellen:

```bash
cp .env.example .env
```

3. In `.env` mindestens diese Werte fuer deine Umgebung setzen:

- `KAFKA_BOOTSTRAP_SERVERS`
- `KAFKA_CDC_TOPICS`
- `MARIADB_HOST`
- `MARIADB_DATABASE`
- `MARIADB_USER`
- `MARIADB_PASSWORD`

## Listener starten

```bash
uv run python manage.py runlistener
```

Hinweis: Beim Start wird das ClickHouse-Schema erstellt, falls es noch nicht existiert.

## Lokaler Docker-Compose-Flow

1. Image bauen:

```bash
docker compose build listener
```

2. ClickHouse starten:

```bash
docker compose up -d clickhouse
```

3. Kafka-Broker pruefen:

```bash
docker compose run --rm kafka-check
```

4. ClickHouse-Schema anlegen:

```bash
docker compose run --rm clickhouse-init
```

5. Listener starten:

```bash
docker compose up -d listener
```

6. Optionaler Smoke-Test:

```bash
docker compose run --rm smoke-test
```

## Tests und Qualitaet

Alle Tests:

```bash
uv run pytest
```

Einzelner Testfall (Beispiel TC09 Korrekturaggregation):

```bash
uv run pytest tests/test_tc09_aggregation_after_correction.py -q
```

Linting:

```bash
uv run ruff check .
```

## Korrekturbuchung

Mit `scripts/correction_booking.py` kann eine bestehende Buchung korrigiert werden:

- Gegenbuchung (`billing_type=correction`, negativer Betrag)
- Optional Ersatzbuchung (gleicher oder neuer Betrag)

Dry-Run (zeigt nur erzeugte Payloads):

```bash
uv run python scripts/correction_booking.py \
  --ledger-entry-id <LEDGER_ENTRY_ID> \
  --replacement-amount-chf 12.50
```

Nach Kafka senden:

```bash
uv run python scripts/correction_booking.py \
  --ledger-entry-id <LEDGER_ENTRY_ID> \
  --replacement-amount-chf 12.50 \
  --send
```

## Demo-Invoice-Lauf

Mit `scripts/invoice.py` koennen Ledger-Buchungen fuer eine Periode aggregiert und als JSON ausgegeben werden.

Mit eingebauten Demo-Daten:

```bash
uv run python scripts/invoice.py --period 2026-04 --demo-data
```

Mit echten ClickHouse-Daten:

```bash
uv run python scripts/invoice.py --period 2026-04
```

Optional mit Filtern und Datei-Output:

```bash
uv run python scripts/invoice.py \
  --period 2026-04 \
  --provider-id 7 \
  --output demo_invoice_run.json
```

## GitHub Actions CI

Der Workflow `.github/workflows/ci.yml` laeuft bei Pushes und Pull Requests.

- `ruff` prueft den Code-Stil.
- `pytest` fuehrt die Python-Unit-Tests aus.
- `talisman` scannt nach Secrets und laedt den Report als Artifact hoch. Der Security-Scan ist weiterhin nicht blockierend.

## Umgebungsvariablen

Siehe `.env.example` fuer alle Werte inkl. Defaults.

Wichtig:

- Lokal mappt Docker Compose ClickHouse auf `18123:8123`.
- Fuer Compose-Services werden standardmaessig `DOCKER_CLICKHOUSE_HOST=clickhouse` und `DOCKER_CLICKHOUSE_PORT=8123` verwendet.
- Fuer den Smoke-Test werden zusaetzlich `SMOKE_*` Variablen benoetigt.
