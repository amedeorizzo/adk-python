# PostgreSQL to Typesense Migration Guide

This guide explains how to migrate your existing session data from PostgreSQL (DatabaseSessionService) to Typesense (TypesenseSessionService).

## Prerequisites

1. **Install dependencies**:
   ```bash
   pip install sqlalchemy psycopg2-binary typesense
   ```

2. **Typesense collections**: The migration script will automatically create missing collections with the correct schema.

   Alternatively, you can initialize TypesenseSessionService before migration:
   ```python
   from google.adk.sessions import TypesenseSessionService

   # This will create collections automatically
   service = TypesenseSessionService(
       host="localhost",
       api_key="your_api_key",
       port=8108,
       protocol="http"
   )
   ```

## Usage

### Basic Migration

```bash
python migrate_to_typesense.py \
    --db-url postgresql://user:password@localhost:5432/your_database \
    --typesense-host localhost \
    --typesense-api-key your_api_key \
    --typesense-port 8108 \
    --typesense-protocol http
```

### Dry Run (Preview What Will Be Migrated)

Test the migration without actually moving data:

```bash
python migrate_to_typesense.py \
    --db-url postgresql://user:password@localhost:5432/your_database \
    --typesense-host localhost \
    --typesense-api-key your_api_key \
    --dry-run
```

### Using Environment Variables

```bash
# Export credentials
export DB_URL="postgresql://user:password@localhost:5432/your_database"
export TYPESENSE_HOST="localhost"
export TYPESENSE_API_KEY="your_secret_api_key"

# Run migration
python migrate_to_typesense.py \
    --db-url "$DB_URL" \
    --typesense-host "$TYPESENSE_HOST" \
    --typesense-api-key "$TYPESENSE_API_KEY"
```

## What Gets Migrated

The script migrates all rows from these PostgreSQL tables to Typesense collections:

| PostgreSQL Table | → | Typesense Collection | Description |
|-----------------|---|---------------------|-------------|
| `storage_sessions` | → | `sessions` | Session metadata and state |
| `storage_events` | → | `events` | Events within sessions |
| `storage_app_states` | → | `app_states` | App-level state |
| `storage_user_states` | → | `user_states` | User-level state |

## Migration Process

The script performs these steps:

1. **Connects to PostgreSQL** - Validates connection and tables exist
2. **Connects to Typesense** - Validates connection and collections exist
3. **Verifies Collections** - Checks that Typesense collections have correct schema (including `enable_nested_fields`)
4. **Migrates Data** in this order:
   - App states (foundational data)
   - User states (depends on apps)
   - Sessions (depends on users)
   - Events (depends on sessions)
5. **Reports Summary** - Shows counts and duration

## Advanced Options

### Automatic Collection Creation

By default, the script automatically creates missing Typesense collections. To disable this:

```bash
python migrate_to_typesense.py \
    --db-url "$DB_URL" \
    --typesense-host "$TYPESENSE_HOST" \
    --typesense-api-key "$TYPESENSE_API_KEY" \
    --no-auto-create
```

With `--no-auto-create`, the script will fail if collections don't exist.

### Skip Collection Verification

If you're confident collections exist and are correct:
```bash
python migrate_to_typesense.py \
    --db-url "$DB_URL" \
    --typesense-host "$TYPESENSE_HOST" \
    --typesense-api-key "$TYPESENSE_API_KEY" \
    --skip-verification
```

### Migrate Specific Tables Only

Modify the script and comment out unwanted migrations in the `main()` function.

## Example: Complete Migration Workflow

```bash

# 1. Test with dry run (collections will be auto-created during verification)
python migrate_to_typesense.py \
    --db-url postgresql://user:pass@localhost:5432/mydb \
    --typesense-host localhost \
    --typesense-api-key xyz \
    --dry-run

# 2. Perform actual migration (collections already exist from dry run)
python migrate_to_typesense.py \
    --db-url postgresql://user:pass@localhost:5432/mydb \
    --typesense-host localhost \
    --typesense-api-key xyz

# 3. Verify
python -c "
import typesense
client = typesense.Client({'nodes': [{'host': 'localhost', 'port': 8108, 'protocol': 'http'}], 'api_key': 'xyz'})
for c in ['sessions', 'events', 'app_states', 'user_states']:
    info = client.collections[c].retrieve()
    print(f'{c}: {info[\"num_documents\"]} documents')
"
```
## Keix fork candidate on ADK 1.39.1

This candidate is based on the official `google/adk-python` tag `v1.39.1`,
commit `51f242a30888f5e7a660b4a5d9809176d05156c9`. The previous Keix fork
baseline was `e513f82ed794e3c85d53dcc9d23f22c187dffcae` on ADK 1.27.4.
The candidate keeps the Typesense service, lazy public export, state helpers,
legacy PostgreSQL migration script and the lean Keix dependency subset. Core
version bounds follow upstream 1.39.1, including Google GenAI SDK 2.x.
ADK itself remains 1.x.

### Storage changes and compatibility

- `get_session` pages through all events unless the caller explicitly supplies
  a recent-event limit. Zero means state-only; positive limits can exceed a
  single Typesense page. Events are returned chronologically.
- Loading history preserves every event, tool call, response ID, author,
  branch and action. The old Gemini-specific text-only sanitizer is removed.
  Provider-specific message adaptation belongs in the model layer.
- New events include a complete Pydantic JSON payload in `event_json`, retaining
  response/usage metadata and binary thought signatures. Legacy event fields
  remain populated for older readers. Legacy records without this payload
  still load through their existing fields.
- Initialization creates missing collections as before, and adds only the
  optional, unindexed `event_json` field to an existing events collection.
  **The first candidate startup against a real Typesense instance performs
  this additive schema operation.** Its key must have collection-schema update
  permission. No production or development data store was contacted while
  preparing or testing this candidate.
- Duplicate session creation now raises ADK `AlreadyExistsError` rather than
  overwriting an existing session's state/history. Callers should fetch the
  existing session when this error is expected.
- Temporary state is visible during the current invocation but is removed from
  persisted session state and event deltas. App/user state uses direct document
  retrieval, preserving the fork's read-your-own-write behavior.
- `list_sessions` accepts an omitted `user_id`, following ADK's current public
  contract, and returns summaries without state or event history. Consumers
  needing state must call `get_session` explicitly.
- Legacy actions accept JSON or restricted base64-pickle reconstruction with
  the same reviewed class allowlist as upstream's legacy SQL migration.
  Arbitrary globals are rejected. Corrupt/unsupported legacy actions are
  logged without their payload and fall back to empty actions on that event.
  New actions are always JSON; no unrestricted unpickling fallback exists.

Old documents cannot recover fields that the previous writer never stored.
No bulk rewrite or migration has been performed. The PostgreSQL migration
script remains a legacy-schema importer; it does not backfill complete modern
`event_json` payloads. Its execution requires a separate migration plan.

Typesense event search remains eventually consistent even though session and
state reads use immediate direct-ID lookup. Timestamp ordering cannot guarantee
an original order for old events with identical microsecond timestamps. Paging
is not a transactional snapshot during concurrent writes. Cross-process
app/user/session updates still need application-level coordination: the
existing read/merge/write persistence has no compare-and-swap transaction.
The tests of concurrent coroutines cover the service's current single-event-loop
behavior and stale-session detection, not distributed atomicity.

### Keix Sonnet adapter compatibility gate

Keix's trial `SonnetLiteLlm` private imports remain available on this tag:
`_get_completion_inputs(request, model)` is async and returns four values;
`_model_response_to_generate_content_response(response)` still takes one
argument. However, the 1.39.1 converter now preserves native Anthropic thought
parts and their raw signed signatures. The existing Keix adapter additionally
inserts its own JSON replay marker, then tries to parse the native signature as
that marker on the next turn. Offline transport tests reproduce
`ValueError: Unrecognized thought signature in Anthropic history`.

Before promoting the candidate, adapt Keix's response handling to replace the
native thought parts with its complete signed replay packet while preserving
normal text/tool parts, or deliberately migrate the adapter to upstream's
native replay contract. Do not loosen model/tool-turn signature checks. The
trial checkout was inspected and tested read-only; this fork patch does not
modify it. Upstream's LiteLLM extra requires `>=1.83.7,<=1.83.14`, and therefore
cannot be combined unchanged with the old Keix `<1.75` constraint.

### Isolated verification

Use `uv` and a separate virtualenv. Focused Typesense tests need only the lean
core and the test tools; the upstream SQL session tests additionally require
SQLAlchemy's asyncio extra and aiosqlite.

```bash
uv venv --python python3.13 .venv
uv pip install --python .venv/bin/python -e . pytest pytest-asyncio pytest-mock
uv pip install --python .venv/bin/python 'sqlalchemy[asyncio]' aiosqlite
.venv/bin/pytest -q tests/unittests/sessions/test_typesense_*.py
.venv/bin/pytest -q tests/unittests/sessions/test_session_service.py \
    tests/unittests/models/test_llm_response.py
```

On Python 3.13.5, the focused Typesense suite passes 22 tests. The expanded
session/response suite passes all other tests, with a reproducible upstream
failure in `test_concurrent_prepare_tables_no_race_condition` using
SQLAlchemy 2.1.1 and SQLite `:memory:`: a concurrent-created session cannot
be read back. Both the test and `DatabaseSessionService` source are unchanged
from the official tag. The Typesense service is not the backend exercised by
that failing test. The full ADK suite and all optional cloud integrations
have not been certified.

The paired Keix-agent integration candidate now replaces native thought parts
with one validated replay packet and preserves signed complete tool turns when
restoring full Typesense history. With LiteLLM 1.83.14, all nine adapter tests and
seven restored-history tests pass, including two fresh Runner instances and the
real Typesense serializer. The transport is mocked and spends no API credit.
This fix is in Keix-agent, not in this fork: promote the two changes together.
The old Keix `<1.75` LiteLLM bound must also be updated before that promotion.
