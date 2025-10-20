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