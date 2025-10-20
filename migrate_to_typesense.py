#!/usr/bin/env python3
"""
Migration script to move session data from PostgreSQL to Typesense.

This script reads data from the PostgreSQL tables used by DatabaseSessionService
and migrates it to Typesense collections used by TypesenseSessionService.

Features:
- Automatically creates missing Typesense collections with correct schema
- Dry-run mode to preview migration without moving data
- Handles all data type conversions (timestamps, actions, nested objects)
- Error handling and detailed progress reporting

Usage:
    python migrate_to_typesense.py --db-url postgresql://user:pass@host:5432/dbname \
                                   --typesense-host localhost \
                                   --typesense-api-key your_api_key \
                                   --typesense-port 8108 \
                                   --typesense-protocol http

    Optional flags:
        --dry-run              Preview what would be migrated
        --no-auto-create       Don't create missing collections automatically
        --skip-verification    Skip collection verification step
"""

import argparse
import base64
import pickle
import sys
from datetime import datetime
from typing import Any

import typesense
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# Typesense collection schemas (must match TypesenseSessionService)
SESSIONS_SCHEMA = {
    "name": "sessions",
    "enable_nested_fields": True,
    "fields": [
        {"name": "app_name", "type": "string"},
        {"name": "user_id", "type": "string"},
        {"name": "session_id", "type": "string"},
        {"name": "composite_key", "type": "string"},
        {"name": "state", "type": "object"},
        {"name": "create_time", "type": "int64"},
        {"name": "update_time", "type": "int64"},
    ],
}

EVENTS_SCHEMA = {
    "name": "events",
    "enable_nested_fields": True,
    "fields": [
        {"name": "event_id", "type": "string"},
        {"name": "app_name", "type": "string"},
        {"name": "user_id", "type": "string"},
        {"name": "session_id", "type": "string"},
        {"name": "composite_key", "type": "string"},
        {"name": "invocation_id", "type": "string"},
        {"name": "author", "type": "string"},
        {"name": "actions", "type": "string", "optional": True},
        {"name": "long_running_tool_ids_json", "type": "string", "optional": True},
        {"name": "branch", "type": "string", "optional": True},
        {"name": "timestamp", "type": "int64"},
        {"name": "content", "type": "object", "optional": True},
        {"name": "grounding_metadata", "type": "object", "optional": True},
        {"name": "custom_metadata", "type": "object", "optional": True},
        {"name": "partial", "type": "bool", "optional": True},
        {"name": "turn_complete", "type": "bool", "optional": True},
        {"name": "error_code", "type": "string", "optional": True},
        {"name": "error_message", "type": "string", "optional": True},
        {"name": "interrupted", "type": "bool", "optional": True},
    ],
}

APP_STATES_SCHEMA = {
    "name": "app_states",
    "enable_nested_fields": True,
    "fields": [
        {"name": "app_name", "type": "string"},
        {"name": "state", "type": "object"},
        {"name": "update_time", "type": "int64"},
    ],
}

USER_STATES_SCHEMA = {
    "name": "user_states",
    "enable_nested_fields": True,
    "fields": [
        {"name": "app_name", "type": "string"},
        {"name": "user_id", "type": "string"},
        {"name": "composite_key", "type": "string"},
        {"name": "state", "type": "object"},
        {"name": "update_time", "type": "int64"},
    ],
}


def to_microseconds(timestamp) -> int:
    """Convert timestamp to microseconds.

    Args:
        timestamp: Either a datetime object or Unix timestamp (float)

    Returns:
        Microseconds since Unix epoch as int64
    """
    if isinstance(timestamp, datetime):
        # Convert datetime to Unix timestamp first
        return int(timestamp.timestamp() * 1_000_000)
    else:
        # Assume it's already a Unix timestamp (float)
        return int(timestamp * 1_000_000)


def make_composite_key(*parts: str) -> str:
    """Create composite key for Typesense document ID."""
    return ":".join(parts)


def serialize_actions(actions) -> str:
    """Base64 encode actions (handles dict, bytes, and memoryview)."""
    if isinstance(actions, memoryview):
        # PostgreSQL BYTEA returns memoryview, convert to bytes
        return base64.b64encode(actions.tobytes()).decode("ascii")
    elif isinstance(actions, bytes):
        # Already pickled as bytes
        return base64.b64encode(actions).decode("ascii")
    else:
        # Dict from JSON column, need to pickle first
        return base64.b64encode(pickle.dumps(actions)).decode("ascii")


def migrate_sessions(pg_session, ts_client, dry_run=False):
    """Migrate sessions from PostgreSQL to Typesense."""
    print("\n📦 Migrating sessions...")

    query = text("""
        SELECT app_name, user_id, id, state, create_time, update_time
        FROM sessions
        ORDER BY create_time
    """)

    result = pg_session.execute(query)
    sessions = result.fetchall()

    print(f"Found {len(sessions)} sessions to migrate")

    migrated = 0
    for row in sessions:
        app_name, user_id, session_id, state, create_time, update_time = row

        composite_key = make_composite_key(app_name, user_id, session_id)

        doc = {
            "id": composite_key,
            "app_name": app_name,
            "user_id": user_id,
            "session_id": session_id,
            "composite_key": composite_key,
            "state": state or {},
            "create_time": to_microseconds(create_time),
            "update_time": to_microseconds(update_time),
        }

        if not dry_run:
            try:
                ts_client.collections["sessions"].documents.upsert(doc)
                migrated += 1
            except Exception as e:
                print(f"❌ Error migrating session {composite_key}: {e}")
        else:
            print(f"  Would migrate: {composite_key}")
            migrated += 1

    print(f"✅ Migrated {migrated}/{len(sessions)} sessions")
    return migrated


def migrate_events(pg_session, ts_client, dry_run=False):
    """Migrate events from PostgreSQL to Typesense."""
    print("\n📦 Migrating events...")

    query = text("""
        SELECT id, app_name, user_id, session_id, invocation_id, author,
               actions, long_running_tool_ids_json, branch, timestamp,
               content, grounding_metadata, custom_metadata, partial,
               turn_complete, error_code, error_message, interrupted
        FROM events
        ORDER BY timestamp
    """)

    result = pg_session.execute(query)
    events = result.fetchall()

    print(f"Found {len(events)} events to migrate")

    migrated = 0
    for row in events:
        (event_id, app_name, user_id, session_id, invocation_id, author,
         actions, long_running_tool_ids_json, branch, timestamp,
         content, grounding_metadata, custom_metadata, partial,
         turn_complete, error_code, error_message, interrupted) = row

        composite_key = make_composite_key(event_id, app_name, user_id, session_id)

        doc = {
            "id": composite_key,
            "event_id": event_id,
            "composite_key": composite_key,
            "app_name": app_name,
            "user_id": user_id,
            "session_id": session_id,
            "invocation_id": invocation_id,
            "author": author,
            "timestamp": to_microseconds(timestamp),
        }

        # Handle actions (already pickled in DB, need to base64 encode)
        if actions:
            doc["actions"] = serialize_actions(actions)

        # Add optional fields
        if long_running_tool_ids_json:
            doc["long_running_tool_ids_json"] = long_running_tool_ids_json
        if branch:
            doc["branch"] = branch
        if content:
            doc["content"] = content
        if grounding_metadata:
            doc["grounding_metadata"] = grounding_metadata
        if custom_metadata:
            doc["custom_metadata"] = custom_metadata
        if partial is not None:
            doc["partial"] = partial
        if turn_complete is not None:
            doc["turn_complete"] = turn_complete
        if error_code:
            doc["error_code"] = error_code
        if error_message:
            doc["error_message"] = error_message
        if interrupted is not None:
            doc["interrupted"] = interrupted

        if not dry_run:
            try:
                ts_client.collections["events"].documents.upsert(doc)
                migrated += 1
            except Exception as e:
                print(f"❌ Error migrating event {composite_key}: {e}")
        else:
            print(f"  Would migrate: {composite_key}")
            migrated += 1

    print(f"✅ Migrated {migrated}/{len(events)} events")
    return migrated


def migrate_app_states(pg_session, ts_client, dry_run=False):
    """Migrate app states from PostgreSQL to Typesense."""
    print("\n📦 Migrating app states...")

    query = text("""
        SELECT app_name, state, update_time
        FROM app_states
    """)

    result = pg_session.execute(query)
    app_states = result.fetchall()

    print(f"Found {len(app_states)} app states to migrate")

    migrated = 0
    for row in app_states:
        app_name, state, update_time = row

        doc = {
            "id": app_name,
            "app_name": app_name,
            "state": state or {},
            "update_time": to_microseconds(update_time),
        }

        if not dry_run:
            try:
                ts_client.collections["app_states"].documents.upsert(doc)
                migrated += 1
            except Exception as e:
                print(f"❌ Error migrating app state {app_name}: {e}")
        else:
            print(f"  Would migrate: {app_name}")
            migrated += 1

    print(f"✅ Migrated {migrated}/{len(app_states)} app states")
    return migrated


def migrate_user_states(pg_session, ts_client, dry_run=False):
    """Migrate user states from PostgreSQL to Typesense."""
    print("\n📦 Migrating user states...")

    query = text("""
        SELECT app_name, user_id, state, update_time
        FROM user_states
    """)

    result = pg_session.execute(query)
    user_states = result.fetchall()

    print(f"Found {len(user_states)} user states to migrate")

    migrated = 0
    for row in user_states:
        app_name, user_id, state, update_time = row

        composite_key = make_composite_key(app_name, user_id)

        doc = {
            "id": composite_key,
            "app_name": app_name,
            "user_id": user_id,
            "composite_key": composite_key,
            "state": state or {},
            "update_time": to_microseconds(update_time),
        }

        if not dry_run:
            try:
                ts_client.collections["user_states"].documents.upsert(doc)
                migrated += 1
            except Exception as e:
                print(f"❌ Error migrating user state {composite_key}: {e}")
        else:
            print(f"  Would migrate: {composite_key}")
            migrated += 1

    print(f"✅ Migrated {migrated}/{len(user_states)} user states")
    return migrated


def verify_collections(ts_client, auto_create=True):
    """Verify that Typesense collections exist with correct schema.

    Args:
        ts_client: Typesense client
        auto_create: If True, automatically create missing collections

    Returns:
        True if all collections exist or were created successfully
    """
    print("\n🔍 Verifying Typesense collections...")

    schemas = {
        "sessions": SESSIONS_SCHEMA,
        "events": EVENTS_SCHEMA,
        "app_states": APP_STATES_SCHEMA,
        "user_states": USER_STATES_SCHEMA,
    }

    for collection_name, schema in schemas.items():
        try:
            collection = ts_client.collections[collection_name].retrieve()
            print(f"  ✅ Collection '{collection_name}' exists")

            # Check for enable_nested_fields
            if not collection.get("enable_nested_fields"):
                print(f"  ⚠️  WARNING: '{collection_name}' doesn't have enable_nested_fields=true")
                print(f"     This may cause issues with nested objects")
                print(f"     Consider deleting and recreating: ts_client.collections['{collection_name}'].delete()")
        except typesense.exceptions.ObjectNotFound:
            if auto_create:
                print(f"  📦 Collection '{collection_name}' not found, creating...")
                try:
                    ts_client.collections.create(schema)
                    print(f"  ✅ Collection '{collection_name}' created successfully")
                except Exception as e:
                    print(f"  ❌ Failed to create collection '{collection_name}': {e}")
                    return False
            else:
                print(f"  ❌ Collection '{collection_name}' not found!")
                print(f"     Run with auto-create enabled or initialize TypesenseSessionService")
                return False

    return True


def main():
    parser = argparse.ArgumentParser(
        description="Migrate session data from PostgreSQL to Typesense"
    )
    parser.add_argument(
        "--db-url",
        required=True,
        help="PostgreSQL connection URL (e.g., postgresql://user:pass@host:5432/dbname)",
    )
    parser.add_argument(
        "--typesense-host",
        required=True,
        help="Typesense host (e.g., localhost)",
    )
    parser.add_argument(
        "--typesense-api-key",
        required=True,
        help="Typesense API key",
    )
    parser.add_argument(
        "--typesense-port",
        type=int,
        default=8108,
        help="Typesense port (default: 8108)",
    )
    parser.add_argument(
        "--typesense-protocol",
        choices=["http", "https"],
        default="http",
        help="Typesense protocol (default: http)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be migrated without actually migrating",
    )
    parser.add_argument(
        "--skip-verification",
        action="store_true",
        help="Skip Typesense collection verification",
    )
    parser.add_argument(
        "--no-auto-create",
        action="store_true",
        help="Don't automatically create missing Typesense collections",
    )

    args = parser.parse_args()

    print("=" * 60)
    print("PostgreSQL to Typesense Migration")
    print("=" * 60)

    if args.dry_run:
        print("\n⚠️  DRY RUN MODE - No data will be migrated\n")

    # Connect to PostgreSQL
    print(f"\n🔌 Connecting to PostgreSQL: {args.db_url.split('@')[1] if '@' in args.db_url else args.db_url}")
    try:
        engine = create_engine(args.db_url)
        Session = sessionmaker(bind=engine)
        pg_session = Session()

        # Test connection
        pg_session.execute(text("SELECT 1"))
        print("✅ PostgreSQL connection successful")
    except Exception as e:
        print(f"❌ Failed to connect to PostgreSQL: {e}")
        return 1

    # Connect to Typesense
    print(f"\n🔌 Connecting to Typesense: {args.typesense_protocol}://{args.typesense_host}:{args.typesense_port}")
    try:
        ts_client = typesense.Client({
            "nodes": [{
                "host": args.typesense_host,
                "port": args.typesense_port,
                "protocol": args.typesense_protocol,
            }],
            "api_key": args.typesense_api_key,
            "connection_timeout_seconds": 5,
        })

        # Test connection
        ts_client.collections.retrieve()
        print("✅ Typesense connection successful")
    except Exception as e:
        print(f"❌ Failed to connect to Typesense: {e}")
        return 1

    # Verify collections
    if not args.skip_verification:
        auto_create = not args.no_auto_create
        if not verify_collections(ts_client, auto_create=auto_create):
            print("\n❌ Collection verification failed. Please fix the issues above.")
            return 1

    # Perform migration
    print("\n" + "=" * 60)
    print("Starting Migration")
    print("=" * 60)

    start_time = datetime.now()

    total_migrated = 0
    total_migrated += migrate_app_states(pg_session, ts_client, args.dry_run)
    total_migrated += migrate_user_states(pg_session, ts_client, args.dry_run)
    total_migrated += migrate_sessions(pg_session, ts_client, args.dry_run)
    total_migrated += migrate_events(pg_session, ts_client, args.dry_run)

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    print("\n" + "=" * 60)
    print("Migration Summary")
    print("=" * 60)
    print(f"Total documents migrated: {total_migrated}")
    print(f"Duration: {duration:.2f} seconds")

    if args.dry_run:
        print("\n⚠️  This was a DRY RUN - no data was actually migrated")
        print("Remove --dry-run flag to perform the actual migration")
    else:
        print("\n✅ Migration completed successfully!")

    pg_session.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
