# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Typesense-based session service implementation."""

from __future__ import annotations

import base64
import binascii
from datetime import datetime
import io
import json
import logging
import pickle
import time
from typing import Any
from typing import Optional
import uuid

import typesense
from typing_extensions import override

from . import _session_util
from ..errors.already_exists_error import AlreadyExistsError
from ..events.event import Event
from ..events.event_actions import EventActions
from ._session_util import _merge_state as merge_state
from ._session_util import extract_state_delta
from .base_session_service import BaseSessionService
from .base_session_service import GetSessionConfig
from .base_session_service import ListSessionsResponse
from .session import Session

logger = logging.getLogger("google_adk." + __name__)

_COLLECTION_INIT_ATTEMPTS = 8
_COLLECTION_UPDATE_BUSY = "Another collection update operation is in progress"

# Typesense collection schemas
SESSIONS_SCHEMA = {
    "name": "sessions",
    "enable_nested_fields": True,
    "fields": [
        {"name": "app_name", "type": "string"},
        {"name": "user_id", "type": "string"},
        {"name": "session_id", "type": "string"},
        {"name": "composite_key", "type": "string"},  # Unique document ID
        {"name": "state", "type": "object"},
        {"name": "create_time", "type": "int64"},  # Unix timestamp microseconds
        {"name": "update_time", "type": "int64"},  # Unix timestamp microseconds
    ],
}

EVENTS_SCHEMA = {
    "name": "events",
    "enable_nested_fields": True,
    "fields": [
        {"name": "event_id", "type": "string"},  # Dedicated event ID field
        {"name": "app_name", "type": "string"},
        {"name": "user_id", "type": "string"},
        {"name": "session_id", "type": "string"},
        {"name": "composite_key", "type": "string"},  # Unique document ID
        {"name": "invocation_id", "type": "string"},
        {"name": "author", "type": "string"},
        {
            "name": "actions",
            "type": "string",
            "optional": True,
        },  # JSON; older records contain restricted legacy pickles
        {
            "name": "long_running_tool_ids_json",
            "type": "string",
            "optional": True,
        },
        {"name": "branch", "type": "string", "optional": True},
        {
            "name": "event_json",
            "type": "string",
            "optional": True,
            "index": False,
        },
        {"name": "timestamp", "type": "int64"},  # Unix timestamp microseconds
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
        {"name": "app_name", "type": "string"},  # Unique document ID
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
        {"name": "composite_key", "type": "string"},  # Unique document ID
        {"name": "state", "type": "object"},
        {"name": "update_time", "type": "int64"},
    ],
}


_ALLOWED_PICKLE_GLOBALS: frozenset[tuple[str, str]] = frozenset({
    # Builtin containers/primitives.
    ("builtins", "dict"),
    ("builtins", "list"),
    ("builtins", "set"),
    ("builtins", "tuple"),
    ("builtins", "str"),
    ("builtins", "bytes"),
    ("builtins", "bytearray"),
    ("builtins", "int"),
    ("builtins", "float"),
    ("builtins", "bool"),
    ("datetime", "datetime"),
    ("datetime", "timedelta"),
    ("datetime", "timezone"),
    # Expected pickled payload for v0 session schema events.
    ("fastapi.openapi.models", "APIKey"),
    ("fastapi.openapi.models", "APIKeyIn"),
    ("fastapi.openapi.models", "HTTPBase"),
    ("fastapi.openapi.models", "HTTPBearer"),
    ("fastapi.openapi.models", "OAuth2"),
    ("fastapi.openapi.models", "OAuthFlow"),
    ("fastapi.openapi.models", "OAuthFlowAuthorizationCode"),
    ("fastapi.openapi.models", "OAuthFlowClientCredentials"),
    ("fastapi.openapi.models", "OAuthFlowImplicit"),
    ("fastapi.openapi.models", "OAuthFlowPassword"),
    ("fastapi.openapi.models", "OAuthFlows"),
    ("fastapi.openapi.models", "OpenIdConnect"),
    ("fastapi.openapi.models", "SecurityBase"),
    ("fastapi.openapi.models", "SecurityScheme"),
    ("fastapi.openapi.models", "SecuritySchemeType"),
    ("google.adk.auth.auth_credential", "AuthCredential"),
    ("google.adk.auth.auth_credential", "AuthCredentialTypes"),
    ("google.adk.auth.auth_credential", "HttpAuth"),
    ("google.adk.auth.auth_credential", "HttpCredentials"),
    ("google.adk.auth.auth_credential", "OAuth2Auth"),
    ("google.adk.auth.auth_credential", "ServiceAccountCredential"),
    ("google.adk.auth.auth_schemes", "CustomAuthScheme"),
    ("google.adk.auth.auth_schemes", "ExtendedOAuth2"),
    ("google.adk.auth.auth_schemes", "OAuthGrantType"),
    ("google.adk.auth.auth_schemes", "OpenIdConnectWithConfig"),
    ("google.adk.auth.auth_tool", "AuthConfig"),
    ("google.adk.events.event_actions", "EventActions"),
    ("google.adk.events.event_actions", "EventCompaction"),
    ("google.adk.events.ui_widget", "UiWidget"),
    ("google.adk.tools.tool_confirmation", "ToolConfirmation"),
    ("google.genai.types", "Blob"),
    ("google.genai.types", "CodeExecutionResult"),
    ("google.genai.types", "Content"),
    ("google.genai.types", "ExecutableCode"),
    ("google.genai.types", "FileData"),
    ("google.genai.types", "FunctionCall"),
    ("google.genai.types", "FunctionResponse"),
    ("google.genai.types", "FunctionResponseBlob"),
    ("google.genai.types", "FunctionResponseFileData"),
    ("google.genai.types", "FunctionResponsePart"),
    ("google.genai.types", "Part"),
    ("google.genai.types", "PartMediaResolution"),
    ("google.genai.types", "VideoMetadata"),
})


class _LegacyActionsUnpickler(pickle.Unpickler):
  """Use upstream's legacy-action allowlist; never arbitrary pickle globals."""

  def find_class(self, module: str, name: str) -> Any:
    if (module, name) in _ALLOWED_PICKLE_GLOBALS:
      return super().find_class(module, name)
    raise pickle.UnpicklingError("Unsupported global in legacy EventActions")


class TypesenseSessionService(BaseSessionService):
  """A session service that uses Typesense for storage."""

  def __init__(
      self,
      host: str,
      api_key: str,
      port: int = 8108,
      protocol: str = "http",
      **kwargs: Any,
  ):
    """Initializes the Typesense session service.

    Args:
      host: The Typesense server hostname (e.g., "localhost" or
        "api.typesense.cloud").
      api_key: The Typesense API key for authentication.
      port: The Typesense server port. Defaults to 8108 for HTTP, use 443 for
        HTTPS.
      protocol: The protocol to use, either "http" or "https". Defaults to
        "http".
      **kwargs: Additional arguments (unused for Typesense).

    Raises:
      ValueError: If the protocol is invalid or initialization fails.
    """
    try:
      # Validate protocol
      if protocol not in ("http", "https"):
        raise ValueError(
            f"Invalid protocol '{protocol}'. Expected 'http' or 'https'."
        )

      self.client = typesense.Client({
          "nodes": [{"host": host, "port": port, "protocol": protocol}],
          "api_key": api_key,
          "connection_timeout_seconds": 2,
      })

      # Initialize collections
      self._initialize_collections()

      logger.info(
          "Typesense session service initialized with %s://%s:%s",
          protocol,
          host,
          port,
      )

    except Exception as e:
      raise ValueError(
          f"Failed to initialize Typesense session service: {e}"
      ) from e

  def _initialize_collections(self):
    """Create/migrate collections, tolerating bounded concurrent startup races."""
    for schema in [
        SESSIONS_SCHEMA,
        EVENTS_SCHEMA,
        APP_STATES_SCHEMA,
        USER_STATES_SCHEMA,
    ]:
      self._ensure_collection(schema)

  def _ensure_collection(self, schema: dict[str, Any]) -> None:
    collection_name = schema["name"]
    collection = self.client.collections[collection_name]
    for attempt in range(_COLLECTION_INIT_ATTEMPTS):
      try:
        existing = collection.retrieve()
      except typesense.exceptions.ObjectNotFound:
        try:
          self.client.collections.create(schema)
          logger.info("Created collection '%s'", collection_name)
          return
        except typesense.exceptions.ObjectAlreadyExists:
          if attempt == _COLLECTION_INIT_ATTEMPTS - 1:
            raise
      else:
        if collection_name != "events" or any(
            field["name"] == "event_json"
            for field in existing.get("fields", [])
        ):
          return
        try:
          collection.update({
              "fields": [
                  next(
                      field
                      for field in EVENTS_SCHEMA["fields"]
                      if field["name"] == "event_json"
                  )
              ]
          })
          return
        except typesense.exceptions.ObjectUnprocessable as exc:
          if (
              exc.errno != 422
              or _COLLECTION_UPDATE_BUSY not in str(exc)
              or attempt == _COLLECTION_INIT_ATTEMPTS - 1
          ):
            raise
      # A competing initializer may have completed since the conflict. Always
      # re-read its schema before deciding whether another update is necessary.
      time.sleep(min(0.1 * 2**attempt, 1.0))

  def _make_composite_key(self, *parts: str) -> str:
    """Creates a composite key for Typesense document ID."""
    return ":".join(parts)

  def _get_document(
      self, collection: str, doc_id: str
  ) -> Optional[dict[str, Any]]:
    """Direct document lookup by id (the composite key IS the doc id).

    Reads the document store, so a doc is visible immediately after its own
    write returns. The SEARCH index lags writes by a beat — looking up a
    just-created session via ``documents.search`` lost that race and surfaced
    as ``Session not found: <id>`` (Runner re-fetches the session right after
    ``create_session``) on the FIRST message of a brand-new chat.
    """
    try:
      return self.client.collections[collection].documents[doc_id].retrieve()
    except typesense.exceptions.ObjectNotFound:
      return None

  def _search_documents(
      self,
      collection: str,
      event_filter: str,
      *,
      sort_by: Optional[str] = None,
      limit: Optional[int] = None,
  ) -> list[dict[str, Any]]:
    """Read all matching pages, bounded only by an explicit caller limit."""
    documents = []
    page = 1
    per_page = min(limit, 250) if limit is not None else 250
    while True:
      params = {
          "q": "*",
          "filter_by": event_filter,
          "page": page,
          "per_page": per_page,
      }
      if sort_by:
        params["sort_by"] = sort_by
      result = self.client.collections[collection].documents.search(params)
      hits = result["hits"]
      documents.extend(hit["document"] for hit in hits)
      if limit is not None and len(documents) >= limit:
        return documents[:limit]
      if len(hits) < per_page or len(documents) >= result.get(
          "found", float("inf")
      ):
        return documents
      page += 1

  def _to_microseconds(self, timestamp: float) -> int:
    """Converts Unix timestamp to microseconds."""
    return int(timestamp * 1_000_000)

  def _from_microseconds(self, microseconds: int) -> float:
    """Converts microseconds to Unix timestamp."""
    return microseconds / 1_000_000

  def _serialize_actions(
      self, actions: Optional[EventActions]
  ) -> Optional[str]:
    """Serializes EventActions to a JSON string."""
    return actions.model_dump_json() if actions else None

  def _deserialize_actions(
      self, actions_str: Optional[str]
  ) -> Optional[EventActions]:
    """Deserializes EventActions from its stored string.

    The current format is JSON (see `_serialize_actions`). Legacy events
    stored `actions` as a base64-encoded pickle, so fall back to that format
    when JSON parsing fails — otherwise a single old event poisons the whole
    session load and the run aborts with a pydantic ``json_invalid`` error.
    If neither format decodes, drop the actions for that one event rather
    than failing the entire session.
    """
    if not actions_str:
      return None
    try:
      return EventActions.model_validate_json(actions_str)
    except ValueError:
      pass
    # Legacy format: base64-encoded pickle of an EventActions instance. The
    # unpickled object may be an OLDER schema (missing fields added since,
    # e.g. `rewind_before_invocation_id`), which would AttributeError when
    # accessed later. Re-validate it through the CURRENT model so new fields
    # are populated with their defaults.
    try:
      legacy = _LegacyActionsUnpickler(
          io.BytesIO(base64.b64decode(actions_str, validate=True))
      ).load()
    except (
        ValueError,
        binascii.Error,
        pickle.UnpicklingError,
        EOFError,
        AttributeError,
        TypeError,
    ):
      logger.warning(
          "Could not decode legacy EventActions; dropping actions for this"
          " event.",
      )
      return None
    try:
      if isinstance(legacy, EventActions):
        return EventActions.model_validate(legacy.__dict__)
      if isinstance(legacy, dict):
        return EventActions.model_validate(legacy)
    except (ValueError, TypeError, AttributeError):
      logger.warning(
          "Could not normalize legacy EventActions; dropping actions for this"
          " event.",
      )
    return None

  def _get_app_state(self, app_name: str) -> dict[str, Any]:
    """Fetches app state from Typesense (direct id lookup — see _get_document)."""
    doc = self._get_document("app_states", app_name)
    return doc["state"] if doc else {}

  def _get_user_state(self, app_name: str, user_id: str) -> dict[str, Any]:
    """Fetches user state from Typesense (direct id lookup — see _get_document)."""
    composite_key = self._make_composite_key(app_name, user_id)
    doc = self._get_document("user_states", composite_key)
    return doc["state"] if doc else {}

  def _upsert_app_state(self, app_name: str, state: dict[str, Any]):
    """Updates or inserts app state in Typesense."""
    now = self._to_microseconds(datetime.now().timestamp())
    document = {
        "id": app_name,
        "app_name": app_name,
        "state": state,
        "update_time": now,
    }
    self.client.collections["app_states"].documents.upsert(document)

  def _upsert_user_state(
      self, app_name: str, user_id: str, state: dict[str, Any]
  ):
    """Updates or inserts user state in Typesense."""
    now = self._to_microseconds(datetime.now().timestamp())
    composite_key = self._make_composite_key(app_name, user_id)
    document = {
        "id": composite_key,
        "app_name": app_name,
        "user_id": user_id,
        "composite_key": composite_key,
        "state": state,
        "update_time": now,
    }
    self.client.collections["user_states"].documents.upsert(document)

  @override
  async def create_session(
      self,
      *,
      app_name: str,
      user_id: str,
      state: Optional[dict[str, Any]] = None,
      session_id: Optional[str] = None,
  ) -> Session:
    """Creates a new session."""
    # Fetch existing states
    app_state = self._get_app_state(app_name)
    user_state = self._get_user_state(app_name, user_id)

    # Extract state deltas
    deltas = extract_state_delta(state if state else {})
    app_state_delta = deltas["app"]
    user_state_delta = deltas["user"]
    session_state = deltas["session"]

    # Apply state deltas
    app_state.update(app_state_delta)
    user_state.update(user_state_delta)

    # Generate session ID if not provided
    if not session_id:
      session_id = str(uuid.uuid4())

    # Create session document
    now = datetime.now().timestamp()
    now_micro = self._to_microseconds(now)
    composite_key = self._make_composite_key(app_name, user_id, session_id)

    session_doc = {
        "id": composite_key,
        "app_name": app_name,
        "user_id": user_id,
        "session_id": session_id,
        "composite_key": composite_key,
        "state": session_state,
        "create_time": now_micro,
        "update_time": now_micro,
    }

    try:
      self.client.collections["sessions"].documents.create(session_doc)
    except typesense.exceptions.ObjectAlreadyExists as exc:
      raise AlreadyExistsError(
          f"Session with id {session_id} already exists."
      ) from exc

    # Update app and user states if there are deltas
    if app_state_delta:
      self._upsert_app_state(app_name, app_state)
    if user_state_delta:
      self._upsert_user_state(app_name, user_id, user_state)

    # Merge states for response
    merged_state = merge_state(app_state, user_state, session_state)

    return Session(
        app_name=app_name,
        user_id=user_id,
        id=session_id,
        state=merged_state,
        events=[],
        last_update_time=now,
    )

  @override
  async def get_session(
      self,
      *,
      app_name: str,
      user_id: str,
      session_id: str,
      config: Optional[GetSessionConfig] = None,
  ) -> Optional[Session]:
    """Gets a session."""
    # Direct id lookup (doc id == composite key). A search-based lookup here
    # raced the create: the Runner re-fetches the session right after
    # create_session, before the search index caught up, and the first
    # message of a new chat died with "Session not found".
    composite_key = self._make_composite_key(app_name, user_id, session_id)
    session_doc = self._get_document("sessions", composite_key)
    if session_doc is None:
      return None

    # Build event filter
    event_filter = (
        f"app_name:={app_name} && user_id:={user_id} &&"
        f" session_id:={session_id}"
    )
    if config and config.after_timestamp is not None:
      after_micro = self._to_microseconds(config.after_timestamp)
      event_filter += f" && timestamp:>={after_micro}"

    num_recent_events = config.num_recent_events if config else None
    if num_recent_events is not None and num_recent_events < 0:
      raise ValueError("num_recent_events must be non-negative")
    events = []
    if num_recent_events != 0:
      documents = self._search_documents(
          "events",
          event_filter,
          sort_by="timestamp:desc",
          limit=num_recent_events,
      )
      events = [self._document_to_event(doc) for doc in reversed(documents)]
      # Persistence must preserve tool calls, response IDs, author/branch and
      # metadata. Provider-specific repair belongs in the LLM message layer.

    # Fetch states
    app_state = self._get_app_state(app_name)
    user_state = self._get_user_state(app_name, user_id)
    session_state = session_doc["state"]

    # Merge states
    merged_state = merge_state(app_state, user_state, session_state)

    return Session(
        app_name=app_name,
        user_id=user_id,
        id=session_id,
        state=merged_state,
        events=events,
        last_update_time=self._from_microseconds(session_doc["update_time"]),
    )

  @override
  async def list_sessions(
      self, *, app_name: str, user_id: Optional[str] = None
  ) -> ListSessionsResponse:
    """Lists all sessions for a user with pagination support."""
    event_filter = f"app_name:={app_name}"
    if user_id is not None:
      event_filter += f" && user_id:={user_id}"
    documents = self._search_documents("sessions", event_filter)
    # Match the current BaseSessionService contract: summaries omit history
    # and state, including when listing all users in an application.
    sessions = [
        Session(
            app_name=app_name,
            user_id=doc["user_id"],
            id=doc["session_id"],
            last_update_time=self._from_microseconds(doc["update_time"]),
        )
        for doc in documents
    ]

    return ListSessionsResponse(sessions=sessions)

  @override
  async def delete_session(
      self, *, app_name: str, user_id: str, session_id: str
  ) -> None:
    """Deletes a session and all its events."""
    composite_key = self._make_composite_key(app_name, user_id, session_id)

    # Delete session
    try:
      self.client.collections["sessions"].documents[composite_key].delete()
    except typesense.exceptions.ObjectNotFound:
      pass  # Session already deleted

    # Delete all events for this session
    event_filter = (
        f"app_name:={app_name} && user_id:={user_id} &&"
        f" session_id:={session_id}"
    )
    try:
      self.client.collections["events"].documents.delete(
          {"filter_by": event_filter}
      )
    except typesense.exceptions.ObjectNotFound:
      pass  # No events to delete

  @override
  async def append_event(self, session: Session, event: Event) -> Event:
    """Appends an event to a session."""
    if event.partial:
      return event

    # Get current session to check staleness
    composite_key = self._make_composite_key(
        session.app_name, session.user_id, session.id
    )
    # Direct id lookup (doc id == composite key) — same read-your-own-write
    # rationale as get_session: the first append after create_session must
    # never miss the session because the search index hasn't caught up.
    session_doc = self._get_document("sessions", composite_key)
    if session_doc is None:
      raise ValueError(f"Session {session.id} not found")

    stored_update_time = self._from_microseconds(session_doc["update_time"])

    if stored_update_time > session.last_update_time:
      raise ValueError(
          "The last_update_time provided in the session object"
          f" {datetime.fromtimestamp(session.last_update_time):%Y-%m-%d %H:%M:%S}"
          " is earlier than the update_time in storage"
          f" {datetime.fromtimestamp(stored_update_time):%Y-%m-%d %H:%M:%S}."
          " Please check if it is a stale session."
      )

    # Fetch states
    app_state = self._get_app_state(session.app_name)
    user_state = self._get_user_state(session.app_name, session.user_id)
    session_state = session_doc["state"]

    # Temp values are visible during this invocation but must not be persisted.
    self._apply_temp_state(session, event)
    event = self._trim_temp_delta_state(event)

    # Extract state delta from event
    app_state_delta = {}
    user_state_delta = {}
    session_state_delta = {}
    if event.actions and event.actions.state_delta:
      deltas = extract_state_delta(event.actions.state_delta)
      app_state_delta = deltas["app"]
      user_state_delta = deltas["user"]
      session_state_delta = deltas["session"]

    # Merge state and update storage
    if app_state_delta:
      app_state.update(app_state_delta)
      self._upsert_app_state(session.app_name, app_state)
    if user_state_delta:
      user_state.update(user_state_delta)
      self._upsert_user_state(session.app_name, session.user_id, user_state)
    if session_state_delta:
      session_state.update(session_state_delta)

    # Create event document
    event_composite_key = self._make_composite_key(
        event.id, session.app_name, session.user_id, session.id
    )
    event_doc = self._event_to_document(session, event, event_composite_key)
    self.client.collections["events"].documents.create(event_doc)

    # Update session's update_time and state
    now = datetime.now().timestamp()
    now_micro = self._to_microseconds(now)
    session_doc["state"] = session_state
    session_doc["update_time"] = now_micro
    self.client.collections["sessions"].documents.upsert(session_doc)

    # Update session object
    session.last_update_time = now

    # Also update the in-memory session
    await super().append_event(session=session, event=event)
    return event

  def _event_to_document(
      self, session: Session, event: Event, composite_key: str
  ) -> dict[str, Any]:
    """Converts an Event object to a Typesense document."""
    event_json = event.model_dump_json(exclude_none=True)
    serialized = json.loads(event_json)
    doc = {
        "id": composite_key,
        "event_id": event.id,  # Store event ID explicitly
        "composite_key": composite_key,
        "app_name": session.app_name,
        "user_id": session.user_id,
        "session_id": session.id,
        "invocation_id": event.invocation_id,
        "author": event.author,
        "actions": self._serialize_actions(event.actions),
        "timestamp": self._to_microseconds(event.timestamp),
        "event_json": event_json,
    }

    # Add optional fields
    if event.long_running_tool_ids:

      doc["long_running_tool_ids_json"] = json.dumps(
          list(event.long_running_tool_ids)
      )
    if event.branch:
      doc["branch"] = event.branch
    if event.content:
      doc["content"] = serialized["content"]
    if event.grounding_metadata:
      doc["grounding_metadata"] = serialized["grounding_metadata"]
    if event.custom_metadata:
      doc["custom_metadata"] = event.custom_metadata
    if event.partial is not None:
      doc["partial"] = event.partial
    if event.turn_complete is not None:
      doc["turn_complete"] = event.turn_complete
    if event.error_code:
      doc["error_code"] = event.error_code
    if event.error_message:
      doc["error_message"] = event.error_message
    if event.interrupted is not None:
      doc["interrupted"] = event.interrupted

    return doc

  def _document_to_event(self, doc: dict[str, Any]) -> Event:
    """Converts a Typesense document to an Event object."""

    if doc.get("event_json"):
      return Event.model_validate_json(doc["event_json"])

    long_running_tool_ids = set()
    if (
        "long_running_tool_ids_json" in doc
        and doc["long_running_tool_ids_json"]
    ):
      long_running_tool_ids = set(json.loads(doc["long_running_tool_ids_json"]))

    return Event(
        id=doc["event_id"],  # Read from dedicated event_id field
        invocation_id=doc["invocation_id"],
        author=doc["author"],
        branch=doc.get("branch"),
        actions=self._deserialize_actions(doc.get("actions")) or EventActions(),
        timestamp=self._from_microseconds(doc["timestamp"]),
        content=_session_util.decode_content(doc.get("content")),
        long_running_tool_ids=long_running_tool_ids,
        partial=doc.get("partial"),
        turn_complete=doc.get("turn_complete"),
        error_code=doc.get("error_code"),
        error_message=doc.get("error_message"),
        interrupted=doc.get("interrupted"),
        grounding_metadata=_session_util.decode_grounding_metadata(
            doc.get("grounding_metadata")
        ),
        custom_metadata=doc.get("custom_metadata"),
    )
