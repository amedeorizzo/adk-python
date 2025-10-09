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
from datetime import datetime
import logging
import pickle
from typing import Any
from typing import Optional
import uuid
import json

import typesense
from typing_extensions import override

from . import _session_util
from ..events.event import Event
from ..events.event_actions import EventActions
from ._session_util import extract_state_delta
from ._session_util import merge_state
from .base_session_service import BaseSessionService
from .base_session_service import GetSessionConfig
from .base_session_service import ListSessionsResponse
from .session import Session

logger = logging.getLogger("google_adk." + __name__)

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
        },  # Pickled and base64 encoded
        {
            "name": "long_running_tool_ids_json",
            "type": "string",
            "optional": True,
        },
        {"name": "branch", "type": "string", "optional": True},
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
    """Creates Typesense collections if they don't exist."""
    for schema in [
        SESSIONS_SCHEMA,
        EVENTS_SCHEMA,
        APP_STATES_SCHEMA,
        USER_STATES_SCHEMA,
    ]:
      collection_name = schema["name"]
      try:
        self.client.collections[collection_name].retrieve()
        logger.debug("Collection '%s' already exists", collection_name)
      except typesense.exceptions.ObjectNotFound:
        self.client.collections.create(schema)
        logger.info("Created collection '%s'", collection_name)

  def _make_composite_key(self, *parts: str) -> str:
    """Creates a composite key for Typesense document ID."""
    return ":".join(parts)

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
    """Deserializes EventActions from a JSON string."""
    if not actions_str:
      return None
    return EventActions.model_validate_json(actions_str)

  def _get_app_state(self, app_name: str) -> dict[str, Any]:
    """Fetches app state from Typesense."""
    try:
      search_results = self.client.collections["app_states"].documents.search({
          "q": "*",
          "filter_by": f"app_name:={app_name}",
          "per_page": 1,
      })
      if search_results["found"] > 0:
        return search_results["hits"][0]["document"]["state"]
      return {}
    except typesense.exceptions.ObjectNotFound:
      return {}

  def _get_user_state(self, app_name: str, user_id: str) -> dict[str, Any]:
    """Fetches user state from Typesense."""
    try:
      composite_key = self._make_composite_key(app_name, user_id)
      search_results = self.client.collections["user_states"].documents.search({
          "q": "*",
          "filter_by": f"composite_key:={composite_key}",
          "per_page": 1,
      })
      if search_results["found"] > 0:
        return search_results["hits"][0]["document"]["state"]
      return {}
    except typesense.exceptions.ObjectNotFound:
      return {}

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
    app_state_delta, user_state_delta, session_state = extract_state_delta(
        state if state else {}
    )

    # Apply state deltas
    app_state.update(app_state_delta)
    user_state.update(user_state_delta)

    # Update app and user states if there are deltas
    if app_state_delta:
      self._upsert_app_state(app_name, app_state)
    if user_state_delta:
      self._upsert_user_state(app_name, user_id, user_state)

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

    self.client.collections["sessions"].documents.create(session_doc)

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
    # Search for session
    composite_key = self._make_composite_key(app_name, user_id, session_id)
    try:
      search_results = self.client.collections["sessions"].documents.search({
          "q": "*",
          "filter_by": f"composite_key:={composite_key}",
          "per_page": 1,
      })
      if search_results["found"] == 0:
        return None

      session_doc = search_results["hits"][0]["document"]
    except typesense.exceptions.ObjectNotFound:
      return None

    # Build event filter
    event_filter = (
        f"app_name:={app_name} && user_id:={user_id} &&"
        f" session_id:={session_id}"
    )
    if config and config.after_timestamp:
      after_micro = self._to_microseconds(config.after_timestamp)
      event_filter += f" && timestamp:>={after_micro}"

    # Search for events
    event_search_params = {
        "q": "*",
        "filter_by": event_filter,
        "sort_by": "timestamp:desc",
    }
    if config and config.num_recent_events:
      event_search_params["per_page"] = config.num_recent_events

    event_results = self.client.collections["events"].documents.search(
        event_search_params
    )

    # Convert events
    events = []
    for hit in reversed(event_results["hits"]):
      event_doc = hit["document"]
      events.append(self._document_to_event(event_doc))

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
      self, *, app_name: str, user_id: str
  ) -> ListSessionsResponse:
    """Lists all sessions for a user with pagination support."""
    # Fetch states once
    app_state = self._get_app_state(app_name)
    user_state = self._get_user_state(app_name, user_id)

    sessions = []
    page = 1
    per_page = 250  # Typesense max

    while True:
      # Search for sessions with pagination
      search_results = self.client.collections["sessions"].documents.search({
          "q": "*",
          "filter_by": f"app_name:={app_name} && user_id:={user_id}",
          "per_page": per_page,
          "page": page,
      })

      # Break if no results found
      if not search_results["hits"]:
        break

      # Process results
      for hit in search_results["hits"]:
        session_doc = hit["document"]
        session_state = session_doc["state"]
        merged_state = merge_state(app_state, user_state, session_state)

        sessions.append(
            Session(
                app_name=app_name,
                user_id=user_id,
                id=session_doc["session_id"],
                state=merged_state,
                events=[],
                last_update_time=self._from_microseconds(
                    session_doc["update_time"]
                ),
            )
        )

      # Check if there are more pages
      if len(search_results["hits"]) < per_page:
        break

      page += 1

    return ListSessionsResponse(sessions=sessions)

  @override
  async def delete_session(
      self, app_name: str, user_id: str, session_id: str
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
    try:
      search_results = self.client.collections["sessions"].documents.search({
          "q": "*",
          "filter_by": f"composite_key:={composite_key}",
          "per_page": 1,
      })
      if search_results["found"] == 0:
        raise ValueError(f"Session {session.id} not found")

      session_doc = search_results["hits"][0]["document"]
      stored_update_time = self._from_microseconds(session_doc["update_time"])

      if stored_update_time > session.last_update_time:
        raise ValueError(
            "The last_update_time provided in the session object"
            f" {datetime.fromtimestamp(session.last_update_time):%Y-%m-%d %H:%M:%S} is"
            " earlier than the update_time in storage"
            f" {datetime.fromtimestamp(stored_update_time):%Y-%m-%d %H:%M:%S}."
            " Please check if it is a stale session."
        )
    except typesense.exceptions.ObjectNotFound:
      raise ValueError(f"Session {session.id} not found")

    # Fetch states
    app_state = self._get_app_state(session.app_name)
    user_state = self._get_user_state(session.app_name, session.user_id)
    session_state = session_doc["state"]

    # Extract state delta from event
    app_state_delta = {}
    user_state_delta = {}
    session_state_delta = {}
    if event.actions and event.actions.state_delta:
      app_state_delta, user_state_delta, session_state_delta = (
          extract_state_delta(event.actions.state_delta)
      )

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
    }

    # Add optional fields
    if event.long_running_tool_ids:

      doc["long_running_tool_ids_json"] = json.dumps(
          list(event.long_running_tool_ids)
      )
    if event.branch:
      doc["branch"] = event.branch
    if event.content:
      doc["content"] = event.content.model_dump(exclude_none=True, mode="json")
    if event.grounding_metadata:
      doc["grounding_metadata"] = event.grounding_metadata.model_dump(
          exclude_none=True, mode="json"
      )
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
        actions=self._deserialize_actions(doc.get("actions")),
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
