"""Regression tests for TypesenseSessionService._deserialize_actions.

Legacy events stored ``actions`` as base64-encoded pickle; the current format
is JSON. The loader must read BOTH so an old event doesn't abort the whole
session load (which surfaced to users as a generic processing error).
"""

import base64
import pickle

from google.adk.events.event_actions import EventActions
from google.adk.sessions.typesense_session_service import (
    TypesenseSessionService,
)


def _service() -> TypesenseSessionService:
  # _deserialize_actions is stateless; bypass __init__ so the test doesn't
  # need a live Typesense connection.
  return object.__new__(TypesenseSessionService)


def test_deserialize_json_format():
  svc = _service()
  actions = EventActions(transfer_to_agent="data_scientist")
  out = svc._deserialize_actions(actions.model_dump_json())
  assert out is not None
  assert out.transfer_to_agent == "data_scientist"


def test_deserialize_legacy_base64_pickle():
  svc = _service()
  actions = EventActions(transfer_to_agent="data_scientist")
  legacy = base64.b64encode(pickle.dumps(actions)).decode("ascii")
  out = svc._deserialize_actions(legacy)
  assert out is not None
  assert out.transfer_to_agent == "data_scientist"


def test_deserialize_none_or_empty():
  svc = _service()
  assert svc._deserialize_actions(None) is None
  assert svc._deserialize_actions("") is None


def test_deserialize_garbage_is_dropped_not_raised():
  svc = _service()
  # Neither valid JSON nor a valid base64 pickle → drop actions, never raise.
  assert svc._deserialize_actions("!!! not json, not a pickle !!!") is None
