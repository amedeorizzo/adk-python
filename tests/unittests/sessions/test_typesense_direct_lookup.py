"""Regression tests: session lookups must use DIRECT id retrieval, not search.

The Typesense search index lags writes by a beat. ``Runner.run_async``
re-fetches the session immediately after ``create_session``; when
``get_session`` looked the doc up via ``documents.search`` it lost that
read-your-own-write race and the FIRST message of a brand-new chat died with
``Session not found: <id>`` (observed live on prod, 2026-07-27).

The fake client below simulates exactly that window: the doc EXISTS in the
document store (direct ``documents[id].retrieve()`` works) but the search
index hasn't caught up (``documents.search`` finds nothing). The service must
still resolve the session.
"""

import asyncio

import typesense

from google.adk.sessions.typesense_session_service import (
    TypesenseSessionService,
)


class _FakeDocumentHandle:

  def __init__(self, doc):
    self._doc = doc

  def retrieve(self):
    if self._doc is None:
      raise typesense.exceptions.ObjectNotFound("not found")
    return self._doc


class _FakeDocuments:
  """Docs are visible by id; search NEVER finds anything (index lag)."""

  def __init__(self, docs_by_id):
    self._docs = docs_by_id

  def __getitem__(self, doc_id):
    return _FakeDocumentHandle(self._docs.get(doc_id))

  def search(self, _params):
    return {"found": 0, "hits": []}


class _FakeCollection:

  def __init__(self, docs_by_id):
    self.documents = _FakeDocuments(docs_by_id)


class _FakeCollections:

  def __init__(self, data):
    self._data = data

  def __getitem__(self, name):
    return _FakeCollection(self._data.get(name, {}))


class _FakeClient:

  def __init__(self, data):
    self.collections = _FakeCollections(data)


def _service(data) -> TypesenseSessionService:
  svc = object.__new__(TypesenseSessionService)
  svc.client = _FakeClient(data)
  return svc


SESSION_KEY = "desks:user-1:sess-1"
SESSION_DOC = {
    "id": SESSION_KEY,
    "app_name": "desks",
    "user_id": "user-1",
    "session_id": "sess-1",
    "composite_key": SESSION_KEY,
    "state": {"lang": "it"},
    "create_time": 1_000_000,
    "update_time": 1_000_000,
}


def test_get_session_resolves_before_search_index_catches_up():
  svc = _service({"sessions": {SESSION_KEY: SESSION_DOC}})
  session = asyncio.run(
      svc.get_session(app_name="desks", user_id="user-1", session_id="sess-1")
  )
  assert session is not None
  assert session.id == "sess-1"
  assert session.state.get("lang") == "it"


def test_get_session_missing_doc_returns_none():
  svc = _service({"sessions": {}})
  session = asyncio.run(
      svc.get_session(app_name="desks", user_id="user-1", session_id="sess-1")
  )
  assert session is None


def test_state_getters_use_direct_lookup():
  svc = _service({
      "app_states": {"desks": {"id": "desks", "state": {"a": 1}}},
      "user_states": {
          "desks:user-1": {"id": "desks:user-1", "state": {"u": 2}}
      },
  })
  assert svc._get_app_state("desks") == {"a": 1}
  assert svc._get_user_state("desks", "user-1") == {"u": 2}
  assert svc._get_app_state("missing") == {}
  assert svc._get_user_state("desks", "missing") == {}


def test_get_document_direct_lookup():
  """The shared helper append_event/get_session rely on: a fresh doc is
  visible by id even when search finds nothing; a missing doc is None."""
  svc = _service({"sessions": {SESSION_KEY: SESSION_DOC}})
  doc = svc._get_document("sessions", SESSION_KEY)
  assert doc is not None
  assert doc["session_id"] == "sess-1"
  assert svc._get_document("sessions", "desks:user-1:other") is None
