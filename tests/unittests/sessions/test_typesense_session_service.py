# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# https://www.apache.org/licenses/LICENSE-2.0
"""Session contract regressions using an isolated Typesense document store."""

from __future__ import annotations

import copy
from types import SimpleNamespace
from unittest import mock

from google.adk.errors.already_exists_error import AlreadyExistsError
from google.adk.events.event import Event
from google.adk.events.event_actions import EventActions
from google.adk.sessions.base_session_service import GetSessionConfig
from google.adk.sessions.typesense_session_service import EVENTS_SCHEMA
from google.adk.sessions.typesense_session_service import TypesenseSessionService
from google.genai import types
import pytest
import typesense


class _Document:

  def __init__(self, documents, key):
    self.documents = documents
    self.key = key

  def retrieve(self):
    if self.key not in self.documents.data:
      raise typesense.exceptions.ObjectNotFound('missing')
    return copy.deepcopy(self.documents.data[self.key])

  def delete(self):
    self.documents.data.pop(self.key, None)


class _Documents:
  """Mock only Typesense: writes are immediately readable by ID."""

  def __init__(self, data=None):
    self.data = data or {}
    self.calls = []
    self.index_lag = False

  def __getitem__(self, key):
    return _Document(self, key)

  def create(self, doc):
    if doc['id'] in self.data:
      raise typesense.exceptions.ObjectAlreadyExists('exists')
    self.data[doc['id']] = copy.deepcopy(doc)
    return copy.deepcopy(doc)

  def upsert(self, doc):
    self.data[doc['id']] = copy.deepcopy(doc)
    return copy.deepcopy(doc)

  def search(self, params):
    self.calls.append(params.copy())
    if self.index_lag:
      return {'found': 0, 'hits': []}
    docs = list(self.data.values())
    for term in params['filter_by'].split(' && '):
      if ':>=' in term:
        key, value = term.split(':>=')
        docs = [doc for doc in docs if doc[key] >= int(value)]
      else:
        key, value = term.split(':=')
        docs = [doc for doc in docs if doc[key] == value]
    if params.get('sort_by'):
      docs.sort(key=lambda doc: doc['timestamp'], reverse=True)
    per_page = params['per_page']
    start = (params['page'] - 1) * per_page
    return {
        'found': len(docs),
        'hits': [
            {'document': copy.deepcopy(doc)}
            for doc in docs[start : start + per_page]
        ],
    }

  def delete(self, params):
    terms = [term.split(':=') for term in params['filter_by'].split(' && ')]
    for key, doc in list(self.data.items()):
      if all(doc[field] == value for field, value in terms):
        del self.data[key]


@pytest.fixture
def service():
  instance = object.__new__(TypesenseSessionService)
  instance.client = SimpleNamespace(
      collections={
          name: SimpleNamespace(documents=_Documents())
          for name in ('sessions', 'events', 'app_states', 'user_states')
      }
  )
  return instance


async def _create(service, session_id='session', user_id='user', state=None):
  return await service.create_session(
      app_name='app', user_id=user_id, session_id=session_id, state=state
  )


async def _get(service, config=None):
  return await service.get_session(
      app_name='app', user_id='user', session_id='session', config=config
  )


@pytest.mark.asyncio
async def test_create_then_get_ignores_session_and_state_search_index_lag(
    service,
):
  for collection in service.client.collections.values():
    collection.documents.index_lag = True
  created = await _create(service, state={'app:a': 1, 'user:u': 2, 's': 3})
  loaded = await _get(service)
  assert loaded.id == created.id
  assert loaded.state == created.state == {'app:a': 1, 'user:u': 2, 's': 3}
  assert not service.client.collections['sessions'].documents.calls
  assert not service.client.collections['app_states'].documents.calls
  assert not service.client.collections['user_states'].documents.calls


@pytest.mark.asyncio
async def test_full_history_and_recent_window_paginate_in_chronological_order(
    service,
):
  session = await _create(service)
  documents = service.client.collections['events'].documents
  for i in range(521):
    event = Event(
        id=f'event-{i}',
        author='user',
        invocation_id=f'inv-{i}',
        timestamp=float(i),
        content=types.Content(role='user', parts=[types.Part(text=str(i))]),
    )
    documents.create(service._event_to_document(session, event, f'key-{i}'))
  loaded = await _get(service)
  assert [event.id for event in loaded.events] == [
      f'event-{i}' for i in range(521)
  ]
  assert [call['page'] for call in documents.calls] == [1, 2, 3]
  documents.calls.clear()
  loaded = await _get(service, GetSessionConfig(num_recent_events=300))
  assert [event.id for event in loaded.events] == [
      f'event-{i}' for i in range(221, 521)
  ]
  assert [call['page'] for call in documents.calls] == [1, 2]


@pytest.mark.asyncio
async def test_zero_history_and_timestamp_zero_filter_are_respected(service):
  session = await _create(service)
  await service.append_event(session, Event(author='user', timestamp=-1.0))
  documents = service.client.collections['events'].documents
  assert (
      await _get(service, GetSessionConfig(num_recent_events=0))
  ).events == []
  assert documents.calls == []
  assert (await _get(service, GetSessionConfig(after_timestamp=0))).events == []
  assert 'timestamp:>=0' in documents.calls[-1]['filter_by']
  with pytest.raises(ValueError, match='non-negative'):
    await _get(service, GetSessionConfig(num_recent_events=-1))


@pytest.mark.asyncio
async def test_roundtrip_preserves_function_pairs_branches_metadata_and_bytes(
    service,
):
  session = await _create(service)
  events = [
      Event(
          author='user',
          timestamp=1,
          invocation_id='turn',
          content=types.Content(
              role='user', parts=[types.Part(text='analyse')]
          ),
      ),
      Event(
          author='analyst',
          timestamp=2,
          invocation_id='turn',
          branch='root.analyst',
          content=types.Content(
              role='model',
              parts=[
                  types.Part(
                      function_call=types.FunctionCall(
                          name='read', id='call-id', args={'node': 'x'}
                      ),
                      thought_signature=b'\xff\x00',
                  )
              ],
          ),
          usage_metadata=types.GenerateContentResponseUsageMetadata(
              prompt_token_count=20
          ),
          custom_metadata={'request': 'abc'},
          model_version='sonnet-test',
          finish_reason=types.FinishReason.STOP,
          long_running_tool_ids={'call-id'},
      ),
      Event(
          author='analyst',
          timestamp=3,
          invocation_id='turn',
          branch='root.analyst',
          content=types.Content(
              role='user',
              parts=[
                  types.Part(
                      function_response=types.FunctionResponse(
                          name='read', id='call-id', response={'ok': True}
                      )
                  )
              ],
          ),
      ),
      Event(
          author='analyst',
          timestamp=4,
          invocation_id='turn',
          content=types.Content(role='model', parts=[types.Part(text='Done')]),
          actions=EventActions(
              state_delta={'answer': 42}, artifact_delta={'report': 1}
          ),
      ),
  ]
  for event in events:
    await service.append_event(session, event)
  loaded = await _get(service)
  assert [event.model_dump() for event in loaded.events] == [
      event.model_dump() for event in events
  ]
  assert loaded.events[1].get_function_calls()[0].id == 'call-id'
  assert loaded.events[2].get_function_responses()[0].id == 'call-id'
  assert loaded.state['answer'] == 42
  assert loaded.last_update_time == pytest.approx(session.last_update_time)


@pytest.mark.asyncio
async def test_state_scopes_and_temporary_values_match_upstream_contract(
    service,
):
  session = await _create(
      service, state={'s': 1, 'app:a': 2, 'user:u': 3, 'temp:discard': 9}
  )
  event = Event(
      author='agent',
      actions=EventActions(
          state_delta={'s': 4, 'app:a': 5, 'user:u': 6, 'temp:now': 7}
      ),
  )
  await service.append_event(session, event)
  assert session.state == {'s': 4, 'app:a': 5, 'user:u': 6, 'temp:now': 7}
  loaded = await _get(service)
  assert loaded.state == {'s': 4, 'app:a': 5, 'user:u': 6}
  assert loaded.events[0].actions.state_delta == {
      's': 4,
      'app:a': 5,
      'user:u': 6,
  }


@pytest.mark.asyncio
async def test_duplicate_create_never_overwrites_existing_state_or_history(
    service,
):
  session = await _create(service, state={'s': 1, 'app:a': 2})
  await service.append_event(session, Event(author='user'))
  with pytest.raises(AlreadyExistsError):
    await _create(service, state={'s': 99, 'app:a': 99})
  loaded = await _get(service)
  assert loaded.state == {'s': 1, 'app:a': 2}
  assert len(loaded.events) == 1


@pytest.mark.asyncio
async def test_app_wide_list_omits_state_and_history_and_preserves_user_identity(
    service,
):
  await _create(service, state={'s': 1})
  await _create(
      service, session_id='other', user_id='other-user', state={'s': 2}
  )
  listed = await service.list_sessions(app_name='app')
  assert {(session.id, session.user_id) for session in listed.sessions} == {
      ('session', 'user'),
      ('other', 'other-user'),
  }
  assert all(
      session.state == {} and session.events == []
      for session in listed.sessions
  )
  assert (
      len(
          (await service.list_sessions(app_name='app', user_id='user')).sessions
      )
      == 1
  )


@pytest.mark.asyncio
async def test_legacy_json_document_retains_calls_and_falls_back_to_default_actions(
    service,
):
  session = await _create(service)
  event = Event(
      author='agent',
      content=types.Content(
          role='model',
          parts=[
              types.Part(
                  function_call=types.FunctionCall(
                      id='legacy-call', name='read'
                  )
              )
          ],
      ),
  )
  doc = service._event_to_document(session, event, 'legacy')
  doc.pop('event_json')
  doc['actions'] = 'corrupt legacy payload'
  service.client.collections['events'].documents.create(doc)
  loaded = await _get(service)
  assert len(loaded.events) == 1
  assert loaded.events[0].get_function_calls()[0].id == 'legacy-call'
  assert loaded.events[0].actions == EventActions()


def test_schema_upgrade_adds_only_optional_unindexed_event_json():
  service = object.__new__(TypesenseSessionService)
  collections = {
      name: mock.Mock()
      for name in ('sessions', 'events', 'app_states', 'user_states')
  }
  for collection in collections.values():
    collection.retrieve.return_value = {'fields': []}
  service.client = SimpleNamespace(collections=collections)
  service._initialize_collections()
  field = next(
      field
      for field in EVENTS_SCHEMA['fields']
      if field['name'] == 'event_json'
  )
  assert field == {
      'name': 'event_json',
      'type': 'string',
      'optional': True,
      'index': False,
  }
  collections['events'].update.assert_called_once_with({'fields': [field]})
  for name in ('sessions', 'app_states', 'user_states'):
    collections[name].update.assert_not_called()
  collections['events'].retrieve.return_value = {'fields': [field]}
  collections['events'].update.reset_mock()
  service._initialize_collections()
  collections['events'].update.assert_not_called()


@pytest.mark.asyncio
async def test_append_after_create_uses_direct_state_lookup_during_index_lag(
    service,
):
  session = await _create(service, state={'app:a': 1})
  for name in ('sessions', 'app_states', 'user_states'):
    service.client.collections[name].documents.index_lag = True
  event = Event(
      author='agent',
      actions=EventActions(state_delta={'app:a': 2, 'user:u': 3, 's': 4}),
  )
  await service.append_event(session, event)
  loaded = await _get(service)
  assert loaded.state == {'app:a': 2, 'user:u': 3, 's': 4}
  assert loaded.events[0].id == event.id


@pytest.mark.asyncio
async def test_distinct_sessions_preserve_shared_state_and_reject_stale_append(
    service,
):
  import asyncio

  first = await _create(service, state={'app:initial': 1, 'user:initial': 1})
  second = await _create(service, session_id='other')
  stale = await _get(service)
  await asyncio.gather(
      service.append_event(
          first,
          Event(
              author='agent',
              actions=EventActions(state_delta={'app:one': 1, 'user:one': 1}),
          ),
      ),
      service.append_event(
          second,
          Event(
              author='agent',
              actions=EventActions(state_delta={'app:two': 2, 'user:two': 2}),
          ),
      ),
  )
  loaded = await _get(service)
  assert loaded.state == {
      'app:initial': 1,
      'user:initial': 1,
      'app:one': 1,
      'user:one': 1,
      'app:two': 2,
      'user:two': 2,
  }
  with pytest.raises(ValueError, match='stale session'):
    await service.append_event(stale, Event(author='agent'))
  assert len((await _get(service)).events) == 1
