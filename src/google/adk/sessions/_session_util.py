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
"""Utility functions for session service."""
from __future__ import annotations

import copy
from typing import Any
from typing import Optional

from google.genai import types

from .state import State


def decode_content(
    content: Optional[dict[str, Any]],
) -> Optional[types.Content]:
  """Decodes a content object from a JSON dictionary."""
  if not content:
    return None
  return types.Content.model_validate(content)


def decode_grounding_metadata(
    grounding_metadata: Optional[dict[str, Any]],
) -> Optional[types.GroundingMetadata]:
  """Decodes a grounding metadata object from a JSON dictionary."""
  if not grounding_metadata:
    return None
  return types.GroundingMetadata.model_validate(grounding_metadata)


def extract_state_delta(
    state: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
  """Extracts state deltas for app, user, and session scopes.

  Args:
    state: The state dictionary containing mixed scopes.

  Returns:
    A tuple of (app_state_delta, user_state_delta, session_state_delta).
  """
  app_state_delta = {}
  user_state_delta = {}
  session_state_delta = {}
  if state:
    for key in state.keys():
      if key.startswith(State.APP_PREFIX):
        app_state_delta[key.removeprefix(State.APP_PREFIX)] = state[key]
      elif key.startswith(State.USER_PREFIX):
        user_state_delta[key.removeprefix(State.USER_PREFIX)] = state[key]
      elif not key.startswith(State.TEMP_PREFIX):
        session_state_delta[key] = state[key]
  return app_state_delta, user_state_delta, session_state_delta


def merge_state(
    app_state: dict[str, Any],
    user_state: dict[str, Any],
    session_state: dict[str, Any],
) -> dict[str, Any]:
  """Merges app, user, and session states into a single state dictionary.

  Args:
    app_state: The app-level state.
    user_state: The user-level state.
    session_state: The session-level state.

  Returns:
    A merged state dictionary with appropriate prefixes.
  """
  merged_state = copy.deepcopy(session_state)
  for key in app_state.keys():
    merged_state[State.APP_PREFIX + key] = app_state[key]
  for key in user_state.keys():
    merged_state[State.USER_PREFIX + key] = user_state[key]
  return merged_state
