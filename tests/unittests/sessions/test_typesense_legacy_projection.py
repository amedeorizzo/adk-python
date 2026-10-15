# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# https://www.apache.org/licenses/LICENSE-2.0

"""Full-fidelity current events and transport-free legacy content."""

from __future__ import annotations

import json
import unittest

from google.adk.events.event import Event
from google.adk.events.event_actions import EventActions
from google.adk.sessions.session import Session
from google.adk.sessions.typesense_session_service import TypesenseSessionService
from google.genai import types


class TypesenseLegacyProjectionTests(unittest.TestCase):

  def setUp(self):
    self.service = object.__new__(TypesenseSessionService)
    self.session = Session(
        id="synthetic-session",
        app_name="synthetic-app",
        user_id="synthetic-user",
    )

  def event(self, parts):
    return Event(
        id="synthetic-event",
        invocation_id="synthetic-invocation",
        author="synthetic-scientist",
        timestamp=1234.5,
        content=types.Content(role="model", parts=parts),
        actions=EventActions(
            state_delta={"count": 2},
            artifact_delta={"public-report": 1},
            transfer_to_agent="synthetic-reviewer",
        ),
    )

  def document(self, event):
    return self.service._event_to_document(self.session, event, "synthetic-key")

  def test_signed_event_restores_exact_bytes_from_full_json(self):
    signature = json.dumps({
        "marker": "keix-anthropic-thinking-v1",
        "model": "claude-sonnet-5-5",
        "tool_ids": ["synthetic-call"],
        "blocks": [{"type": "thinking", "signature": "synthetic-public"}],
    }).encode()
    event = self.event([
        types.Part(thought=True, thought_signature=signature),
        types.Part(text="Visible public explanation"),
        types.Part(
            function_call=types.FunctionCall(
                id="synthetic-call", name="public_sum", args={"values": [2, 3]}
            )
        ),
    ])
    before = event.model_dump_json(exclude_none=True)
    document = self.document(event)
    self.assertEqual(document["event_json"], before)
    restored = self.service._document_to_event(document)
    self.assertEqual(restored.content.parts[0].thought_signature, signature)
    self.assertEqual(
        restored.content.parts[2].function_call.id, "synthetic-call"
    )
    self.assertEqual(
        document["content"]["parts"],
        [
            {"text": "Visible public explanation"},
            {
                "function_call": {
                    "id": "synthetic-call",
                    "name": "public_sum",
                    "args": {"values": [2, 3]},
                }
            },
        ],
    )
    self.assertEqual(event.model_dump_json(exclude_none=True), before)

  def test_signature_on_visible_text_and_tool_preserves_payload(self):
    event = self.event([
        types.Part(
            text="Visible signed text", thought_signature=b"public-text"
        ),
        types.Part(
            function_call=types.FunctionCall(
                id="synthetic-call", name="public_sum", args={"count": 2}
            ),
            thought_signature=b"public-tool",
        ),
        types.Part(
            function_response=types.FunctionResponse(
                id="synthetic-call", name="public_sum", response={"total": 5}
            ),
            thought_signature=b"public-response",
        ),
    ])
    original = json.loads(event.model_dump_json(exclude_none=True))["content"]
    expected = {
        **original,
        "parts": [
            {
                key: value
                for key, value in part.items()
                if key != "thought_signature"
            }
            for part in original["parts"]
        ],
    }
    document = self.document(event)
    self.assertEqual(document["content"], expected)
    restored = self.service._document_to_event(document)
    self.assertEqual(
        restored.content.parts[0].thought_signature, b"public-text"
    )
    self.assertEqual(
        restored.content.parts[1].thought_signature, b"public-tool"
    )

  def test_signature_only_and_flag_only_do_not_leave_empty_legacy_parts(self):
    event = self.event([
        types.Part(thought_signature=b"public-signature"),
        types.Part(thought=False),
    ])
    document = self.document(event)
    self.assertNotIn("content", document)
    restored = self.service._document_to_event(document)
    self.assertEqual(len(restored.content.parts), 2)
    self.assertEqual(
        restored.content.parts[0].thought_signature, b"public-signature"
    )

  def test_internal_thought_text_is_not_published_as_legacy_visible_text(self):
    event = self.event([
        types.Part(thought=True, text="Public synthetic internal scratch"),
        types.Part(text="Visible conclusion"),
    ])
    document = self.document(event)
    self.assertEqual(
        document["content"]["parts"], [{"text": "Visible conclusion"}]
    )
    restored = self.service._document_to_event(document)
    self.assertTrue(restored.content.parts[0].thought)
    self.assertEqual(
        restored.content.parts[0].text, "Public synthetic internal scratch"
    )

  def test_thought_only_event_remains_complete_in_current_reader(self):
    event = self.event([
        types.Part(thought=True, thought_signature=b"public-thought-only"),
    ])
    original = event.model_dump_json(exclude_none=True)
    document = self.document(event)
    self.assertNotIn("content", document)
    self.assertEqual(document["event_json"], original)
    restored = self.service._document_to_event(document)
    self.assertEqual(
        restored.content.parts[0].thought_signature, b"public-thought-only"
    )

  def test_business_keys_inside_tool_payloads_are_not_redacted(self):
    payload = {"thought": "business category", "thought_signature": "column"}
    event = self.event([
        types.Part(
            function_call=types.FunctionCall(name="public_echo", args=payload),
            thought_signature=b"transport-call",
        ),
        types.Part(
            function_response=types.FunctionResponse(
                name="public_echo", response=payload
            ),
            thought_signature=b"transport-response",
        ),
    ])
    document = self.document(event)
    self.assertEqual(
        document["content"]["parts"][0]["function_call"]["args"], payload
    )
    self.assertEqual(
        document["content"]["parts"][1]["function_response"]["response"],
        payload,
    )

  def test_ordinary_text_and_upload_and_actions_are_unchanged(self):
    event = self.event([
        types.Part(text="Public upload"),
        types.Part(
            inline_data=types.Blob(
                mime_type="text/plain", data=b"public-synthetic-file"
            )
        ),
    ])
    original = event.model_dump_json(exclude_none=True)
    document = self.document(event)
    self.assertEqual(document["content"], json.loads(original)["content"])
    self.assertEqual(
        json.loads(document["actions"]),
        json.loads(event.actions.model_dump_json()),
    )
    self.assertEqual(document["id"], "synthetic-key")
    self.assertEqual(document["event_id"], event.id)
    self.assertEqual(event.model_dump_json(exclude_none=True), original)


if __name__ == "__main__":
  unittest.main(verbosity=2)
