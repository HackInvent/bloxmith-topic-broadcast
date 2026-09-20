# -----------------------------------------------------------------------------
# Role: Bridges one graph input/output pair through one run-scoped topic.
# File Name: block.py
# Author: Alexandre EL
# Email: alex@hackinvent.com
# Created Date: 2026-08-25
# -----------------------------------------------------------------------------

from __future__ import annotations

from collections.abc import Mapping
from html import escape
from typing import Any
import json

from bloxsmith_app.block_api import (
    BlockDefinition,
    BlockRuntimeContext,
    BlockRuntimeOutput,
    BlockRuntimePreparation,
    BlockRuntimePreparationContext,
    BlockRuntimeResult,
    RuntimeTopicBinding,
    RuntimeTopicClient,
    RuntimeTopicEvent,
    RuntimeTopicPublishError,
    render_inspector_template,
    render_node_card_template,
    TEXT_PLAIN,
)


DEFAULT_TOPIC = "workflow.events"


# Functional behavior:
# FB1 - Declare one run-scoped publish/subscribe binding in Active Runtime, including this node's own publications.
# FB2 - Publish every graph input event to the configured topic while preserving its value and content type.
# FB3 - Relay each received topic event to the single declared graph output port.
# FB4 - Relay graph input events locally to the output in centralized simulation, where Runtime Topics is unavailable.
# FB5 - Own topic validation, the fixed optional input/output contract, and visible block UI inside this package.
class TopicBroadcastBlock(BlockDefinition):
    """Autonomous single-input/single-output bridge for run-scoped Runtime Topics."""

    kind = "topic_broadcast"

    def render_node_card(self, *, node: dict[str, Any], payload: dict[str, Any] | None = None) -> dict[str, Any]:
        """Render the topic and fixed one-input/one-output role on the canvas card."""

        del payload
        topic = self.topic_name(node.get("config") if isinstance(node.get("config"), dict) else {})
        return render_node_card_template(
            block=self,
            node=node,
            node_classes=["topic-broadcast-node"],
            replacements={
                "title": node.get("title") or self.default_title(),
                "topic": topic or "topic non configuré",
                "flow": "1 entrée · 1 sortie",
            },
        )

    def render_inspector_panel(self, *, node: dict[str, Any], payload: dict[str, Any] | None = None) -> dict[str, Any]:
        """Render the block-owned topic editor and generic dynamic-port surface."""

        topic = self.topic_name(node.get("config") if isinstance(node.get("config"), dict) else {})
        template = (self.directory / "inspector_panel.html").read_text(encoding="utf-8")
        html = render_inspector_template(
            template=template,
            node={**node, "type": self.kind, "kind": self.kind},
            payload=payload,
            replacements={
                "topic": escape(topic, quote=True),
                "description": escape(
                    "Publie l'entrée sur ce topic et réplique chaque événement reçu vers la sortie."
                ),
            },
        )
        return {
            "html": html,
            "context": {
                "node_id": str(node.get("id") or ""),
                "topic": topic,
                "full_panel": True,
            },
        }

    def render_modal(self, *, node: dict[str, Any], payload: dict[str, Any] | None = None) -> dict[str, Any]:
        """Render the block-owned modal with topic, ports, and runtime state."""

        topic = self.topic_name(node.get("config") if isinstance(node.get("config"), dict) else {})
        template = (self.directory / "block_modal.html").read_text(encoding="utf-8")
        html = self._render_generic_modal_template(template=template, node=node, payload=payload or {})
        html = html.replace("{{ topic }}", escape(topic, quote=True))
        return {
            "html": html,
            "context": {
                "node_id": str(node.get("id") or ""),
                "node_kind": self.kind,
                "topic": topic,
            },
        }

    def topic_name(self, config: Mapping[str, Any] | None) -> str:
        """Return the stripped configured topic, keeping an explicit empty value invalid.

        Args:
            config: Raw node configuration.
        """

        config = config if isinstance(config, Mapping) else {}
        raw_topic = config["topic"] if "topic" in config else DEFAULT_TOPIC
        return str(raw_topic if raw_topic is not None else "").strip()

    def prepare_runtime(self, context: BlockRuntimePreparationContext) -> BlockRuntimePreparation:
        """Validate the topic and declare an active publish/subscribe worker binding.

        Args:
            context: Preparation context carrying config and selected runtime mode.
        """

        self._validate_port_contract(context)
        binding = RuntimeTopicBinding(
            topic=self.topic_name(context.config),
            publish=True,
            subscribe=True,
            receive_own=True,
        )
        if context.runtime_mode != "zeromq_active":
            return BlockRuntimePreparation()
        return BlockRuntimePreparation(topic_bindings=(binding,), keep_alive=True)

    def execute_runtime(self, context: BlockRuntimeContext) -> BlockRuntimeResult:
        """Publish graph events or relay one independently delivered topic event.

        Args:
            context: Generic context for centralized simulation or Active Runtime.
        """

        try:
            self._validate_port_contract(context)
            binding = RuntimeTopicBinding(
                topic=self.topic_name(context.config),
                publish=True,
                subscribe=True,
                receive_own=True,
            )
        except ValueError as exc:
            return self._failure(context, str(exc))

        if context.topic_events:
            return self._relay_topic_event(context, binding.topic, context.topic_events[-1])
        if not context.input_events:
            return BlockRuntimeResult(
                status="skipped",
                outputs=[],
                logs=[f"[topic-broadcast] {context.node_id}: aucun événement à traiter."],
                last_message="",
                content_type=TEXT_PLAIN,
                worker_received="-",
                metadata={
                    "topic_broadcast": {
                        "topic": binding.topic,
                        "direction": "idle",
                        "event_count": 0,
                    }
                },
            )
        if context.runtime_mode == "zeromq_active":
            return self._publish_input_events(context, binding.topic)
        return self._relay_centralized_input(context, binding.topic)

    def _publish_input_events(self, context: BlockRuntimeContext, topic: str) -> BlockRuntimeResult:
        """Publish all fresh graph input events through the injected topic client."""

        client = context.services.get("runtime_topics")
        if not isinstance(client, RuntimeTopicClient):
            return self._failure(context, "Runtime Topics publication client unavailable.")

        message_ids: list[str] = []
        try:
            for event in context.input_events:
                message_ids.append(
                    client.publish(
                        topic,
                        event.value,
                        content_type=str(event.content_type or TEXT_PLAIN),
                    )
                )
        except RuntimeTopicPublishError as exc:
            return self._failure(context, str(exc))

        last_event = context.input_events[-1]
        summary = f"{len(message_ids)} événement(s) publié(s) sur {topic}"
        return BlockRuntimeResult(
            status="success",
            outputs=[],
            logs=[f"[topic-broadcast] {context.node_id}: {summary}."],
            last_message=str(last_event.value if last_event.value is not None else ""),
            content_type=str(last_event.content_type or TEXT_PLAIN),
            worker_received=summary,
            metadata={
                "topic_broadcast": {
                    "topic": topic,
                    "direction": "published",
                    "event_count": len(message_ids),
                    "message_ids": message_ids,
                }
            },
        )

    def _relay_topic_event(
        self,
        context: BlockRuntimeContext,
        topic: str,
        event: RuntimeTopicEvent,
    ) -> BlockRuntimeResult:
        """Serialize one topic payload and emit it through the single graph output."""

        value = self._serialize_payload(event.payload)
        content_type = str(event.content_type or TEXT_PLAIN)
        outputs = self._broadcast_outputs(context, value=value, content_type=content_type)
        summary = f"événement {event.message_id} reçu de {event.source_node_id} sur {topic}"
        return BlockRuntimeResult(
            status="success",
            outputs=outputs,
            logs=[f"[topic-broadcast] {context.node_id}: {summary}."],
            last_message=value,
            content_type=content_type,
            worker_received=summary,
            metadata={
                "topic_broadcast": {
                    "topic": topic,
                    "direction": "received",
                    "event_count": 1,
                    "message_id": event.message_id,
                    "source_node_id": event.source_node_id,
                    "output_count": len(outputs),
                }
            },
        )

    def _relay_centralized_input(self, context: BlockRuntimeContext, topic: str) -> BlockRuntimeResult:
        """Provide the documented local input-to-output fallback for centralized runs."""

        event = context.input_events[-1]
        value = str(event.value if event.value is not None else "")
        content_type = str(event.content_type or TEXT_PLAIN)
        outputs = self._broadcast_outputs(context, value=value, content_type=content_type)
        summary = f"relais local vers {len(outputs)} sortie(s) pour le topic {topic}"
        return BlockRuntimeResult(
            status="success",
            outputs=outputs,
            logs=[f"[topic-broadcast] {context.node_id}: {summary}."],
            last_message=value,
            content_type=content_type,
            worker_received=summary,
            metadata={
                "topic_broadcast": {
                    "topic": topic,
                    "direction": "centralized_relay",
                    "event_count": len(context.input_events),
                    "output_count": len(outputs),
                }
            },
        )

    def _broadcast_outputs(
        self,
        context: BlockRuntimeContext,
        *,
        value: str,
        content_type: str,
    ) -> list[BlockRuntimeOutput]:
        """Build the runtime value for the single validated output port."""

        return [
            BlockRuntimeOutput(
                port_id=int(getattr(port, "id", 0) or 0),
                port_name=str(getattr(port, "name", "") or ""),
                value=value,
                content_type=content_type,
            )
            for port in context.output_ports
        ]

    def _serialize_payload(self, payload: object) -> str:
        """Convert structured topic payloads to graph-safe text without altering strings."""

        if isinstance(payload, str):
            return payload
        return json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=str)

    def _validate_port_contract(self, context: BlockRuntimeContext) -> None:
        """Require exactly one optional input and one output before runtime starts."""

        if len(context.input_ports) != 1 or len(context.output_ports) != 1:
            raise ValueError(
                "Topic Broadcast requires exactly one input and one output "
                f"(received {len(context.input_ports)} input(s), {len(context.output_ports)} output(s))."
            )

        required_names: list[str] = []
        for port in context.input_ports:
            requirement = str(getattr(port, "execution_requirement", "") or "").strip()
            legacy_required = bool(getattr(port, "required", False))
            if requirement == "required_for_execution" or (not requirement and legacy_required):
                required_names.append(str(getattr(port, "name", "") or getattr(port, "id", "?")))
        if required_names:
            raise ValueError(
                "Topic Broadcast inputs must be not_required_for_execution: "
                + ", ".join(required_names)
                + "."
            )

    def _failure(self, context: BlockRuntimeContext, message: str) -> BlockRuntimeResult:
        """Return a structured block failure without emitting graph outputs."""

        return BlockRuntimeResult(
            status="failed",
            outputs=[],
            logs=[f"[topic-broadcast-error] {context.node_id}: {message}"],
            error=message,
            exit_code=1,
            last_message=message,
            content_type=TEXT_PLAIN,
            worker_received="-",
        )
