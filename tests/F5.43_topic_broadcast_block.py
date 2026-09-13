#!/usr/bin/env python3
# -----------------------------------------------------------------------------
# Role: Verifies Topic Broadcast direct, UI, centralized, and active behavior.
# File Name: F5.43_topic_broadcast_block.py
# Author: Alexandre EL
# Email: alex@hackinvent.com
# Created Date: 2026-08-25
# -----------------------------------------------------------------------------

"""F5.43 - Topic Broadcast autonomous block coverage."""

# Test cases:
# - FB1/FB5 - Validate the active-only run-scoped binding, keep-alive policy, self-reception, topic validation, fixed ports, and block-owned UI.
# - FB2 - Publish every fresh graph input event with its original value and content type.
# - FB3 - Relay one topic event to the declared output and verify disconnected active publisher/subscriber instances end to end.
# - FB4 - Relay a centralized input event locally to the declared output.

from pathlib import Path
from types import SimpleNamespace
import sys


ROOT_DIR = Path(__file__).resolve().parents[3]
TESTS_DIR = ROOT_DIR / "tests"
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
if str(TESTS_DIR) not in sys.path:
    sys.path.insert(0, str(TESTS_DIR))

from ui_smoke_common import (  # noqa: E402
    create_run_api,
    data_edge,
    display_node,
    expect,
    graph_payload,
    isolated_server,
    play_run_api,
    prepare_run_api,
    stop_run_api,
    text_node,
    wait_for_run_predicate,
    wait_for_run_terminal,
)

from blocs.topic_broadcast.block import TopicBroadcastBlock  # noqa: E402
from bloxsmith_app.block_runtime import BlockInputEvent, BlockRuntimeContext  # noqa: E402
from bloxsmith_app.block_ui import render_block_inspector_panel, render_block_modal, render_block_node_card  # noqa: E402
from bloxsmith_app.port_requirements import NOT_REQUIRED_FOR_EXECUTION  # noqa: E402
from bloxsmith_app.runtime_topics.contracts import RuntimeTopicEvent  # noqa: E402


class FakeRuntimeTopicClient:
    """Capture publications made by direct block executions."""

    def __init__(self) -> None:
        self.publications: list[dict[str, object]] = []

    def publish(
        self,
        topic: str,
        payload: object,
        *,
        content_type: str,
        correlation_id: str | None = None,
    ) -> str:
        """Record one publication and return a stable test message id."""

        message_id = f"message-{len(self.publications) + 1}"
        self.publications.append(
            {
                "message_id": message_id,
                "topic": topic,
                "payload": payload,
                "content_type": content_type,
                "correlation_id": correlation_id,
            }
        )
        return message_id


def topic_broadcast_node(node_id: str, *, topic: str = "orders.created", x: int = 360, y: int = 120) -> dict:
    """Return one graph node using the fixed single-input/single-output contract."""

    return {
        "id": node_id,
        "kind": "topic_broadcast",
        "title": "Topic Broadcast",
        "position": {"x": x, "y": y},
        "inputs": [
            {
                "id": 1,
                "name": "input_1",
                "title": "In 1",
                "accepts": ["message/*"],
                "multiplicity": "many",
                "required": False,
                "execution_requirement": NOT_REQUIRED_FOR_EXECUTION,
            },
        ],
        "outputs": [
            {"id": 1, "name": "output_1", "title": "Out 1", "emits": ["message/*"], "multiplicity": "many"},
        ],
        "config": {"topic": topic},
    }


def input_event(edge_id: str, port_id: int, value: str, content_type: str = "text/plain") -> BlockInputEvent:
    """Build one fresh graph input event for direct execution."""

    return BlockInputEvent(
        edge_id=edge_id,
        input_port_id=port_id,
        input_port_name=f"input_{port_id}",
        source_node_id=f"source-{port_id}",
        source_port_id=1,
        value=value,
        content_type=content_type,
        sequence=port_id,
    )


def direct_context(
    *,
    runtime_mode: str,
    input_events: tuple[BlockInputEvent, ...] = (),
    topic_events: tuple[RuntimeTopicEvent, ...] = (),
    services: dict[str, object] | None = None,
    topic: str = "orders.created",
) -> BlockRuntimeContext:
    """Build a direct context with one input and one output."""

    return BlockRuntimeContext(
        run_id="run-unit",
        node_id="topic-unit",
        kind="topic_broadcast",
        title="Topic Unit",
        config={"topic": topic},
        input_ports=(
            SimpleNamespace(id=1, name="input_1", execution_requirement=NOT_REQUIRED_FOR_EXECUTION),
        ),
        output_ports=(SimpleNamespace(id=1, name="output_1"),),
        input_events=input_events,
        topic_events=topic_events,
        runtime_mode=runtime_mode,
        services=services,
        root_dir=ROOT_DIR,
    )


def test_preparation_model_and_ui() -> None:
    """Validate binding policy, topic validation, dynamic ports, and owned UI."""

    block = TopicBroadcastBlock()
    active = block.prepare_runtime(direct_context(runtime_mode="zeromq_active"))
    expect(len(active.topic_bindings) == 1, "Le bloc actif doit déclarer exactement un topic.")
    binding = active.topic_bindings[0]
    expect(binding.topic == "orders.created", "Le topic configuré doit être déclaré tel quel.")
    expect(binding.publish and binding.subscribe, "Le bloc doit publier et s'abonner au topic.")
    expect(binding.receive_own, "Le bloc doit recevoir ses propres publications avant d'émettre sur sa sortie.")
    expect(active.keep_alive, "Le subscriber doit rester actif jusqu'au Stop.")

    centralized = block.prepare_runtime(direct_context(runtime_mode="centralized"))
    expect(centralized.topic_bindings == (), "La simulation ne doit pas déclarer le transport topic.")
    expect(not centralized.keep_alive, "La simulation centralisée ne doit pas rester vivante.")

    try:
        block.prepare_runtime(direct_context(runtime_mode="centralized", topic="topic invalide"))
    except ValueError:
        pass
    else:
        raise AssertionError("Un nom de topic invalide doit être refusé dans les deux modes.")

    required_context = direct_context(runtime_mode="centralized")
    required_context.input_ports = (
        SimpleNamespace(
            id=1,
            name="blocking_input",
            execution_requirement="required_for_execution",
            required=True,
        ),
    )
    try:
        block.prepare_runtime(required_context)
    except ValueError as exc:
        expect("blocking_input" in str(exc), "L'erreur doit identifier l'entrée requise incompatible.")
    else:
        raise AssertionError("Une entrée requise doit être refusée avant l'exécution du bloc.")

    capabilities = block.model.get("port_capabilities", {})
    expect(capabilities.get("can_add_inputs") is False, "Le bloc ne doit pas accepter d'entrée supplémentaire.")
    expect(capabilities.get("can_add_outputs") is False, "Le bloc ne doit pas accepter de sortie supplémentaire.")
    expect(capabilities.get("allow_required_input") is False, "Les entrées requises doivent rester interdites.")
    expect(capabilities.get("default_required") is False, "Les nouvelles entrées doivent être optionnelles.")
    expect(len(block.model.get("ports", {}).get("inputs", [])) == 1, "Le modèle doit déclarer une seule entrée.")
    expect(len(block.model.get("ports", {}).get("outputs", [])) == 1, "Le modèle doit déclarer une seule sortie.")

    extra_output_context = direct_context(runtime_mode="centralized")
    extra_output_context.output_ports = (
        SimpleNamespace(id=1, name="output_1"),
        SimpleNamespace(id=2, name="output_2"),
    )
    try:
        block.prepare_runtime(extra_output_context)
    except ValueError as exc:
        expect("exactly one input and one output" in str(exc), "L'erreur doit expliquer le contrat de ports fixe.")
    else:
        raise AssertionError("Une sortie supplémentaire doit être refusée avant l'exécution.")

    node = topic_broadcast_node("topic-ui")
    modal_html = str(render_block_modal("topic_broadcast", {"node": node, "runtime": {}}).get("html") or "")
    inspector_html = str(render_block_inspector_panel("topic_broadcast", {"node": node}).get("html") or "")
    card_html = str(render_block_node_card("topic_broadcast", {"node": node}).get("html") or "")
    expect('data-block-config-field="topic"' in modal_html, "Le modal doit éditer le topic.")
    expect('data-block-config-field="topic"' in inspector_html, "L'inspector doit éditer le topic.")
    expect("orders.created" in card_html, "La node-card doit afficher le topic configuré.")


def test_direct_active_publication_and_topic_relay() -> None:
    """Validate every input publication and all-output topic replication."""

    block = TopicBroadcastBlock()
    client = FakeRuntimeTopicClient()
    published = block.execute_runtime(
        direct_context(
            runtime_mode="zeromq_active",
            input_events=(
                input_event("edge-1", 1, "first"),
                input_event("edge-2", 1, '{"count":2}', "application/json"),
            ),
            services={"runtime_topics": client},
        )
    )
    expect(published.status == "success", "La publication directe doit réussir.")
    expect(published.outputs == [], "Une entrée active doit publier sur le topic, pas directement sur le graphe.")
    expect(
        [(item["payload"], item["content_type"]) for item in client.publications]
        == [("first", "text/plain"), ('{"count":2}', "application/json")],
        "Chaque événement d'entrée doit être publié sans transformation.",
    )
    expect(
        published.metadata.get("topic_broadcast", {}).get("event_count") == 2,
        "Le nombre de publications doit être tracé.",
    )

    relayed = block.execute_runtime(
        direct_context(
            runtime_mode="zeromq_active",
            topic_events=(
                RuntimeTopicEvent(
                    message_id="topic-message-1",
                    run_id="run-unit",
                    topic="orders.created",
                    source_node_id="topic-unit",
                    payload={"order_id": 42},
                    content_type="application/json",
                    sequence=1,
                ),
            ),
        )
    )
    expect(relayed.status == "success", "La réception directe doit réussir.")
    expect(len(relayed.outputs) == 1, "Le message reçu doit être émis sur l'unique sortie.")
    expect({output.value for output in relayed.outputs} == {'{"order_id":42}'}, "Le payload JSON doit être sérialisé.")
    expect({output.content_type for output in relayed.outputs} == {"application/json"}, "Le content type doit être préservé.")


def test_centralized_runtime() -> None:
    """Run the local input-to-all-outputs simulation fallback."""

    with isolated_server() as server:
        document = graph_payload(
            "F5 Topic Broadcast centralized",
            [
                text_node("text-1", "Text", "central message", 80, 120),
                topic_broadcast_node("topic-central", x=360, y=120),
                display_node("display-1", "Display", 720, 120),
            ],
            [
                data_edge("edge-text-topic", "text-1", 1, "topic-central", 1),
                data_edge("edge-topic-display-1", "topic-central", 1, "display-1", 1),
            ],
        )
        created = create_run_api(server, document, runtime_mode="centralized")
        run = wait_for_run_terminal(server, str(created.get("run_id") or ""), timeout_sec=20)
        expect(run.get("status") == "success", "Le run Topic Broadcast centralisé doit réussir.")
        expect(
            run.get("output_values", {}).get("topic-central:1", {}).get("value") == "central message",
            "La première sortie centralisée doit recevoir la valeur.",
        )


def test_active_runtime_topic_broadcast() -> None:
    """Run two disconnected instances communicating only through Runtime Topics."""

    with isolated_server() as server:
        document = graph_payload(
            "F5 Topic Broadcast active",
            [
                text_node("text-1", "Text", "active topic message", 60, 80),
                topic_broadcast_node("topic-publisher", x=340, y=80),
                topic_broadcast_node("topic-subscriber", x=340, y=300),
                display_node("display-publisher", "Publisher Display", 740, 80),
                display_node("display-subscriber", "Subscriber Display", 740, 300),
            ],
            [
                data_edge("edge-text-publisher", "text-1", 1, "topic-publisher", 1),
                data_edge("edge-publisher-display", "topic-publisher", 1, "display-publisher", 1),
                data_edge("edge-subscriber-display", "topic-subscriber", 1, "display-subscriber", 1),
            ],
        )
        prepared = prepare_run_api(server, document, runtime_mode="zeromq_active")
        run_id = str(prepared.get("run_id") or "")
        play_run_api(server, run_id)
        state = wait_for_run_predicate(
            server,
            run_id,
            lambda item: (
                item.get("output_values", {}).get("topic-publisher:1", {}).get("value")
                == "active topic message"
                and item.get("output_values", {}).get("topic-subscriber:1", {}).get("value")
                == "active topic message"
                and item.get("results", {}).get("display-publisher", {}).get("display_received_count")
                == 1
                and item.get("results", {}).get("display-subscriber", {}).get("display_received_count")
                == 1
            ),
            "Les deux Topic Broadcast et leurs Displays n'ont pas tous traité la publication Runtime Topics.",
            timeout_sec=20,
        )
        expect(state.get("status") == "running", "Les subscribers keep-alive doivent conserver le run actif.")
        expect(
            state.get("results", {}).get("display-publisher", {}).get("display_received_count") == 1,
            "Le Display local doit recevoir l'auto-publication par la sortie du publisher.",
        )
        expect(
            state.get("results", {}).get("display-subscriber", {}).get("display_received_count") == 1,
            "Le Display distant doit recevoir la publication par la sortie du subscriber.",
        )
        stop_run_api(server, run_id)
        stopped = wait_for_run_terminal(server, run_id, timeout_sec=20)
        expect(stopped.get("status") in {"cancelled", "success"}, "Stop doit terminer le runtime topic actif.")


def main() -> None:
    test_preparation_model_and_ui()
    test_direct_active_publication_and_topic_relay()
    test_centralized_runtime()
    test_active_runtime_topic_broadcast()
    print("[ok] F5.43_topic_broadcast_block")


if __name__ == "__main__":
    main()
