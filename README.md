# Topic Broadcast Block

<!-- block-metadata:start -->
[![Block version: unversioned](https://img.shields.io/badge/block-unversioned-lightgrey)](model.json)
[![BloxSmith compatibility: 1.0.9](https://img.shields.io/badge/BloxSmith-1.0.9-brightgreen)](compatibility.json)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)

Verified BloxSmith versions: **1.0.9** (bundled-block tests; see [test evidence](compatibility.json)).
<!-- block-metadata:end -->


## Role

`topic_broadcast` is a single-input/single-output bridge for named, run-scoped Runtime Topics. Use several instances configured with the same topic when data must cross graph branches without a direct edge between the publisher and every subscriber.

In Active Runtime, each graph input event is published once on the configured topic. Every instance subscribed to that topic receives publications from other nodes and copies each received event to its output.

## Ports

- The single input `input_1` accepts `message/*`, has `many` multiplicity, and never blocks execution.
- The single output `output_1` emits `message/*` with `many` multiplicity.
- Inputs and outputs cannot be added or removed.
- Active and centralized preparation reject a graph that does not contain exactly one input and one output.
- Preparation also rejects an imported or externally mutated graph that marks the input as required.

## Configuration

- `topic` (default `workflow.events`) is required.
- A topic must start with an alphanumeric character, contain at most 128 characters, and may then contain letters, numbers, `.`, `_`, `:`, `/`, or `-`.
- The block always declares both publish and subscribe permissions for its configured topic.
- A block receives its own publications. Its output is therefore emitted only after the input has completed a round trip through Runtime Topics.
- A topic reception only emits to the graph output and is never republished, so this two-stage cycle does not create an infinite loop.

## Runtime Behavior

### Active Runtime (`zeromq_active`)

1. Every fresh graph input event is published on `topic` with its original text value and content type; this first activation does not emit a graph output.
2. Every topic publication activates each subscribed `topic_broadcast` instance independently, including the node that originally published it.
3. Each topic reception is copied identically to the output. Structured payloads published by another block are serialized as compact JSON for the graph output.
4. Subscribers stay alive until the active run is stopped.

Changing `topic` changes the worker topology. Stop the active run and load it again after changing this setting.

### One Shot Simulation (`centralized`)

Runtime Topics transport is not active in centralized simulation. The block therefore provides a local fallback: the most recent input event for an activation is copied to the output. Disconnected publisher/subscriber instances do not communicate in this mode; connect the simulated path with a normal graph edge when testing it centrally.

## Example

Create two `topic_broadcast` blocks with `topic = orders.created`:

- connect a source to an input of the first block;
- optionally connect the output of this first block to a local consumer;
- leave the second block disconnected from the first;
- connect the output of the second block to a consumer;
- start an Active Runtime run.

The first block publishes the source event. Both Topic Broadcast instances receive it through `orders.created`, including the publisher itself, and emit the same value on their respective outputs.

## Limits and Delivery Semantics

- Topics are isolated to one run. They are not a cross-run or cross-project message bus.
- Delivery is transient and best effort: there is no persistence, replay, acknowledgement, or exactly-once guarantee.
- Per-publisher order is preserved, but separate publishers have no global ordering guarantee.
- Equal payloads are separate events and are not deduplicated by this block.
- Active Runtime proves every exact subscriber/topic path through the broker before Play can publish
  seeds; it does not rely on a fixed startup delay.
- Message size, outbound publication backlog, inbound activation backlog, and ZeroMQ socket buffers
  use the application-wide `runtime.runtime_topic_*` limits.
- Oversized messages, full queues, readiness failures, and transport failures use structured
  Runtime Topics errors diffused through the graph's backend-error channel.
- A topic without another declared publisher is legal but produces an orphan-subscription warning.
- Empty graph output values are subject to the framework's normal non-publication behavior.
- The block disables the incompatible `Required` choice in its modal and inspector surfaces.
- Use normal ports and edges for visible, deterministic dataflow; use this block only when named runtime broadcast is the intended behavior.

## Compatibility policy

[compatibility.json](compatibility.json) records HackInvent's verified BloxSmith versions and test evidence. Only the versions listed above have been verified, using the block-owned suites in a **bundled-block test installation**. This is not a certification of managed-package installation, every browser/OS, or live provider availability. Other framework versions are unverified, not necessarily incompatible.

The block-version badge follows `model.json`, not a published Git tag. `unversioned` means that no block release version is declared; no number is inferred from the framework version. The framework still uses `model.json` for its runtime/install contract; the tester-owned JSON does not replace it. Official integration tests run in the private `bloxmith-blocs` workspace. Test helpers and the proprietary framework are not bundled in this public block repository.
