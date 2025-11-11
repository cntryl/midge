# 🧭 Architectural Principles

## 1. **Composable by Design**

Midge is built as a collection of small, explicit components connected through well-defined traits.
Each layer (e.g., WAL, memtable, compactor, transaction manager) exposes its capabilities through interfaces like `KvStore` and `KvTransaction`, never through hidden globals or cross-module state.

* Composition is explicit — no magic singletons or implicit coordination.
* Each component can be tested, replaced, or extended in isolation.
* Higher layers orchestrate behavior by wiring traits, not subclassing.

> **Goal:** a system where composition, not inheritance, drives capability.

---

## 2. **Deterministic Behavior**

All observable outcomes must derive from input data and configuration — not from timing, concurrency races, or environment quirks.

* No logic should depend on "when" an operation runs, only on *what* was requested.
* Internal state transitions are deterministic, driven by ordered sequences (e.g., WAL entries).
* Every operation can be reproduced by replaying its inputs, making debugging, testing, and recovery predictable.

> **Goal:** the same sequence of inputs always produces the same state transition.

---

## 3. **No Hidden Timers or Side Effects**

The system should never rely on implicit background loops, thread sleeps, or timeouts for correctness.

* Compactions, snapshots, or expirations are *scheduled actions*, not background heuristics.
* Nothing happens "eventually" — every change originates from a clear command or deterministic policy.
* Randomness, wall-clock time, or async delays are isolated to boundary components (e.g., metrics collection, administrative maintenance).

> **Goal:** time is an input, not a dependency.

---

## 4. **Reproducibility and Purity**

Wherever possible, subsystems are *pure state machines*:

* Given a previous state and an event, produce a new state and optional side effects.
* Side effects (I/O, persistence, messaging) are explicit and testable boundaries.
* Unit tests can simulate complex flows without threads or timers.

> **Goal:** predictable, replayable, simulation-friendly behavior.

---

## 5. **Transparent Causality**

Every observable change should have a traceable origin:

* State transitions are logged in causal order via the WAL or event stream.
* No implicit propagation or "self-healing" behavior that hides root causes.
* Observability (logs, metrics, traces) reflects deterministic flows, not scheduler noise.

> **Goal:** clarity and explainability from cause to effect.

---

**In short:**

> Midge favors *composition over coupling, determinism over timing, and explicit actions over side effects.*
