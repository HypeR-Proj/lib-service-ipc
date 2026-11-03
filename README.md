<!--
SPDX-FileCopyrightText: 2026 roolrz
SPDX-License-Identifier: Apache-2.0
-->

# lib-service-ipc

Bounded request/reply IPC for Native services. Requests carry caller-owned reply
channels; the library serializes JSON and frames bounded responses.

## Workspace tests

Use the [HypeR workspace](https://github.com/HypeR-Proj/HypeR) to resolve dependencies:

```sh
python3 project.py test lib-service-ipc
```

Paths in the documentation are relative to the repo-managed workspace root.
Edit this repository and prepare the workspace again before building.

## Receive outcomes

`receive<T>` returns `Admission::Accepted(Call<T>)` or
`Admission::Rejected(reason)` for one completed request. Rejection covers malformed
JSON, unexpected handle counts and a matched capability transfer rolled back by
the kernel. Dropping the rejected request releases any received handles; no call
is published to application policy. A server may continue with another request.

An `Err` is an invocation, endpoint or wait failure, or a trusted ABI violation.
Do not retry every error: report persistent local failure to the listener owner.
`admission::private_failure` additionally treats closure, timeout and cancellation
of a request-private handshake as rejection of that request. It must not be used
to classify failure of the shared listening endpoint.

## Request budgets and errors

`request_until(connector, request, deadline_ns)` uses one absolute monotonic
budget for local encoding, capability admission and all reply frames. `request`
chooses a single five-second budget at entry. The request wire is a JSON envelope
with `deadline_ns` and `request` fields; unknown envelope fields are rejected.
Expired requests close their private reply owner without entering service policy.

An admitted `Call` exposes `deadline()` for the state owner. `respond_until`
can tighten this deadline and cannot extend it; ordinary `respond` also caps
reply I/O at five seconds. Each chunk uses the same deadline. A caller-provided
infinite deadline does not remove the ordinary server reply cap.

`RpcError::Encoding` identifies local serialization or encoded-size failure,
including request/response direction. `RpcError::InvalidResponse` identifies
invalid peer framing or response data. `RpcError::Transport` preserves the
underlying Native error, including trusted SDK contract failures. Low-level
`send` and `receive` retain Native transport errors and typed admission results.
Deadline expiry does not undo a command that the service already applied.

## Cancellation and applied outcomes

Transport admission alone does not accept a state change. A service which queues
a `Call` must move that original call to its state owner and call `is_pending`
immediately before accepting the operation. Expired calls and closed reply peers
return false; local clock/wait failures remain errors. That liveness observation
is the cancellation cutoff. It cannot atomically order a later peer close with
a later state write.

After acceptance, retain applied state and its resource/retirement owners even
when the reply cannot be delivered. A timeout or closed channel is an uncertain
outcome: inspect service state before retrying a non-idempotent operation.
