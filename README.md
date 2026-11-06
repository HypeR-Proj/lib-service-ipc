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
can tighten this deadline and cannot extend it; ordinary `respond` inherits it
without adding another timeout. Each chunk uses the same deadline. Services must
choose an explicit finite reply limit when an infinite caller budget must not
retain their workers. Journal queries and component-management replies use a
five-second service limit, separate from the library default request budget.

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

## Preparing bounded replies

Request and reply encoding stops at the wire limit while writing JSON. The
`TooLarge.bytes` value is the observed overflowing length, not a promise to
measure the complete response. This bounds encoded output, not allocations made
by the service while constructing its response object, or arbitrary computation
inside a custom serializer. Deadline checks do not preempt such computation.

`PreparedResponse::encode(&response)` prepares bounded bytes without consuming a
`Call`. If preparation fails, the service may retain that call and explicitly
choose a smaller application error or close it. Send with
`call.respond_prepared_until(prepared, absolute_deadline)`. The effective deadline
still cannot exceed the caller's. Sending consumes the call before any frame is
published; a failed partial send cannot be restarted on the same reply stream.
The convenience `respond`/`respond_until` methods consume the call even when
preparation fails; use separate preparation when recovery is required.

`RpcError::is_reply_abandoned()` classifies closure, expiry and cancellation of a
private reply. Other failures deserve bounded service diagnostics, including
local encoding failures; they must not be silently treated as a normal vanished
client. This classifier does not apply to errors on a public listener.

## Service-author sequence

1. Prepare state and bounded admission/worker queues before publishing a listener.
2. Match `Admission`; reject one malformed client without retiring the listener.
3. Move the accepted `Call` to the state owner. Check `is_pending` immediately
   before accepting effects; transport admission is an earlier milestone.
4. Retain applied state independently of the reply and release state locks before
   encoding or waiting on a client.
5. Select an absolute service deadline before encoding if a shorter limit is
   needed. Prepare bounded output while retaining the call when an application
   fallback is supported; then consume it in one send attempt.
6. Classify private abandonment separately from local faults. On shutdown, close
   admission and drain/join workers under the service's explicit finite policy.

Startup readiness, protocol publication and acceptance of an application operation
are separate contracts. See [component connections](https://github.com/HypeR-Proj/lib-component#connection-failures-and-publication)
for publication milestones. Reply loss never rolls back accepted side effects.
