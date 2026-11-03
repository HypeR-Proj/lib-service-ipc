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
