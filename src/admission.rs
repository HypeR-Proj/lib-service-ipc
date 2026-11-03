// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

//! Per-request refusal is distinct from failure of the shared receive endpoint.

use hyper_os::{Error, ReceiveRejection, Result, Status};

/// The result of admitting one request, before publishing service-owned state.
#[derive(Debug, Eq, PartialEq)]
pub enum Admission<T> {
    Accepted(T),
    /// Drop this request's owners and continue accepting unrelated requests.
    Rejected(Rejection),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Rejection {
    Transfer(ReceiveRejection),
    InvalidMessage,
    /// Only a request-private handshake ended; the public listener is intact.
    Disconnected(Status),
}

/// A matched transfer has already rolled back. Every other receive failure
/// belongs to the invocation, endpoint or wait and must reach its owner.
pub fn receive_failure<T>(error: Error) -> Result<Admission<T>> {
    match error {
        Error::ReceiveRejected(reason) => Ok(Admission::Rejected(Rejection::Transfer(reason))),
        error => Err(error),
    }
}

/// Failure of a caller-owned second-stage channel is not public listener EOF.
/// Keep unrelated local failures visible rather than retrying them indefinitely.
pub fn private_failure<T>(error: Error) -> Result<Admission<T>> {
    match error {
        Error::Status(status @ (Status::PEER_CLOSED | Status::TIMED_OUT | Status::CANCELLED)) => {
            Ok(Admission::Rejected(Rejection::Disconnected(status)))
        }
        error => receive_failure(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matched_rejections_do_not_hide_identical_local_failures() {
        for status in [
            Status::BAD_HANDLE,
            Status::ACCESS_DENIED,
            Status::FAULT,
            Status::NO_MEMORY,
            Status::RESOURCE_LIMIT,
            Status::INTERNAL,
        ] {
            assert_eq!(
                receive_failure::<()>(Error::Status(status)),
                Err(Error::Status(status))
            );
            assert_eq!(
                receive_failure::<()>(Error::ReceiveRejected(ReceiveRejection::Status(status))),
                Ok(Admission::Rejected(Rejection::Transfer(
                    ReceiveRejection::Status(status)
                )))
            );
        }
        assert_eq!(
            receive_failure::<()>(Error::InvalidResponse),
            Err(Error::InvalidResponse)
        );
        assert_eq!(
            receive_failure::<()>(Error::MessageTooLarge {
                bytes: 1,
                handles: 65
            }),
            Err(Error::MessageTooLarge {
                bytes: 1,
                handles: 65
            })
        );
    }

    #[test]
    fn disconnect_scope_is_explicit() {
        for status in [Status::PEER_CLOSED, Status::TIMED_OUT, Status::CANCELLED] {
            assert_eq!(
                receive_failure::<()>(Error::Status(status)),
                Err(Error::Status(status))
            );
            assert_eq!(
                private_failure::<()>(Error::Status(status)),
                Ok(Admission::Rejected(Rejection::Disconnected(status)))
            );
        }
        assert_eq!(
            private_failure::<()>(Error::Status(Status::BAD_HANDLE)),
            Err(Error::Status(Status::BAD_HANDLE))
        );
    }
}
