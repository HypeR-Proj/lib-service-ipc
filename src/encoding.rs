// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

//! Bound wire output while serializing, before any reply frame is published.

use crate::{EncodingFailure, MAX_RESPONSE, MessageKind, RpcError, RpcResult};
use serde::Serialize;
use std::io::{self, Write};

/// Prepared bytes contain no reply authority. Encoding failure leaves the
/// service's `Call` untouched so it can choose a smaller application response.
pub struct PreparedResponse(Vec<u8>);

impl PreparedResponse {
    pub fn encode<T: Serialize>(response: &T) -> RpcResult<Self> {
        encode(response, MessageKind::Response, MAX_RESPONSE).map(Self)
    }

    pub(crate) fn bytes(&self) -> &[u8] {
        &self.0
    }
}

struct Output {
    bytes: Vec<u8>,
    limit: usize,
    message: MessageKind,
    failure: Option<RpcError>,
}

impl Write for Output {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.failure.is_some() {
            return Err(io::Error::other("encoding output already rejected"));
        }
        let needed = self.bytes.len().saturating_add(bytes.len());
        if needed > self.limit {
            self.failure = Some(RpcError::Encoding {
                message: self.message,
                reason: EncodingFailure::TooLarge {
                    bytes: needed,
                    limit: self.limit,
                },
            });
            return Err(io::Error::other("encoded message exceeds wire limit"));
        }
        if needed > self.bytes.capacity() {
            let capacity = needed
                .checked_next_power_of_two()
                .unwrap_or(self.limit)
                .min(self.limit);
            if self
                .bytes
                .try_reserve_exact(capacity - self.bytes.len())
                .is_err()
            {
                self.failure = Some(hyper_os::Error::Status(hyper_os::Status::NO_MEMORY).into());
                return Err(io::Error::other("encoding allocation failed"));
            }
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub(crate) fn encode<T: Serialize>(
    value: &T,
    message: MessageKind,
    limit: usize,
) -> RpcResult<Vec<u8>> {
    let mut output = Output {
        bytes: Vec::new(),
        limit,
        message,
        failure: None,
    };
    let result = serde_json::to_writer(&mut output, value);
    // A custom serializer cannot erase a writer failure by catching its error.
    if let Some(error) = output.failure {
        return Err(error);
    }
    result.map_err(|_| RpcError::Encoding {
        message,
        reason: EncodingFailure::Serialization,
    })?;
    Ok(output.bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::ser::SerializeSeq;
    use std::cell::Cell;

    struct LargeSequence<'a>(&'a Cell<usize>);
    impl Serialize for LargeSequence<'_> {
        fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            let mut sequence = serializer.serialize_seq(None)?;
            for _ in 0..1_000_000 {
                self.0.set(self.0.get() + 1);
                sequence.serialize_element(&"escaped\0value")?;
            }
            sequence.end()
        }
    }

    #[test]
    fn output_is_rejected_without_traversing_the_entire_response() {
        let visited = Cell::new(0);
        let error = encode(&LargeSequence(&visited), MessageKind::Response, 128).err();
        assert!(matches!(
            error,
            Some(RpcError::Encoding {
                reason: EncodingFailure::TooLarge { limit: 128, .. },
                ..
            })
        ));
        assert!(visited.get() < 10);
    }

    #[test]
    fn exact_wire_limits_and_escape_expansion() -> RpcResult<()> {
        for limit in [crate::MAX_REQUEST, MAX_RESPONSE] {
            let value = "a".repeat(limit - 2);
            assert_eq!(encode(&value, MessageKind::Response, limit)?.len(), limit);
            assert!(matches!(
                encode(&(value + "a"), MessageKind::Response, limit),
                Err(RpcError::Encoding {
                    reason: EncodingFailure::TooLarge { .. },
                    ..
                })
            ));
        }
        assert_eq!(encode(&"\0🙂", MessageKind::Response, 12)?.len(), 12);
        assert!(encode(&"\0🙂", MessageKind::Response, 11).is_err());
        Ok(())
    }

    #[test]
    fn writer_failure_cannot_be_cleared_by_later_writes() {
        let mut output = Output {
            bytes: Vec::new(),
            limit: 2,
            message: MessageKind::Response,
            failure: None,
        };
        assert!(output.write_all(b"too long").is_err());
        assert!(output.write_all(b"ok").is_err());
        assert!(output.bytes.is_empty());
        assert!(output.failure.is_some());
    }
}
