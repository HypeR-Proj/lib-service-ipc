// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

//! Bounded request/reply IPC. Every request supplies a private reply endpoint;
//! large replies are framed on caller-created bounded byte channels. A client
//! cannot retain reply snapshots charged to the server. Replies have a deadline;
//! servers keep these workers separate from their lifecycle/ingestion reactor.

pub mod admission;
mod encoding;
mod error;
pub use admission::{Admission, Rejection};
pub use encoding::PreparedResponse;
use encoding::encode;
pub use error::{EncodingFailure, MessageKind, RpcError, RpcResult};

use core::mem::MaybeUninit;
use hyper_os::capability_channel::{
    CapabilityChannel, CapabilityDisposition, CapabilityReceiveSlot,
};
use hyper_os::handle::{
    ByteChannelObject, CapabilityChannelObject, HandleRef, OwnedHandle, Rights, RightsOffer,
};
use hyper_os::wait::{ObjectSignals, WaitItem, wait_many};
use hyper_os::{Error, Result};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

pub const MAX_REQUEST: usize = 2048;
pub const MAX_RESPONSE: usize = 256 * 1024;
pub const CLIENT_RIGHTS: Rights = Rights::WRITE
    .union(Rights::WAIT)
    .union(Rights::TRANSFER)
    .union(Rights::DUPLICATE);
pub const SERVER_RIGHTS: Rights = Rights::READ.union(Rights::WAIT);
const REPLY_RIGHTS: Rights = Rights::WRITE.union(Rights::WAIT);
const REPLY_MAGIC: &[u8] = b"HYPER-REPLY/1";

pub fn deadline() -> Result<u64> {
    Ok(hyper_os::time::deadline_after(std::time::Duration::from_secs(5))?.as_raw())
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RequestEnvelope<T> {
    deadline_ns: u64,
    request: T,
}

pub fn request<T: Serialize, R: DeserializeOwned>(
    connector: HandleRef<'_, CapabilityChannelObject>,
    request: &T,
) -> RpcResult<R> {
    request_until(connector, request, deadline()?)
}

/// One caller budget covers encoding, admission, reply header and every chunk.
pub fn request_until<T: Serialize, R: DeserializeOwned>(
    connector: HandleRef<'_, CapabilityChannelObject>,
    request: &T,
    until: u64,
) -> RpcResult<R> {
    check_deadline(until)?;
    let bytes = encode(
        &RequestEnvelope {
            deadline_ns: until,
            request,
        },
        MessageKind::Request,
        MAX_REQUEST,
    )?;
    let channel = CapabilityChannel::from_handle(connector.duplicate(CLIENT_RIGHTS)?);
    let (receiver, sender) = hyper_os::channel::create_pair()?;
    let mut sender = Some(sender);
    send(
        &channel,
        until,
        &bytes,
        &mut [CapabilityDisposition::move_handle(
            &mut sender,
            RightsOffer::Exact(REPLY_RIGHTS),
        )?],
    )?;
    let mut header = [0; REPLY_MAGIC.len() + 8];
    if receiver
        .as_byte_channel()
        .receive_until(&mut header, until)
        .map_err(reply_error)?
        != header.len()
        || &header[..REPLY_MAGIC.len()] != REPLY_MAGIC
    {
        return Err(RpcError::InvalidResponse);
    }
    let mut length = [0; 8];
    length.copy_from_slice(&header[REPLY_MAGIC.len()..]);
    let length =
        usize::try_from(u64::from_le_bytes(length)).map_err(|_| RpcError::InvalidResponse)?;
    if length > MAX_RESPONSE {
        return Err(RpcError::InvalidResponse);
    }
    let mut bytes = vec![0; length];
    let mut offset = 0;
    while offset < length {
        let n = receiver
            .as_byte_channel()
            .receive_until(&mut bytes[offset..], until)
            .map_err(reply_error)?;
        if n == 0 {
            return Err(RpcError::InvalidResponse);
        }
        offset += n;
    }
    serde_json::from_slice(&bytes).map_err(|_| RpcError::InvalidResponse)
}

fn reply_error(error: Error) -> RpcError {
    match error {
        Error::MessageTooLarge { .. } => RpcError::InvalidResponse,
        error => RpcError::Transport(error),
    }
}

/// One accepted request, detached from the listener before running policy.
pub struct Call<T> {
    pub request: T,
    reply: OwnedHandle<ByteChannelObject>,
    until: u64,
}

pub fn receive<T: DeserializeOwned>(
    listener: &CapabilityChannel,
    until: u64,
) -> Result<Admission<Call<T>>> {
    let mut bytes = [MaybeUninit::uninit(); MAX_REQUEST];
    let mut slots = [CapabilityReceiveSlot::new::<ByteChannelObject>(
        REPLY_RIGHTS,
    )];
    let message = match listener.receive(until, &mut bytes, &mut slots) {
        Ok(message) => message,
        Err(error) => return admission::receive_failure(error),
    };
    if message.capability_count() != 1 {
        return Ok(Admission::Rejected(Rejection::InvalidMessage));
    }
    let Ok(envelope) = serde_json::from_slice::<RequestEnvelope<T>>(message.bytes()) else {
        return Ok(Admission::Rejected(Rejection::InvalidMessage));
    };
    if hyper_os::time::monotonic_now()?.as_nanoseconds() >= envelope.deadline_ns {
        return Ok(Admission::Rejected(Rejection::Expired));
    }
    let reply = slots[0].take()?.ok_or(Error::MissingHandle)?;
    Ok(Admission::Accepted(Call {
        request: envelope.request,
        reply,
        until: envelope.deadline_ns,
    }))
}

impl<T> Call<T> {
    /// The caller's original absolute budget, available to the service owner.
    pub fn deadline(&self) -> u64 {
        self.until
    }

    /// Check cancellation at the state owner's acceptance boundary.
    ///
    /// A successful check is the cutoff: later peer closure or expiry cannot
    /// roll back accepted side effects. Keep their owners and expose state for
    /// inspection when the reply is lost. Local wait/clock errors remain errors.
    pub fn is_pending(&self) -> Result<bool> {
        if hyper_os::time::monotonic_now()?.as_nanoseconds() >= self.until {
            return Ok(false);
        }
        match wait_many(
            &[WaitItem::new(
                self.reply.as_handle_ref(),
                ObjectSignals::<ByteChannelObject>::PEER_CLOSED,
            )],
            0,
        ) {
            Ok(_) => Ok(false),
            Err(Error::Status(hyper_os::Status::TIMED_OUT)) => {
                Ok(hyper_os::time::monotonic_now()?.as_nanoseconds() < self.until)
            }
            Err(error) => Err(error),
        }
    }

    pub fn respond<R: Serialize>(self, response: &R) -> RpcResult<()> {
        let until = self.until;
        self.respond_until(response, until)
    }

    /// A server may tighten the reply budget, but cannot extend its caller's.
    pub fn respond_until<R: Serialize>(self, response: &R, until: u64) -> RpcResult<()> {
        let until = until.min(self.until);
        check_deadline(until)?;
        let response = PreparedResponse::encode(response)?;
        self.respond_prepared_until(response, until)
    }

    /// Consume the reply owner before publishing any frame. A failed or partial
    /// send cannot be retried with a second response on the same stream.
    pub fn respond_prepared_until(self, response: PreparedResponse, until: u64) -> RpcResult<()> {
        let until = until.min(self.until);
        check_deadline(until)?;
        let bytes = response.bytes();
        let mut header = [0; REPLY_MAGIC.len() + 8];
        header[..REPLY_MAGIC.len()].copy_from_slice(REPLY_MAGIC);
        header[REPLY_MAGIC.len()..].copy_from_slice(&(bytes.len() as u64).to_le_bytes());
        self.reply.as_byte_channel().send_until(&header, until)?;
        for chunk in bytes.chunks(hyper_os::channel::MAX_MESSAGE_BYTES) {
            self.reply.as_byte_channel().send_until(chunk, until)?;
        }
        Ok(())
    }
}

pub fn send(
    channel: &CapabilityChannel,
    until: u64,
    bytes: &[u8],
    offers: &mut [CapabilityDisposition<'_>],
) -> Result<()> {
    loop {
        check_deadline(until)?;
        match channel.try_send(bytes, offers) {
            Err(Error::Status(hyper_os::Status::WOULD_BLOCK)) => {
                wait_many(
                    &[WaitItem::new(
                        channel.as_handle_ref(),
                        ObjectSignals::<CapabilityChannelObject>::PEER_RECEIVING
                            .union(ObjectSignals::<CapabilityChannelObject>::PEER_CLOSED),
                    )],
                    until,
                )?;
            }
            result => return result,
        }
    }
}

fn check_deadline(until: u64) -> Result<()> {
    if hyper_os::time::monotonic_now()?.as_nanoseconds() >= until {
        return Err(Error::Status(hyper_os::Status::TIMED_OUT));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Unencodable;
    impl Serialize for Unencodable {
        fn serialize<S: serde::Serializer>(&self, _: S) -> core::result::Result<S::Ok, S::Error> {
            Err(serde::ser::Error::custom(
                "intentional local serialization failure",
            ))
        }
    }

    #[test]
    fn only_private_reply_transport_abandonment_is_expected() {
        use hyper_os::Status;
        for status in [Status::PEER_CLOSED, Status::TIMED_OUT, Status::CANCELLED] {
            assert!(RpcError::from(Error::Status(status)).is_reply_abandoned());
        }
        for error in [
            RpcError::from(Error::Status(Status::NO_MEMORY)),
            RpcError::from(Error::Status(Status::ACCESS_DENIED)),
            RpcError::from(Error::InvalidResponse),
            RpcError::InvalidResponse,
            RpcError::Encoding {
                message: MessageKind::Response,
                reason: EncodingFailure::Serialization,
            },
        ] {
            assert!(!error.is_reply_abandoned());
        }
    }

    #[test]
    fn local_encoding_failures_preserve_direction_and_reason() {
        for kind in [MessageKind::Request, MessageKind::Response] {
            assert_eq!(
                encode(&Unencodable, kind, 16),
                Err(RpcError::Encoding {
                    message: kind,
                    reason: EncodingFailure::Serialization,
                })
            );
            // JSON escaping, not just source string length, consumes the limit.
            assert_eq!(
                encode(&"\n".repeat(8), kind, 16),
                Err(RpcError::Encoding {
                    message: kind,
                    reason: EncodingFailure::TooLarge {
                        bytes: 17,
                        limit: 16
                    },
                })
            );
        }
    }

    #[test]
    fn oversized_peer_frames_differ_from_trusted_transport_failures() {
        assert_eq!(
            reply_error(Error::MessageTooLarge {
                bytes: 4096,
                handles: 0
            }),
            RpcError::InvalidResponse
        );
        assert_eq!(
            reply_error(Error::InvalidResponse),
            RpcError::Transport(Error::InvalidResponse)
        );
        assert_eq!(
            reply_error(Error::Status(hyper_os::Status::TIMED_OUT)),
            RpcError::Transport(Error::Status(hyper_os::Status::TIMED_OUT))
        );
    }
}
