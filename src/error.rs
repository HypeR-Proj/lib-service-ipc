// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

//! Local serialization, transport and peer framing are different failure origins.

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MessageKind {
    Request,
    Response,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EncodingFailure {
    Serialization,
    /// Bytes observed at the first overflowing write, not the complete encoded
    /// length: encoding stops without constructing the remainder.
    TooLarge {
        bytes: usize,
        limit: usize,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RpcError {
    Transport(hyper_os::Error),
    Encoding {
        message: MessageKind,
        reason: EncodingFailure,
    },
    /// The peer's response frame or serialized response violated the protocol.
    InvalidResponse,
}

impl From<hyper_os::Error> for RpcError {
    fn from(error: hyper_os::Error) -> Self {
        Self::Transport(error)
    }
}

impl RpcError {
    /// Expected abandonment of one private reply, never a shared-listener error.
    pub fn is_reply_abandoned(&self) -> bool {
        matches!(
            self,
            Self::Transport(hyper_os::Error::Status(
                hyper_os::Status::PEER_CLOSED
                    | hyper_os::Status::TIMED_OUT
                    | hyper_os::Status::CANCELLED
            ))
        )
    }
}

impl core::fmt::Display for RpcError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl core::error::Error for RpcError {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        match self {
            Self::Transport(error) => Some(error),
            _ => None,
        }
    }
}

pub type RpcResult<T> = core::result::Result<T, RpcError>;
