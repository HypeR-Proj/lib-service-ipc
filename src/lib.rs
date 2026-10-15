// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

//! Bounded request/reply IPC. Every request supplies a private reply endpoint;
//! large replies are framed on caller-created bounded byte channels. A client
//! cannot retain reply snapshots charged to the server. Replies have a deadline;
//! servers keep these workers separate from their lifecycle/ingestion reactor.

use core::mem::MaybeUninit;
use hyper_os::capability_channel::{
    CapabilityChannel, CapabilityDisposition, CapabilityReceiveSlot,
};
use hyper_os::handle::{
    ByteChannelObject, CapabilityChannelObject, HandleRef, OwnedHandle, Rights, RightsOffer,
};
use hyper_os::wait::{ObjectSignals, WaitItem, wait_many};
use hyper_os::{Error, Result};
use serde::{Serialize, de::DeserializeOwned};

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

pub fn request<T: Serialize, R: DeserializeOwned>(
    connector: HandleRef<'_, CapabilityChannelObject>,
    request: &T,
) -> Result<R> {
    let bytes = serde_json::to_vec(request).map_err(|_| Error::InvalidResponse)?;
    if bytes.len() > MAX_REQUEST {
        return Err(Error::InvalidResponse);
    }
    let channel = CapabilityChannel::from_handle(connector.duplicate(CLIENT_RIGHTS)?);
    let (receiver, sender) = hyper_os::channel::create_pair()?;
    let mut sender = Some(sender);
    send(
        &channel,
        deadline()?,
        &bytes,
        &mut [CapabilityDisposition::move_handle(
            &mut sender,
            RightsOffer::Exact(REPLY_RIGHTS),
        )?],
    )?;
    let until = deadline()?;
    let mut header = [0; REPLY_MAGIC.len() + 8];
    if receive_bytes(&receiver, until, &mut header)? != header.len()
        || &header[..REPLY_MAGIC.len()] != REPLY_MAGIC
    {
        return Err(Error::InvalidResponse);
    }
    let mut length = [0; 8];
    length.copy_from_slice(&header[REPLY_MAGIC.len()..]);
    let length = usize::try_from(u64::from_le_bytes(length)).map_err(|_| Error::InvalidResponse)?;
    if length > MAX_RESPONSE {
        return Err(Error::InvalidResponse);
    }
    let mut bytes = vec![0; length];
    let mut offset = 0;
    while offset < length {
        let n = receive_bytes(&receiver, until, &mut bytes[offset..])?;
        if n == 0 {
            return Err(Error::InvalidResponse);
        }
        offset += n;
    }
    serde_json::from_slice(&bytes).map_err(|_| Error::InvalidResponse)
}

/// One accepted request, detached from the listener before running policy.
pub struct Call<T> {
    pub request: T,
    reply: OwnedHandle<ByteChannelObject>,
}

pub fn receive<T: DeserializeOwned>(listener: &CapabilityChannel, until: u64) -> Result<Call<T>> {
    let mut bytes = [MaybeUninit::uninit(); MAX_REQUEST];
    let mut slots = [CapabilityReceiveSlot::new::<ByteChannelObject>(
        REPLY_RIGHTS,
    )];
    let message = listener.receive(until, &mut bytes, &mut slots)?;
    if message.capability_count() != 1 {
        return Err(Error::InvalidResponse);
    }
    let request = serde_json::from_slice(message.bytes()).map_err(|_| Error::InvalidResponse)?;
    let reply = slots[0].take()?.ok_or(Error::MissingHandle)?;
    Ok(Call { request, reply })
}

impl<T> Call<T> {
    pub fn respond<R: Serialize>(self, response: &R) -> Result<()> {
        let bytes = serde_json::to_vec(response).map_err(|_| Error::InvalidResponse)?;
        if bytes.len() > MAX_RESPONSE {
            return Err(Error::InvalidResponse);
        }
        let until = deadline()?;
        let mut header = [0; REPLY_MAGIC.len() + 8];
        header[..REPLY_MAGIC.len()].copy_from_slice(REPLY_MAGIC);
        header[REPLY_MAGIC.len()..].copy_from_slice(&(bytes.len() as u64).to_le_bytes());
        send_bytes(&self.reply, until, &header)?;
        for chunk in bytes.chunks(hyper_os::channel::MAX_MESSAGE_BYTES) {
            send_bytes(&self.reply, until, chunk)?;
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

fn send_bytes(channel: &OwnedHandle<ByteChannelObject>, until: u64, bytes: &[u8]) -> Result<()> {
    loop {
        check_deadline(until)?;
        match channel.as_byte_channel().try_send(bytes) {
            Err(Error::Status(hyper_os::Status::WOULD_BLOCK)) => {
                wait_many(
                    &[WaitItem::new(
                        channel.as_handle_ref(),
                        ObjectSignals::<ByteChannelObject>::WRITABLE
                            .union(ObjectSignals::<ByteChannelObject>::PEER_CLOSED),
                    )],
                    until,
                )?;
            }
            result => return result,
        }
    }
}

fn receive_bytes(
    channel: &OwnedHandle<ByteChannelObject>,
    until: u64,
    bytes: &mut [u8],
) -> Result<usize> {
    loop {
        check_deadline(until)?;
        match channel.as_byte_channel().try_receive(bytes) {
            Err(Error::Status(hyper_os::Status::WOULD_BLOCK)) => {
                wait_many(
                    &[WaitItem::new(
                        channel.as_handle_ref(),
                        ObjectSignals::<ByteChannelObject>::READABLE
                            .union(ObjectSignals::<ByteChannelObject>::PEER_CLOSED),
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
