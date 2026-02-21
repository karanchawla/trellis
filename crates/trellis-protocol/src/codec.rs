use bytes::{BufMut, BytesMut};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::io;
use std::marker::PhantomData;
use tokio_util::codec::{Decoder, Encoder};

#[derive(Debug, Default)]
pub struct NdJsonCodec<T> {
    marker: PhantomData<T>,
}

impl<T> NdJsonCodec<T> {
    pub fn new() -> Self {
        Self {
            marker: PhantomData,
        }
    }
}

impl<T> Encoder<T> for NdJsonCodec<T>
where
    T: Serialize,
{
    type Error = io::Error;

    fn encode(&mut self, item: T, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let mut encoded = serde_json::to_vec(&item)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
        encoded.push(b'\n');
        dst.reserve(encoded.len());
        dst.put_slice(&encoded);
        Ok(())
    }
}

impl<T> Decoder for NdJsonCodec<T>
where
    T: DeserializeOwned,
{
    type Item = T;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        loop {
            let newline_pos = match src.iter().position(|byte| *byte == b'\n') {
                Some(pos) => pos,
                None => return Ok(None),
            };

            let mut line = src.split_to(newline_pos + 1);
            line.truncate(newline_pos);

            if line.ends_with(b"\r") {
                line.truncate(line.len().saturating_sub(1));
            }

            if line.iter().all(u8::is_ascii_whitespace) {
                continue;
            }

            let parsed = serde_json::from_slice::<T>(&line)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
            return Ok(Some(parsed));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BrokerMessage, RejectionReason, WorkerMessage};
    use std::collections::HashMap;

    #[test]
    fn encodes_and_decodes_single_message() {
        let message = WorkerMessage::Claim {
            worker_id: "w-1".to_string(),
            task_types: vec!["extract".to_string()],
        };

        let mut codec = NdJsonCodec::<WorkerMessage>::new();
        let mut buffer = BytesMut::new();
        codec
            .encode(message.clone(), &mut buffer)
            .expect("encode message");

        let decoded = codec
            .decode(&mut buffer)
            .expect("decode message")
            .expect("message present");
        assert_eq!(decoded, message);
        assert!(buffer.is_empty());
    }

    #[test]
    fn decodes_multiple_messages_from_stream() {
        let mut codec = NdJsonCodec::<BrokerMessage>::new();
        let mut buffer = BytesMut::new();

        codec
            .encode(BrokerMessage::NoWork, &mut buffer)
            .expect("encode no_work");
        codec
            .encode(
                BrokerMessage::Rejected {
                    reason: RejectionReason::LeaseMismatch,
                },
                &mut buffer,
            )
            .expect("encode rejected");

        let first = codec
            .decode(&mut buffer)
            .expect("decode first")
            .expect("first present");
        let second = codec
            .decode(&mut buffer)
            .expect("decode second")
            .expect("second present");

        assert_eq!(first, BrokerMessage::NoWork);
        assert_eq!(
            second,
            BrokerMessage::Rejected {
                reason: RejectionReason::LeaseMismatch,
            }
        );
        assert!(buffer.is_empty());
    }

    #[test]
    fn handles_partial_reads() {
        let mut codec = NdJsonCodec::<WorkerMessage>::new();
        let mut full = BytesMut::new();
        codec
            .encode(
                WorkerMessage::Heartbeat {
                    task_id: "task-1".to_string(),
                    lease_id: "lease-1".to_string(),
                },
                &mut full,
            )
            .expect("encode heartbeat");

        let split_at = full.len() / 2;
        let remainder = full.split_off(split_at);

        let partial = codec.decode(&mut full).expect("decode partial");
        assert!(partial.is_none());

        full.extend_from_slice(&remainder);
        let decoded = codec
            .decode(&mut full)
            .expect("decode completed")
            .expect("message available");

        assert_eq!(
            decoded,
            WorkerMessage::Heartbeat {
                task_id: "task-1".to_string(),
                lease_id: "lease-1".to_string(),
            }
        );
    }

    #[test]
    fn skips_empty_lines() {
        let mut codec = NdJsonCodec::<BrokerMessage>::new();
        let mut buffer = BytesMut::from("\n\r\n".as_bytes());

        let mut message_line = BytesMut::new();
        codec
            .encode(
                BrokerMessage::Rejected {
                    reason: RejectionReason::BrokerReplaced,
                },
                &mut message_line,
            )
            .expect("encode message");
        buffer.extend_from_slice(&message_line);

        let decoded = codec
            .decode(&mut buffer)
            .expect("decode message")
            .expect("message present");

        assert_eq!(
            decoded,
            BrokerMessage::Rejected {
                reason: RejectionReason::BrokerReplaced,
            }
        );
    }

    #[test]
    fn malformed_json_returns_error() {
        let mut codec = NdJsonCodec::<WorkerMessage>::new();
        let mut buffer = BytesMut::from("{bad-json}\n".as_bytes());
        let error = codec.decode(&mut buffer).expect_err("decode should fail");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn unknown_type_returns_error() {
        let mut codec = NdJsonCodec::<WorkerMessage>::new();
        let mut buffer = BytesMut::from("{\"type\":\"unknown\",\"task_id\":\"a\"}\n".as_bytes());

        let error = codec.decode(&mut buffer).expect_err("decode should fail");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn assigned_input_refs_can_be_empty() {
        let mut codec = NdJsonCodec::<BrokerMessage>::new();
        let mut buffer = BytesMut::new();
        codec
            .encode(
                BrokerMessage::Assigned {
                    task_id: "extract".to_string(),
                    task_type: "extract".to_string(),
                    lease_id: "lease-1".to_string(),
                    timeout_seconds: 30,
                    input_refs: HashMap::new(),
                },
                &mut buffer,
            )
            .expect("encode assigned");

        let decoded = codec
            .decode(&mut buffer)
            .expect("decode assigned")
            .expect("assigned present");

        match decoded {
            BrokerMessage::Assigned { input_refs, .. } => assert!(input_refs.is_empty()),
            other => panic!("unexpected message: {other:?}"),
        }
    }

    #[test]
    fn encoded_payload_contains_wire_discriminator() {
        let mut codec = NdJsonCodec::<WorkerMessage>::new();
        let mut buffer = BytesMut::new();
        codec
            .encode(
                WorkerMessage::Claim {
                    worker_id: "worker-1".to_string(),
                    task_types: vec!["extract".to_string()],
                },
                &mut buffer,
            )
            .expect("encode claim");

        let raw = String::from_utf8(buffer.to_vec()).expect("utf8");
        assert!(raw.contains("\"type\":\"claim\""));
        assert!(raw.contains("\"worker_id\":\"worker-1\""));
    }

    #[test]
    fn rejected_reason_round_trip_preserves_variant() {
        let mut codec = NdJsonCodec::<BrokerMessage>::new();
        let mut buffer = BytesMut::new();
        codec
            .encode(
                BrokerMessage::Rejected {
                    reason: RejectionReason::BrokerReplaced,
                },
                &mut buffer,
            )
            .expect("encode rejected");

        let decoded = codec
            .decode(&mut buffer)
            .expect("decode rejected")
            .expect("message present");
        assert_eq!(
            decoded,
            BrokerMessage::Rejected {
                reason: RejectionReason::BrokerReplaced
            }
        );
    }
}
