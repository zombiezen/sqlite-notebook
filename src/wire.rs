use std::{borrow::Cow, collections::HashMap};

use anyhow::{anyhow, Result};
use chrono::{DateTime, FixedOffset, Utc};
use hmac::{digest, Hmac, Mac};
use rand::Rng;
use serde::{ser::SerializeMap, Deserialize, Serialize};
use sha2::Sha256;
use uuid::Builder as UuidBuilder;

const DELIM: &[u8] = b"<IDS|MSG>";

pub(crate) const PROTOCOL_VERSION: &str = "5.0";

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct Authentication<S, K> {
    pub(crate) signature_scheme: S,
    pub(crate) key: K,
}

#[derive(Clone, Debug)]
pub(crate) struct Message<P> {
    identities: Vec<P>,
    header: P,
    parent_header: P,
    metadata: P,
    content: P,
    buffers: Vec<P>,
}

impl<P> Message<P> {
    pub(crate) fn identities(&self) -> &[P] {
        &self.identities
    }
}

impl Message<Vec<u8>> {
    pub(crate) fn new(
        rng: impl Rng,
        r#type: impl Into<String>,
        session: impl Into<String>,
        content: &(impl Serialize + ?Sized),
    ) -> Result<Self, serde_json::Error> {
        let header = MessageHeader::new(rng, r#type, session);
        let header = serde_json::to_vec(&header)?;
        let content = serde_json::to_vec(content)?;
        Ok(Message {
            identities: Vec::new(),
            header,
            parent_header: b"{}".to_vec(),
            metadata: b"{}".to_vec(),
            content,
            buffers: Vec::new(),
        })
    }

    pub(crate) fn set_identities<I>(&mut self, identities: impl IntoIterator<Item = I>)
    where
        I: Into<Vec<u8>>,
    {
        self.identities.clear();
        self.identities
            .extend(identities.into_iter().map(|id| id.into()));
    }

    pub(crate) fn set_parent_header(&mut self, h: impl Into<Vec<u8>>) {
        self.parent_header = h.into();
    }
}

impl<P: AsRef<[u8]>> Message<P> {
    pub(crate) fn serialize<S, K>(&self, auth: &Authentication<S, K>) -> Result<Vec<Vec<u8>>>
    where
        S: AsRef<str>,
        K: AsRef<[u8]>,
    {
        let sig_hex = hex::encode(self.sign(auth)?);
        let mut result =
            Vec::<Vec<u8>>::with_capacity(self.identities.len() + 6 + self.buffers.len());
        result.extend(self.identities.iter().map(|id| id.as_ref().to_vec()));
        result.push(DELIM.to_vec());
        result.push(sig_hex.into_bytes());
        result.push(self.header.as_ref().to_vec());
        result.push(self.parent_header.as_ref().to_vec());
        result.push(self.metadata.as_ref().to_vec());
        result.push(self.content.as_ref().to_vec());
        result.extend(self.buffers.iter().map(|b| b.as_ref().to_vec()));
        Ok(result)
    }

    pub(crate) fn deserialize<S, K>(
        auth: &Authentication<S, K>,
        parts: impl IntoIterator<Item = P>,
    ) -> Result<Self>
    where
        S: AsRef<str>,
        K: AsRef<[u8]>,
    {
        let mut parts = parts.into_iter();
        let mut identities = Vec::new();
        loop {
            match parts.next() {
                None => return Err(anyhow!("delimiter not found")),
                Some(p) if p.as_ref() == DELIM => break,
                Some(p) => identities.push(p),
            }
        }
        let sig = hex::decode(
            parts
                .next()
                .ok_or_else(|| anyhow!("too few frames"))?
                .as_ref(),
        )?;
        let header = parts.next().ok_or_else(|| anyhow!("too few frames"))?;
        let _: serde::de::IgnoredAny =
            serde_json::from_slice(header.as_ref()).map_err(|err| anyhow!("header: {}", err))?;
        let parent_header = parts.next().ok_or_else(|| anyhow!("too few frames"))?;
        let _: serde::de::IgnoredAny = serde_json::from_slice(parent_header.as_ref())
            .map_err(|err| anyhow!("parent header: {}", err))?;
        let metadata = parts.next().ok_or_else(|| anyhow!("too few frames"))?;
        let _: serde::de::IgnoredAny = serde_json::from_slice(metadata.as_ref())
            .map_err(|err| anyhow!("metadata: {}", err))?;
        let content = parts.next().ok_or_else(|| anyhow!("too few frames"))?;
        let _: serde::de::IgnoredAny =
            serde_json::from_slice(content.as_ref()).map_err(|err| anyhow!("content: {}", err))?;
        let buffers = parts.collect();
        let msg = Self {
            identities,
            header,
            parent_header,
            metadata,
            content,
            buffers,
        };
        msg.verify(auth, &sig)?;
        Ok(msg)
    }

    pub(crate) fn raw_header(&self) -> &[u8] {
        self.header.as_ref()
    }

    pub(crate) fn parse_header(&self) -> Result<MessageHeader, serde_json::Error> {
        serde_json::from_slice(self.header.as_ref())
    }

    pub(crate) fn deserialize_content<'a, T: Deserialize<'a>>(
        &'a self,
    ) -> Result<T, serde_json::Error> {
        serde_json::from_slice(self.content.as_ref())
    }

    fn sign<S, K>(&self, auth: &Authentication<S, K>) -> Result<Vec<u8>>
    where
        S: AsRef<str>,
        K: AsRef<[u8]>,
    {
        if auth.key.as_ref().is_empty() {
            return Ok(Vec::new());
        }

        if auth.signature_scheme.as_ref() != "hmac-sha256" {
            return Err(anyhow!(
                "sign jupyter message: unknown signature scheme {}",
                auth.signature_scheme.as_ref()
            ));
        }

        let mut mac = Hmac::<Sha256>::new_from_slice(auth.key.as_ref())?;
        self.update_mac(&mut mac);
        Ok(mac.finalize().into_bytes().to_vec())
    }

    fn verify<S, K>(&self, auth: &Authentication<S, K>, sig: &[u8]) -> Result<()>
    where
        S: AsRef<str>,
        K: AsRef<[u8]>,
    {
        if auth.key.as_ref().is_empty() {
            return Ok(());
        }

        if auth.signature_scheme.as_ref() != "hmac-sha256" {
            return Err(anyhow!(
                "sign jupyter message: unknown signature scheme {}",
                auth.signature_scheme.as_ref()
            ));
        }

        let mut mac = Hmac::<Sha256>::new_from_slice(auth.key.as_ref())?;
        self.update_mac(&mut mac);
        mac.verify_slice(sig).map_err(|err| err.into())
    }

    fn update_mac<U: digest::Update>(&self, u: &mut U) {
        u.update(self.header.as_ref());
        u.update(self.parent_header.as_ref());
        u.update(self.metadata.as_ref());
        u.update(self.content.as_ref());
        for buf in &self.buffers {
            u.update(buf.as_ref());
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct MessageHeader {
    #[serde(rename = "msg_id")]
    pub(crate) id: String,
    pub(crate) session: String,
    pub(crate) username: String,
    pub(crate) date: DateTime<FixedOffset>,
    #[serde(rename = "msg_type")]
    pub(crate) r#type: String,
    pub(crate) version: String,
}

impl MessageHeader {
    fn new(mut rng: impl Rng, typ: impl Into<String>, session: impl Into<String>) -> MessageHeader {
        let mut random_bytes = [0u8; 16];
        rng.fill_bytes(&mut random_bytes);
        let id = UuidBuilder::from_random_bytes(random_bytes).into_uuid();
        MessageHeader {
            id: id.as_hyphenated().to_string(),
            session: session.into(),
            username: "kernel".to_string(),
            date: Utc::now().into(),
            r#type: typ.into(),
            version: PROTOCOL_VERSION.to_string(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct ExecuteRequest {
    pub(crate) code: String,
    pub(crate) silent: bool,
    pub(crate) store_history: bool,
    pub(crate) user_expressions: HashMap<String, String>,
    pub(crate) allow_stdin: bool,
    pub(crate) stop_on_error: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct ExecuteReply {
    pub(crate) execution_count: i32,
    pub(crate) user_expressions: HashMap<String, String>,
}

impl Serialize for ExecuteReply {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut serializer = serializer.serialize_map(Some(3))?;
        serializer.serialize_entry("status", &ReplyStatus::Ok)?;
        serializer.serialize_entry("execution_count", &self.execution_count)?;
        serializer.serialize_entry("user_expressions", &self.user_expressions)?;
        serializer.end()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct IsCompleteRequest {
    pub(crate) code: String,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct IsCompleteReply {
    pub(crate) status: IsCompleteStatus,
    pub(crate) indent: Option<String>,
}

#[repr(i8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub(crate) enum IsCompleteStatus {
    Complete,
    Incomplete,
    Invalid,
    Unknown,
}

impl Default for IsCompleteStatus {
    fn default() -> Self {
        IsCompleteStatus::Unknown
    }
}

impl Serialize for IsCompleteStatus {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            IsCompleteStatus::Complete => serializer.serialize_str("complete"),
            IsCompleteStatus::Incomplete => serializer.serialize_str("incomplete"),
            IsCompleteStatus::Invalid => serializer.serialize_str("invalid"),
            IsCompleteStatus::Unknown => serializer.serialize_str("unknown"),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ErrorReply<'a> {
    pub(crate) exception_name: Cow<'a, str>,
    pub(crate) exception_value: Cow<'a, str>,
    pub(crate) execution_count: Option<i32>,
    pub(crate) traceback: Vec<Cow<'a, str>>,
}

impl<'a> ErrorReply<'a> {
    #[inline]
    pub(crate) fn new(name: impl Into<Cow<'a, str>>) -> Self {
        ErrorReply {
            exception_name: name.into(),
            exception_value: "".into(),
            execution_count: None,
            traceback: Vec::new(),
        }
    }
}

impl<'a> Serialize for ErrorReply<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let field_count = 3
            + (!self.exception_value.is_empty() as usize)
            + (self.execution_count.is_some() as usize);
        let mut serializer = serializer.serialize_map(Some(field_count))?;
        serializer.serialize_entry("status", &ReplyStatus::Error)?;
        serializer.serialize_entry("ename", &self.exception_name)?;
        if !self.exception_value.is_empty() {
            serializer.serialize_entry("evalue", &self.exception_value)?;
        }
        if let Some(n) = self.execution_count {
            serializer.serialize_entry("execution_count", &n)?;
        }
        serializer.serialize_entry("traceback", &self.traceback)?;
        serializer.end()
    }
}

#[repr(i8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub(crate) enum ReplyStatus {
    Ok,
    Error,
}

impl Default for ReplyStatus {
    fn default() -> Self {
        ReplyStatus::Ok
    }
}

impl Serialize for ReplyStatus {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            ReplyStatus::Ok => serializer.serialize_str("ok"),
            ReplyStatus::Error => serializer.serialize_str("error"),
        }
    }
}
