use serde::{Deserialize, Serialize};

use crate::id::{Blake3Hex, NodeIdHex};
use crate::store::PublicationMode;

const ARTIFACT_ANNOUNCEMENT_SCHEMA: &str = "igc-net/announcement";
const ARTIFACT_ANNOUNCEMENT_VERSION: u8 = 1;
const MAX_ANNOUNCEMENT_BYTES: usize = 1024;

#[derive(Debug, thiserror::Error)]
pub(crate) enum ArtifactAnnouncementError {
    #[error("JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("announcement exceeds 1024-byte limit: {0} bytes")]
    TooLarge(usize),
    #[error("invalid {ticket} ticket: {message}")]
    InvalidTicket {
        ticket: &'static str,
        message: String,
    },
    #[error("{ticket} ticket hash mismatch")]
    TicketHashMismatch { ticket: &'static str },
    #[error("{ticket} ticket node mismatch")]
    TicketNodeMismatch { ticket: &'static str },
    #[error("unsupported announcement schema: {0}")]
    UnsupportedSchema(String),
    #[error("schema_version is unsupported: {0}")]
    UnsupportedVersion(u8),
    #[error("record_id mismatch")]
    RecordIdMismatch,
    #[error("signature verification failed")]
    SignatureVerification,
    #[error("signature must be 128 lowercase hex chars")]
    SignatureEncoding,
    #[error("announcement tickets are required")]
    MissingTickets,
    #[error("protected_hash presence does not match publication_mode")]
    ProtectedHashPresence,
    #[error("companion_tickets presence does not match publication_mode")]
    CompanionTicketPresence,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ArtifactAnnouncement {
    pub(crate) schema: String,
    pub(crate) schema_version: u8,
    pub(crate) record_id: Blake3Hex,
    pub(crate) raw_igc_hash: Blake3Hex,
    pub(crate) publication_mode: PublicationMode,
    pub(crate) tickets: Vec<String>,
    pub(crate) node_id: NodeIdHex,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) protected_hash: Option<Blake3Hex>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) companion_tickets: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) g_record_present: Option<bool>,
    pub(crate) signature: String,
    pub(crate) created_at: String,
}

impl ArtifactAnnouncement {
    pub(crate) fn signed(
        node_secret_key: &iroh::SecretKey,
        raw_igc_hash: Blake3Hex,
        publication_mode: PublicationMode,
        tickets: Vec<String>,
        node_id: NodeIdHex,
        protected_hash: Option<Blake3Hex>,
        companion_tickets: Vec<String>,
        g_record_present: Option<bool>,
        created_at: String,
    ) -> Result<Self, ArtifactAnnouncementError> {
        let record_id = derive_record_id(
            &raw_igc_hash,
            &publication_mode,
            &tickets,
            &node_id,
            protected_hash.as_ref(),
            &companion_tickets,
            g_record_present,
            &created_at,
        )?;
        let signing_bytes = signing_payload(
            &record_id,
            &raw_igc_hash,
            &publication_mode,
            &tickets,
            &node_id,
            protected_hash.as_ref(),
            &companion_tickets,
            g_record_present,
            &created_at,
        )?;
        let signature = hex::encode(node_secret_key.sign(&signing_bytes).to_bytes());

        Ok(Self {
            schema: ARTIFACT_ANNOUNCEMENT_SCHEMA.to_string(),
            schema_version: ARTIFACT_ANNOUNCEMENT_VERSION,
            record_id,
            raw_igc_hash,
            publication_mode,
            tickets,
            node_id,
            protected_hash,
            companion_tickets,
            g_record_present,
            signature,
            created_at,
        })
    }

    pub(crate) fn to_gossip_bytes(&self) -> Result<Vec<u8>, ArtifactAnnouncementError> {
        let json = serde_json::to_vec(self)?;
        validate_payload_size(&json)?;
        Ok(json)
    }

    pub(crate) fn parse_and_validate(
        payload: &[u8],
    ) -> Result<ValidatedArtifactAnnouncement, ArtifactAnnouncementError> {
        validate_payload_size(payload)?;
        let ann: Self = serde_json::from_slice(payload)?;
        ann.validate()
    }

    pub(crate) fn validate(
        self,
    ) -> Result<ValidatedArtifactAnnouncement, ArtifactAnnouncementError> {
        if self.schema != ARTIFACT_ANNOUNCEMENT_SCHEMA {
            return Err(ArtifactAnnouncementError::UnsupportedSchema(self.schema));
        }
        if self.schema_version != ARTIFACT_ANNOUNCEMENT_VERSION {
            return Err(ArtifactAnnouncementError::UnsupportedVersion(
                self.schema_version,
            ));
        }
        if self.tickets.is_empty() {
            return Err(ArtifactAnnouncementError::MissingTickets);
        }
        match self.publication_mode {
            PublicationMode::Protected => {
                if self.protected_hash.is_none() {
                    return Err(ArtifactAnnouncementError::ProtectedHashPresence);
                }
            }
            PublicationMode::Public | PublicationMode::Private => {
                if self.protected_hash.is_some() {
                    return Err(ArtifactAnnouncementError::ProtectedHashPresence);
                }
                if !self.companion_tickets.is_empty() {
                    return Err(ArtifactAnnouncementError::CompanionTicketPresence);
                }
            }
        }

        let expected_record_id = derive_record_id(
            &self.raw_igc_hash,
            &self.publication_mode,
            &self.tickets,
            &self.node_id,
            self.protected_hash.as_ref(),
            &self.companion_tickets,
            self.g_record_present,
            &self.created_at,
        )?;
        if self.record_id != expected_record_id {
            return Err(ArtifactAnnouncementError::RecordIdMismatch);
        }

        let signature = decode_signature_hex(&self.signature)?;
        let signing_payload = signing_payload(
            &self.record_id,
            &self.raw_igc_hash,
            &self.publication_mode,
            &self.tickets,
            &self.node_id,
            self.protected_hash.as_ref(),
            &self.companion_tickets,
            self.g_record_present,
            &self.created_at,
        )?;
        node_id_public_key(&self.node_id)?
            .verify(&signing_payload, &signature)
            .map_err(|_| ArtifactAnnouncementError::SignatureVerification)?;

        let tickets = self
            .tickets
            .iter()
            .map(|ticket| parse_artifact_ticket(ticket, "artifact"))
            .collect::<Result<Vec<_>, _>>()?;
        let companion_tickets = self
            .companion_tickets
            .iter()
            .map(|ticket| parse_artifact_ticket(ticket, "companion"))
            .collect::<Result<Vec<_>, _>>()?;

        for ticket in &tickets {
            let expected_hash = match self.publication_mode {
                PublicationMode::Public | PublicationMode::Private => &self.raw_igc_hash,
                PublicationMode::Protected => self
                    .protected_hash
                    .as_ref()
                    .expect("protected hash was checked above"),
            };
            validate_ticket_identity(ticket, expected_hash, &self.node_id, "artifact")?;
        }
        for ticket in &companion_tickets {
            validate_ticket_identity(ticket, &self.raw_igc_hash, &self.node_id, "companion")?;
        }

        Ok(ValidatedArtifactAnnouncement { ann: self, tickets })
    }
}

pub(crate) struct ValidatedArtifactAnnouncement {
    pub(crate) ann: ArtifactAnnouncement,
    pub(crate) tickets: Vec<iroh_blobs::ticket::BlobTicket>,
}

#[derive(Serialize)]
struct ArtifactAnnouncementIdPayload<'a> {
    schema: &'static str,
    schema_version: u8,
    raw_igc_hash: &'a Blake3Hex,
    publication_mode: &'a PublicationMode,
    tickets: &'a [String],
    node_id: &'a NodeIdHex,
    protected_hash: Option<&'a Blake3Hex>,
    companion_tickets: &'a [String],
    #[serde(skip_serializing_if = "Option::is_none")]
    g_record_present: Option<bool>,
    created_at: &'a str,
}

#[derive(Serialize)]
struct ArtifactAnnouncementSigningPayload<'a> {
    schema: &'static str,
    schema_version: u8,
    record_id: &'a Blake3Hex,
    raw_igc_hash: &'a Blake3Hex,
    publication_mode: &'a PublicationMode,
    tickets: &'a [String],
    node_id: &'a NodeIdHex,
    protected_hash: Option<&'a Blake3Hex>,
    companion_tickets: &'a [String],
    #[serde(skip_serializing_if = "Option::is_none")]
    g_record_present: Option<bool>,
    created_at: &'a str,
}

fn validate_payload_size(payload: &[u8]) -> Result<(), ArtifactAnnouncementError> {
    if payload.len() <= MAX_ANNOUNCEMENT_BYTES {
        Ok(())
    } else {
        Err(ArtifactAnnouncementError::TooLarge(payload.len()))
    }
}

fn derive_record_id(
    raw_igc_hash: &Blake3Hex,
    publication_mode: &PublicationMode,
    tickets: &[String],
    node_id: &NodeIdHex,
    protected_hash: Option<&Blake3Hex>,
    companion_tickets: &[String],
    g_record_present: Option<bool>,
    created_at: &str,
) -> Result<Blake3Hex, ArtifactAnnouncementError> {
    let payload = ArtifactAnnouncementIdPayload {
        schema: ARTIFACT_ANNOUNCEMENT_SCHEMA,
        schema_version: ARTIFACT_ANNOUNCEMENT_VERSION,
        raw_igc_hash,
        publication_mode,
        tickets,
        node_id,
        protected_hash,
        companion_tickets,
        g_record_present,
        created_at,
    };
    let bytes = json_canon::to_vec(&payload)?;
    Ok(Blake3Hex::from_hash(blake3::hash(&bytes)))
}

pub(crate) fn signing_payload(
    record_id: &Blake3Hex,
    raw_igc_hash: &Blake3Hex,
    publication_mode: &PublicationMode,
    tickets: &[String],
    node_id: &NodeIdHex,
    protected_hash: Option<&Blake3Hex>,
    companion_tickets: &[String],
    g_record_present: Option<bool>,
    created_at: &str,
) -> Result<Vec<u8>, ArtifactAnnouncementError> {
    let payload = ArtifactAnnouncementSigningPayload {
        schema: ARTIFACT_ANNOUNCEMENT_SCHEMA,
        schema_version: ARTIFACT_ANNOUNCEMENT_VERSION,
        record_id,
        raw_igc_hash,
        publication_mode,
        tickets,
        node_id,
        protected_hash,
        companion_tickets,
        g_record_present,
        created_at,
    };
    Ok(json_canon::to_vec(&payload)?)
}

fn parse_artifact_ticket(
    ticket: &str,
    name: &'static str,
) -> Result<iroh_blobs::ticket::BlobTicket, ArtifactAnnouncementError> {
    ticket
        .parse::<iroh_blobs::ticket::BlobTicket>()
        .map_err(|e| ArtifactAnnouncementError::InvalidTicket {
            ticket: name,
            message: e.to_string(),
        })
}

fn validate_ticket_identity(
    ticket: &iroh_blobs::ticket::BlobTicket,
    expected_hash: &Blake3Hex,
    expected_node_id: &NodeIdHex,
    name: &'static str,
) -> Result<(), ArtifactAnnouncementError> {
    if Blake3Hex::from_bytes(ticket.hash().as_bytes()) != *expected_hash {
        return Err(ArtifactAnnouncementError::TicketHashMismatch { ticket: name });
    }
    if NodeIdHex::from_public_key(ticket.addr().id) != *expected_node_id {
        return Err(ArtifactAnnouncementError::TicketNodeMismatch { ticket: name });
    }
    Ok(())
}

fn decode_signature_hex(value: &str) -> Result<iroh::Signature, ArtifactAnnouncementError> {
    if value.len() != 128
        || !value
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
    {
        return Err(ArtifactAnnouncementError::SignatureEncoding);
    }
    let bytes = hex::decode(value).map_err(|_| ArtifactAnnouncementError::SignatureEncoding)?;
    let signature_bytes: [u8; 64] = bytes
        .try_into()
        .map_err(|_| ArtifactAnnouncementError::SignatureEncoding)?;
    Ok(iroh::Signature::from_bytes(&signature_bytes))
}

fn node_id_public_key(node_id: &NodeIdHex) -> Result<iroh::PublicKey, ArtifactAnnouncementError> {
    let bytes =
        hex::decode(node_id.as_str()).map_err(|_| ArtifactAnnouncementError::SignatureEncoding)?;
    let key_bytes: [u8; 32] = bytes
        .try_into()
        .map_err(|_| ArtifactAnnouncementError::SignatureEncoding)?;
    iroh::PublicKey::from_bytes(&key_bytes)
        .map_err(|_| ArtifactAnnouncementError::SignatureEncoding)
}
