use std::fmt;
use std::path::{Path, PathBuf};

use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::{Aes256Gcm, Nonce};
use hkdf::Hkdf;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use zeroize::{Zeroize, ZeroizeOnDrop};

use crate::id::PilotId;
use crate::identity::DidKey;
use crate::util::write_json_file_atomic as write_json_file_atomic_impl;

const PILOT_KEYS_DIRNAME: &str = "pilot-keys";
const PILOT_ID_FILENAME: &str = "pilot_id.json";
const ACTIVE_PILOT_AUTH_DIRNAME: &str = "pilot_auth";
const ACTIVE_PILOT_AUTH_FILENAME: &str = "current.json";
const ARCHIVE_DIRNAME: &str = "archive";
const KEY_FILE_SCHEMA: &str = "igc-net/pilot-key-file";
const KEY_FILE_VERSION: u8 = 1;
const NONCE_LEN: usize = 12;
const HKDF_LABEL: &[u8] = b"igc-net-pilot-keys-v1";

#[derive(Debug, thiserror::Error)]
pub enum PilotKeyStoreError {
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("pilot key store already initialized")]
    AlreadyInitialized,
    #[error("pilot identity does not exist")]
    MissingPilotIdentity,
    #[error("pilot key store is incomplete: {0}")]
    Incomplete(&'static str),
    #[error("pilot key file is malformed: {0}")]
    Malformed(&'static str),
    #[error("pilot key file for role {found:?} does not match expected role {expected:?}")]
    WrongRole {
        expected: &'static str,
        found: String,
    },
    #[error("pilot key file schema mismatch")]
    SchemaMismatch,
    #[error("pilot key file version {0} is unsupported")]
    UnsupportedVersion(u8),
    #[error("pilot key file could not be decrypted with this node identity")]
    WrongNodeIdentity,
    #[error("unsafe filesystem permissions on {path}: mode {mode:o}")]
    UnsafePermissions { path: PathBuf, mode: u32 },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PilotKeyStoreStatus {
    pub root_dir: PathBuf,
    pub pilot_id_file: PathBuf,
    pub active_pilot_auth_file: PathBuf,
    pub archive_dir: PathBuf,
    pub has_pilot_id: bool,
    pub has_active_pilot_auth: bool,
    pub archived_key_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PilotPublicIdentity {
    pub pilot_id: PilotId,
    pub pilot_id_public_key_hex: String,
    pub active_pilot_auth_public_key_hex: String,
}

pub struct PilotIdentity {
    pilot_id_root: SensitiveKeyMaterial,
    active_pilot_auth: SensitiveKeyMaterial,
}

impl fmt::Debug for PilotIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PilotIdentity")
            .field("pilot_id", &self.pilot_id())
            .field(
                "active_pilot_auth_public_key_hex",
                &self.active_pilot_auth_public_key_hex(),
            )
            .finish()
    }
}

impl PilotIdentity {
    #[cfg(test)]
    pub(crate) fn from_secret_keys(
        pilot_id_root: iroh::SecretKey,
        active_pilot_auth: iroh::SecretKey,
    ) -> Self {
        Self {
            pilot_id_root: SensitiveKeyMaterial::from_secret_key(pilot_id_root),
            active_pilot_auth: SensitiveKeyMaterial::from_secret_key(active_pilot_auth),
        }
    }

    pub fn pilot_id(&self) -> PilotId {
        PilotId::from_public_key(self.pilot_id_root.public_key())
    }

    pub fn pilot_id_public_key_hex(&self) -> String {
        self.pilot_id_root.public_key().to_string()
    }

    pub fn active_pilot_auth_public_key_hex(&self) -> String {
        self.active_pilot_auth.public_key().to_string()
    }

    pub fn active_pilot_auth_did(&self) -> DidKey {
        DidKey::from_public_key(self.active_pilot_auth.public_key())
    }

    pub fn pilot_id_secret_key(&self) -> iroh::SecretKey {
        self.pilot_id_root.secret_key()
    }

    pub fn active_pilot_auth_secret_key(&self) -> iroh::SecretKey {
        self.active_pilot_auth.secret_key()
    }

    pub fn export_public_identity(&self) -> PilotPublicIdentity {
        PilotPublicIdentity {
            pilot_id: self.pilot_id(),
            pilot_id_public_key_hex: self.pilot_id_public_key_hex(),
            active_pilot_auth_public_key_hex: self.active_pilot_auth_public_key_hex(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PilotKeyStore {
    root: PathBuf,
}

impl PilotKeyStore {
    pub fn open(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn for_data_dir(data_dir: impl AsRef<Path>) -> Self {
        Self::open(data_dir.as_ref().join(PILOT_KEYS_DIRNAME))
    }

    pub fn root_dir(&self) -> &Path {
        &self.root
    }

    pub fn init(&self) -> Result<(), PilotKeyStoreError> {
        ensure_dir(self.root_dir())?;
        ensure_dir(&self.active_pilot_auth_dir())?;
        ensure_dir(&self.archive_dir())?;
        Ok(())
    }

    pub fn inspect(&self) -> Result<PilotKeyStoreStatus, PilotKeyStoreError> {
        self.init()?;
        self.inspect_initialized()
    }

    fn inspect_initialized(&self) -> Result<PilotKeyStoreStatus, PilotKeyStoreError> {
        if self.pilot_id_path().exists() {
            ensure_file_permissions(&self.pilot_id_path())?;
        }
        if self.active_pilot_auth_path().exists() {
            ensure_file_permissions(&self.active_pilot_auth_path())?;
        }
        Ok(PilotKeyStoreStatus {
            root_dir: self.root.clone(),
            pilot_id_file: self.pilot_id_path(),
            active_pilot_auth_file: self.active_pilot_auth_path(),
            archive_dir: self.archive_dir(),
            has_pilot_id: self.pilot_id_path().exists(),
            has_active_pilot_auth: self.active_pilot_auth_path().exists(),
            archived_key_count: count_regular_files(&self.archive_dir())?,
        })
    }

    pub fn load(
        &self,
        node_secret_key: &iroh::SecretKey,
    ) -> Result<Option<PilotIdentity>, PilotKeyStoreError> {
        self.init()?;
        let has_pilot_id = self.pilot_id_path().exists();
        let has_active_pilot_auth = self.active_pilot_auth_path().exists();

        match (has_pilot_id, has_active_pilot_auth) {
            (false, false) => Ok(None),
            (true, true) => {
                let node_sealing_key = derive_sealing_key(node_secret_key);
                let pilot_id_root = read_encrypted_secret_key(
                    &self.pilot_id_path(),
                    "pilot_id",
                    &node_sealing_key,
                )?;
                let active_pilot_auth = read_encrypted_secret_key(
                    &self.active_pilot_auth_path(),
                    "pilot_auth",
                    &node_sealing_key,
                )?;
                Ok(Some(PilotIdentity {
                    pilot_id_root,
                    active_pilot_auth,
                }))
            }
            _ => Err(PilotKeyStoreError::Incomplete(
                "both pilot_id and active pilot_auth files must exist",
            )),
        }
    }

    pub fn generate(
        &self,
        node_secret_key: &iroh::SecretKey,
    ) -> Result<PilotIdentity, PilotKeyStoreError> {
        self.init()?;
        let status = self.inspect_initialized()?;
        if status.has_pilot_id || status.has_active_pilot_auth {
            return Err(PilotKeyStoreError::AlreadyInitialized);
        }

        let mut rng = rand::rng();
        let node_key_bytes = node_secret_key.to_bytes();
        let pilot_id_root = generate_distinct_secret_key(&mut rng, &[&node_key_bytes]);
        let active_pilot_auth =
            generate_distinct_secret_key(&mut rng, &[&node_key_bytes, pilot_id_root.as_bytes()]);

        let node_sealing_key = derive_sealing_key(node_secret_key);
        write_encrypted_secret_key(
            &self.pilot_id_path(),
            "pilot_id",
            &pilot_id_root,
            &node_sealing_key,
        )?;
        write_encrypted_secret_key(
            &self.active_pilot_auth_path(),
            "pilot_auth",
            &active_pilot_auth,
            &node_sealing_key,
        )?;

        Ok(PilotIdentity {
            pilot_id_root,
            active_pilot_auth,
        })
    }

    pub fn load_or_generate(
        &self,
        node_secret_key: &iroh::SecretKey,
    ) -> Result<PilotIdentity, PilotKeyStoreError> {
        match self.load(node_secret_key)? {
            Some(identity) => Ok(identity),
            None => self.generate(node_secret_key),
        }
    }

    pub fn export_public_identity(
        &self,
        node_secret_key: &iroh::SecretKey,
    ) -> Result<Option<PilotPublicIdentity>, PilotKeyStoreError> {
        Ok(self
            .load(node_secret_key)?
            .map(|identity| identity.export_public_identity()))
    }

    pub fn archived_pilot_auth_dids(&self) -> Result<Vec<DidKey>, PilotKeyStoreError> {
        self.init()?;
        let mut dids = Vec::new();
        for entry in std::fs::read_dir(self.archive_dir())? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
                continue;
            }
            ensure_file_permissions(&path)?;
            let public_key_hex = path.file_stem().and_then(|stem| stem.to_str()).ok_or(
                PilotKeyStoreError::Malformed("archived pilot_auth filename must be UTF-8"),
            )?;
            let public_key_bytes = decode_fixed_hex::<32>(
                public_key_hex,
                "archived pilot_auth filename must be 32-byte lowercase hex",
            )?;
            let public_key = iroh::PublicKey::from_bytes(&public_key_bytes).map_err(|_| {
                PilotKeyStoreError::Malformed(
                    "archived pilot_auth filename must be a valid Ed25519 public key",
                )
            })?;
            dids.push(DidKey::from_public_key(public_key));
        }
        dids.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        Ok(dids)
    }

    pub fn generate_next_active_pilot_auth_secret_key(
        &self,
        node_secret_key: &iroh::SecretKey,
    ) -> Result<iroh::SecretKey, PilotKeyStoreError> {
        let identity = self
            .load(node_secret_key)?
            .ok_or(PilotKeyStoreError::MissingPilotIdentity)?;
        let mut rng = rand::rng();
        let node_key_bytes = node_secret_key.to_bytes();
        let next = generate_distinct_secret_key(
            &mut rng,
            &[
                &node_key_bytes,
                identity.pilot_id_root.as_bytes(),
                identity.active_pilot_auth.as_bytes(),
            ],
        );
        Ok(next.secret_key())
    }

    pub fn replace_active_pilot_auth(
        &self,
        node_secret_key: &iroh::SecretKey,
        next_active_pilot_auth_secret_key: &iroh::SecretKey,
    ) -> Result<PilotIdentity, PilotKeyStoreError> {
        self.init()?;
        let current_identity = self
            .load(node_secret_key)?
            .ok_or(PilotKeyStoreError::MissingPilotIdentity)?;
        let next_active_pilot_auth =
            SensitiveKeyMaterial::from_secret_key(next_active_pilot_auth_secret_key.clone());
        let node_key_bytes = node_secret_key.to_bytes();
        if next_active_pilot_auth.as_bytes() == current_identity.active_pilot_auth.as_bytes()
            || next_active_pilot_auth.as_bytes() == current_identity.pilot_id_root.as_bytes()
            || next_active_pilot_auth.as_bytes() == &node_key_bytes
        {
            return Err(PilotKeyStoreError::Malformed(
                "replacement pilot_auth key must be distinct",
            ));
        }

        let node_sealing_key = derive_sealing_key(node_secret_key);
        let archive_path = self
            .archived_pilot_auth_path(&current_identity.active_pilot_auth.public_key().to_string());
        if !archive_path.exists() {
            write_encrypted_secret_key(
                &archive_path,
                "pilot_auth",
                &current_identity.active_pilot_auth,
                &node_sealing_key,
            )?;
        }
        write_encrypted_secret_key(
            &self.active_pilot_auth_path(),
            "pilot_auth",
            &next_active_pilot_auth,
            &node_sealing_key,
        )?;

        Ok(PilotIdentity {
            pilot_id_root: current_identity.pilot_id_root,
            active_pilot_auth: next_active_pilot_auth,
        })
    }

    fn pilot_id_path(&self) -> PathBuf {
        self.root.join(PILOT_ID_FILENAME)
    }

    fn active_pilot_auth_dir(&self) -> PathBuf {
        self.root.join(ACTIVE_PILOT_AUTH_DIRNAME)
    }

    fn active_pilot_auth_path(&self) -> PathBuf {
        self.active_pilot_auth_dir()
            .join(ACTIVE_PILOT_AUTH_FILENAME)
    }

    fn archive_dir(&self) -> PathBuf {
        self.active_pilot_auth_dir().join(ARCHIVE_DIRNAME)
    }

    fn archived_pilot_auth_path(&self, public_key_hex: &str) -> PathBuf {
        self.archive_dir().join(format!("{public_key_hex}.json"))
    }
}

// ── PrivateAccessKeyStore ─────────────────────────────────────────────────────

const PRIVATE_ACCESS_KEY_DIRNAME: &str = "private-access-key";
const PRIVATE_ACCESS_KEY_FILENAME: &str = "current.json";
const PRIVATE_ACCESS_ROLE: &str = "private_access";

/// Encrypted-at-rest store for a pilot's `private_access_keypair` private half.
///
/// The key is sealed with the same AES-256-GCM + HKDF envelope as `PilotKeyStore`.
/// After a successful `provision` or `generate`, the node holds Category 2
/// (`private-access`) capability for that pilot until `delete` is called.
#[derive(Debug, Clone)]
pub struct PrivateAccessKeyStore {
    root: PathBuf,
}

impl PrivateAccessKeyStore {
    pub fn open(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn for_data_dir(data_dir: impl AsRef<Path>) -> Self {
        Self::open(data_dir.as_ref().join(PRIVATE_ACCESS_KEY_DIRNAME))
    }

    /// Generate a fresh random `private_access_keypair` and store the private half.
    ///
    /// Fails if a key is already stored (use `delete` first to replace).
    pub fn generate(
        &self,
        node_secret_key: &iroh::SecretKey,
    ) -> Result<iroh::SecretKey, PilotKeyStoreError> {
        ensure_dir(&self.root)?;
        if self.key_path().exists() {
            return Err(PilotKeyStoreError::AlreadyInitialized);
        }
        let mut rng = rand::rng();
        let key = generate_distinct_secret_key(&mut rng, &[&node_secret_key.to_bytes()]);
        let sealing_key = derive_sealing_key(node_secret_key);
        write_encrypted_secret_key(&self.key_path(), PRIVATE_ACCESS_ROLE, &key, &sealing_key)?;
        Ok(key.secret_key())
    }

    /// Store an externally supplied private key (used during the §5 key-provisioning
    /// handover).  Overwrites any previously stored key atomically.
    pub fn provision(
        &self,
        private_key: &iroh::SecretKey,
        node_secret_key: &iroh::SecretKey,
    ) -> Result<(), PilotKeyStoreError> {
        ensure_dir(&self.root)?;
        let material = SensitiveKeyMaterial::from_secret_key(private_key.clone());
        let sealing_key = derive_sealing_key(node_secret_key);
        write_encrypted_secret_key(&self.key_path(), PRIVATE_ACCESS_ROLE, &material, &sealing_key)
    }

    /// Load the stored private key. Returns `None` if no key has been provisioned.
    pub fn load(
        &self,
        node_secret_key: &iroh::SecretKey,
    ) -> Result<Option<iroh::SecretKey>, PilotKeyStoreError> {
        if !self.key_path().exists() {
            return Ok(None);
        }
        let sealing_key = derive_sealing_key(node_secret_key);
        let material =
            read_encrypted_secret_key(&self.key_path(), PRIVATE_ACCESS_ROLE, &sealing_key)?;
        Ok(Some(material.secret_key()))
    }

    /// Delete the stored key (revocation / key rotation cleanup).
    ///
    /// After this call the node loses Category 2 capability for this pilot.
    /// Callers are responsible for deleting any cached restricted plaintext
    /// per R-ACCESS-17.
    pub fn delete(&self) -> Result<(), PilotKeyStoreError> {
        let path = self.key_path();
        if path.exists() {
            std::fs::remove_file(&path)?;
        }
        Ok(())
    }

    fn key_path(&self) -> PathBuf {
        self.root.join(PRIVATE_ACCESS_KEY_FILENAME)
    }
}

#[derive(Clone, PartialEq, Eq, Zeroize, ZeroizeOnDrop)]
struct SensitiveKeyMaterial([u8; 32]);

impl fmt::Debug for SensitiveKeyMaterial {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SensitiveKeyMaterial(..)")
    }
}

impl SensitiveKeyMaterial {
    fn from_secret_key(secret_key: iroh::SecretKey) -> Self {
        Self(secret_key.to_bytes())
    }

    fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    fn secret_key(&self) -> iroh::SecretKey {
        iroh::SecretKey::from_bytes(self.as_bytes())
    }

    fn public_key(&self) -> iroh::PublicKey {
        self.secret_key().public()
    }
}

#[derive(Serialize, Deserialize)]
struct EncryptedKeyFile {
    schema: String,
    schema_version: u8,
    role: String,
    nonce: String,
    ciphertext: String,
}

fn write_encrypted_secret_key(
    path: &Path,
    role: &'static str,
    secret_key: &SensitiveKeyMaterial,
    node_sealing_key: &[u8; 32],
) -> Result<(), PilotKeyStoreError> {
    let envelope = seal_secret_key(role, secret_key, node_sealing_key)?;
    write_json_file_atomic(path, &envelope)
}

fn read_encrypted_secret_key(
    path: &Path,
    expected_role: &'static str,
    node_sealing_key: &[u8; 32],
) -> Result<SensitiveKeyMaterial, PilotKeyStoreError> {
    ensure_file_permissions(path)?;
    let envelope: EncryptedKeyFile = serde_json::from_slice(&std::fs::read(path)?)?;
    unseal_secret_key(expected_role, envelope, node_sealing_key)
}

fn seal_secret_key(
    role: &'static str,
    secret_key: &SensitiveKeyMaterial,
    node_sealing_key: &[u8; 32],
) -> Result<EncryptedKeyFile, PilotKeyStoreError> {
    let cipher = Aes256Gcm::new_from_slice(node_sealing_key)
        .map_err(|_| PilotKeyStoreError::Malformed("invalid AES-256-GCM key"))?;
    let mut nonce_bytes = [0u8; NONCE_LEN];
    rand::fill(&mut nonce_bytes);
    let aad = aad_for_role(role);
    let ciphertext = cipher
        .encrypt(
            Nonce::from_slice(&nonce_bytes),
            Payload {
                msg: secret_key.as_bytes(),
                aad: aad.as_bytes(),
            },
        )
        .map_err(|_| PilotKeyStoreError::Malformed("encryption failed"))?;

    Ok(EncryptedKeyFile {
        schema: KEY_FILE_SCHEMA.to_string(),
        schema_version: KEY_FILE_VERSION,
        role: role.to_string(),
        nonce: hex::encode(nonce_bytes),
        ciphertext: hex::encode(ciphertext),
    })
}

fn unseal_secret_key(
    expected_role: &'static str,
    envelope: EncryptedKeyFile,
    node_sealing_key: &[u8; 32],
) -> Result<SensitiveKeyMaterial, PilotKeyStoreError> {
    if envelope.schema != KEY_FILE_SCHEMA {
        return Err(PilotKeyStoreError::SchemaMismatch);
    }
    if envelope.schema_version != KEY_FILE_VERSION {
        return Err(PilotKeyStoreError::UnsupportedVersion(
            envelope.schema_version,
        ));
    }
    if envelope.role != expected_role {
        return Err(PilotKeyStoreError::WrongRole {
            expected: expected_role,
            found: envelope.role,
        });
    }

    let nonce_bytes = decode_fixed_hex::<NONCE_LEN>(&envelope.nonce, "invalid nonce encoding")?;
    let ciphertext = hex::decode(&envelope.ciphertext)
        .map_err(|_| PilotKeyStoreError::Malformed("invalid ciphertext encoding"))?;
    if ciphertext.is_empty() {
        return Err(PilotKeyStoreError::Malformed(
            "ciphertext must not be empty",
        ));
    }

    let cipher = Aes256Gcm::new_from_slice(node_sealing_key)
        .map_err(|_| PilotKeyStoreError::Malformed("invalid AES-256-GCM key"))?;
    let plaintext = cipher
        .decrypt(
            Nonce::from_slice(&nonce_bytes),
            Payload {
                msg: ciphertext.as_ref(),
                aad: aad_for_role(expected_role).as_bytes(),
            },
        )
        .map_err(|_| PilotKeyStoreError::WrongNodeIdentity)?;

    let secret_key_bytes: [u8; 32] = plaintext
        .as_slice()
        .try_into()
        .map_err(|_| PilotKeyStoreError::Malformed("decrypted key must be 32 bytes"))?;
    Ok(SensitiveKeyMaterial::from_bytes(secret_key_bytes))
}

fn aad_for_role(role: &str) -> String {
    format!("{KEY_FILE_SCHEMA}:v{KEY_FILE_VERSION}:{role}")
}

fn write_json_file_atomic(path: &Path, value: &EncryptedKeyFile) -> Result<(), PilotKeyStoreError> {
    write_json_file_atomic_impl(
        path,
        value,
        ensure_dir,
        write_private_file,
        PilotKeyStoreError::Incomplete("pilot key file path has no parent directory"),
    )?;
    ensure_file_permissions(path)?;
    Ok(())
}

fn count_regular_files(path: &Path) -> Result<usize, PilotKeyStoreError> {
    if !path.exists() {
        return Ok(0);
    }
    Ok(std::fs::read_dir(path)?
        .filter_map(Result::ok)
        .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_file()))
        .count())
}

fn derive_sealing_key(node_secret_key: &iroh::SecretKey) -> [u8; 32] {
    let ikm = node_secret_key.to_bytes();
    let hkdf = Hkdf::<Sha256>::new(None, &ikm);
    let mut out = [0u8; 32];
    hkdf.expand(HKDF_LABEL, &mut out)
        .expect("32-byte HKDF expand is always valid for SHA-256");
    out
}

fn generate_distinct_secret_key(
    rng: &mut impl rand::CryptoRng,
    disallowed_keys: &[&[u8; 32]],
) -> SensitiveKeyMaterial {
    loop {
        let candidate = SensitiveKeyMaterial::from_secret_key(iroh::SecretKey::generate(rng));
        if disallowed_keys
            .iter()
            .all(|other| candidate.as_bytes() != *other)
        {
            return candidate;
        }
    }
}

fn decode_fixed_hex<const N: usize>(
    value: &str,
    error: &'static str,
) -> Result<[u8; N], PilotKeyStoreError> {
    let decoded = hex::decode(value).map_err(|_| PilotKeyStoreError::Malformed(error))?;
    decoded
        .try_into()
        .map_err(|_| PilotKeyStoreError::Malformed(error))
}

#[cfg(unix)]
fn ensure_dir(path: &Path) -> Result<(), PilotKeyStoreError> {
    use std::os::unix::fs::{DirBuilderExt, PermissionsExt};

    if !path.exists() {
        let mut builder = std::fs::DirBuilder::new();
        builder.mode(0o700);
        builder.create(path)?;
    }

    let mode = std::fs::metadata(path)?.permissions().mode() & 0o777;
    if mode & 0o077 != 0 {
        return Err(PilotKeyStoreError::UnsafePermissions {
            path: path.to_path_buf(),
            mode,
        });
    }
    Ok(())
}

#[cfg(not(unix))]
fn ensure_dir(path: &Path) -> Result<(), PilotKeyStoreError> {
    std::fs::create_dir_all(path)?;
    Ok(())
}

#[cfg(unix)]
fn ensure_file_permissions(path: &Path) -> Result<(), PilotKeyStoreError> {
    use std::os::unix::fs::PermissionsExt;

    let mode = std::fs::metadata(path)?.permissions().mode() & 0o777;
    if mode & 0o077 != 0 {
        return Err(PilotKeyStoreError::UnsafePermissions {
            path: path.to_path_buf(),
            mode,
        });
    }
    Ok(())
}

#[cfg(not(unix))]
fn ensure_file_permissions(_path: &Path) -> Result<(), PilotKeyStoreError> {
    Ok(())
}

#[cfg(unix)]
fn write_private_file(path: &Path, bytes: &[u8]) -> Result<(), PilotKeyStoreError> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;

    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(bytes)?;
    file.flush()?;
    Ok(())
}

#[cfg(not(unix))]
fn write_private_file(path: &Path, bytes: &[u8]) -> Result<(), PilotKeyStoreError> {
    use std::io::Write;

    let mut file = std::fs::File::create(path)?;
    file.write_all(bytes)?;
    file.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn deterministic_secret_key(byte: u8) -> iroh::SecretKey {
        iroh::SecretKey::from_bytes(&[byte; 32])
    }

    fn temp_store() -> (PilotKeyStore, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let store = PilotKeyStore::for_data_dir(dir.path());
        store.init().unwrap();
        (store, dir)
    }

    #[test]
    fn hkdf_matches_rfc_5869_test_vector_case_1() {
        let ikm = [0x0b; 22];
        let salt = hex::decode("000102030405060708090a0b0c").unwrap();
        let info = hex::decode("f0f1f2f3f4f5f6f7f8f9").unwrap();
        let (prk, hkdf) = Hkdf::<Sha256>::extract(Some(&salt), &ikm);
        assert_eq!(
            hex::encode(prk),
            "077709362c2e32df0ddc3f0dc47bba6390b6c73bb50f9c3122ec844ad7c2b3e5"
        );

        let mut okm = [0u8; 32];
        hkdf.expand(&info, &mut okm).unwrap();
        assert_eq!(
            hex::encode(okm),
            "3cb25f25faacd57a90434f64d0362f2a2d2d0a90cf1a5a4c5db02d56ecc4c5bf"
        );
    }

    #[test]
    fn generate_and_load_round_trip_same_node_identity() {
        let (store, _dir) = temp_store();
        let node_secret_key = deterministic_secret_key(7);

        let generated = store.load_or_generate(&node_secret_key).unwrap();
        let loaded = store.load(&node_secret_key).unwrap().unwrap();

        assert_eq!(generated.pilot_id(), loaded.pilot_id());
        assert_eq!(
            generated.active_pilot_auth_public_key_hex(),
            loaded.active_pilot_auth_public_key_hex()
        );
    }

    #[test]
    fn encrypted_key_files_do_not_store_plaintext_secret_bytes() {
        let (store, _dir) = temp_store();
        let node_secret_key = deterministic_secret_key(9);
        let identity = store.generate(&node_secret_key).unwrap();

        let pilot_id_file = std::fs::read(store.pilot_id_path()).unwrap();
        let pilot_auth_file = std::fs::read(store.active_pilot_auth_path()).unwrap();

        assert!(
            !pilot_id_file
                .windows(32)
                .any(|window| window == identity.pilot_id_secret_key().to_bytes())
        );
        assert!(
            !pilot_auth_file
                .windows(32)
                .any(|window| window == identity.active_pilot_auth_secret_key().to_bytes())
        );
    }

    #[test]
    fn wrong_node_identity_cannot_decrypt_pilot_keys() {
        let (store, _dir) = temp_store();
        store.generate(&deterministic_secret_key(11)).unwrap();

        let err = store.load(&deterministic_secret_key(12)).unwrap_err();
        assert!(matches!(err, PilotKeyStoreError::WrongNodeIdentity));
    }

    #[test]
    fn malformed_key_files_are_rejected() {
        let (store, _dir) = temp_store();
        let node_secret_key = deterministic_secret_key(13);
        store.generate(&node_secret_key).unwrap();

        std::fs::write(store.pilot_id_path(), b"{not json").unwrap();
        assert!(matches!(
            store.load(&node_secret_key).unwrap_err(),
            PilotKeyStoreError::Json(_)
        ));

        std::fs::write(
            store.active_pilot_auth_path(),
            serde_json::to_vec(&EncryptedKeyFile {
                schema: KEY_FILE_SCHEMA.to_string(),
                schema_version: KEY_FILE_VERSION,
                role: "pilot_auth".to_string(),
                nonce: "abcd".to_string(),
                ciphertext: "00".to_string(),
            })
            .unwrap(),
        )
        .unwrap();
        assert!(matches!(
            store.load(&node_secret_key).unwrap_err(),
            PilotKeyStoreError::Json(_) | PilotKeyStoreError::Malformed(_)
        ));
    }

    #[test]
    fn inspect_reports_layout_and_public_export() {
        let (store, _dir) = temp_store();
        let node_secret_key = deterministic_secret_key(21);
        let identity = store.generate(&node_secret_key).unwrap();

        let status = store.inspect().unwrap();
        assert!(status.has_pilot_id);
        assert!(status.has_active_pilot_auth);
        assert_eq!(status.archived_key_count, 0);

        let exported = store
            .export_public_identity(&node_secret_key)
            .unwrap()
            .unwrap();
        assert_eq!(exported.pilot_id, identity.pilot_id());
        assert_eq!(
            exported.active_pilot_auth_public_key_hex,
            identity.active_pilot_auth_public_key_hex()
        );
    }

    #[cfg(unix)]
    #[test]
    fn unsafe_permissions_are_rejected() {
        use std::os::unix::fs::PermissionsExt;

        let (store, _dir) = temp_store();
        let node_secret_key = deterministic_secret_key(31);
        store.generate(&node_secret_key).unwrap();

        let mut file_permissions = std::fs::metadata(store.pilot_id_path())
            .unwrap()
            .permissions();
        file_permissions.set_mode(0o644);
        std::fs::set_permissions(store.pilot_id_path(), file_permissions).unwrap();
        assert!(matches!(
            store.load(&node_secret_key).unwrap_err(),
            PilotKeyStoreError::UnsafePermissions { .. }
        ));

        let mut dir_permissions = std::fs::metadata(store.active_pilot_auth_dir())
            .unwrap()
            .permissions();
        dir_permissions.set_mode(0o755);
        std::fs::set_permissions(store.active_pilot_auth_dir(), dir_permissions).unwrap();
        assert!(matches!(
            store.inspect().unwrap_err(),
            PilotKeyStoreError::UnsafePermissions { .. }
        ));
    }

    #[test]
    fn incomplete_key_store_is_rejected() {
        let (store, _dir) = temp_store();
        let node_secret_key = deterministic_secret_key(41);
        store.generate(&node_secret_key).unwrap();
        std::fs::remove_file(store.active_pilot_auth_path()).unwrap();

        assert!(matches!(
            store.load(&node_secret_key).unwrap_err(),
            PilotKeyStoreError::Incomplete(_)
        ));
    }

    #[test]
    fn archived_pilot_auth_dids_lists_retired_identifiers_only() {
        let (store, _dir) = temp_store();
        let node_secret_key = deterministic_secret_key(51);
        let identity = store.load_or_generate(&node_secret_key).unwrap();
        let next = store
            .generate_next_active_pilot_auth_secret_key(&node_secret_key)
            .unwrap();
        let retired = identity.active_pilot_auth_did();

        let rotated = store
            .replace_active_pilot_auth(&node_secret_key, &next)
            .unwrap();
        let archived = store.archived_pilot_auth_dids().unwrap();

        assert_eq!(archived, vec![retired]);
        assert_ne!(archived[0], rotated.active_pilot_auth_did());
    }

    // ── PrivateAccessKeyStore ─────────────────────────────────────────────────

    fn temp_private_access_store() -> (PrivateAccessKeyStore, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let store = PrivateAccessKeyStore::for_data_dir(dir.path());
        (store, dir)
    }

    #[test]
    fn private_access_generate_and_load_round_trip() {
        let (store, _dir) = temp_private_access_store();
        let node_key = deterministic_secret_key(61);

        let generated = store.generate(&node_key).unwrap();
        let loaded = store.load(&node_key).unwrap().unwrap();

        assert_eq!(generated.to_bytes(), loaded.to_bytes());
    }

    #[test]
    fn private_access_load_returns_none_when_absent() {
        let (store, _dir) = temp_private_access_store();
        let node_key = deterministic_secret_key(62);
        assert!(store.load(&node_key).unwrap().is_none());
    }

    #[test]
    fn private_access_generate_rejects_second_call() {
        let (store, _dir) = temp_private_access_store();
        let node_key = deterministic_secret_key(63);
        store.generate(&node_key).unwrap();
        assert!(matches!(
            store.generate(&node_key),
            Err(PilotKeyStoreError::AlreadyInitialized)
        ));
    }

    #[test]
    fn private_access_provision_stores_external_key() {
        let (store, _dir) = temp_private_access_store();
        let node_key = deterministic_secret_key(64);
        let external_key = deterministic_secret_key(65);

        store.provision(&external_key, &node_key).unwrap();
        let loaded = store.load(&node_key).unwrap().unwrap();

        assert_eq!(external_key.to_bytes(), loaded.to_bytes());
    }

    #[test]
    fn private_access_provision_overwrites_existing_key() {
        let (store, _dir) = temp_private_access_store();
        let node_key = deterministic_secret_key(66);

        store.provision(&deterministic_secret_key(67), &node_key).unwrap();
        store.provision(&deterministic_secret_key(68), &node_key).unwrap();
        let loaded = store.load(&node_key).unwrap().unwrap();

        assert_eq!(loaded.to_bytes(), deterministic_secret_key(68).to_bytes());
    }

    #[test]
    fn private_access_delete_clears_stored_key() {
        let (store, _dir) = temp_private_access_store();
        let node_key = deterministic_secret_key(69);
        store.generate(&node_key).unwrap();

        store.delete().unwrap();

        assert!(store.load(&node_key).unwrap().is_none());
    }

    #[test]
    fn private_access_delete_is_idempotent_when_absent() {
        let (store, _dir) = temp_private_access_store();
        store.delete().unwrap(); // no key present — must not error
    }

    #[test]
    fn private_access_wrong_node_key_cannot_decrypt() {
        let (store, _dir) = temp_private_access_store();
        store.generate(&deterministic_secret_key(71)).unwrap();
        assert!(matches!(
            store.load(&deterministic_secret_key(72)),
            Err(PilotKeyStoreError::WrongNodeIdentity)
        ));
    }
}
