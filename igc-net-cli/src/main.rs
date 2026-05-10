//! igc-net — command-line node for the igc-net protocol.
//!
//! All subcommands accept `--data-dir` (default: `$HOME/.igc-net`).

use std::collections::BTreeMap;
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use igc_net::{
    Blake3Hex, DeletionRequestRecord, DidKey, FetchPolicy, FlatFileStore, FlightMetadata,
    GovernanceStore, IgcIrohNode, IndexerConfig, MultiPilotKeyStore, OwnerClaimRecord,
    PilotAuthDidRecord, PilotAuthDidStateStatus, PilotId, PilotIdentity,
    PilotProfileCredentialRequest, PilotProfileCredentialSubjectDraft,
    PilotProfileCredentialVerification, PilotProfileCredentialVerifier, PublicationMode,
    PublicationModeRecord, SystemClock, announce_topic_id, issue_initial_pilot_auth_did_record,
    issue_pilot_profile_credential, publish, publish_private, publish_protected,
    rotate_pilot_auth_did_record, run_indexer,
};

// ── CLI definition ────────────────────────────────────────────────────────────

#[derive(Parser)]
#[command(
    name = "igc-net",
    about = "Open P2P protocol for publishing and exchanging IGC flight logs",
    version
)]
struct Cli {
    /// Root data directory (env: IGC_NET_DATA_DIR).
    #[arg(long, global = true, value_name = "PATH")]
    data_dir: Option<PathBuf>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Publish an IGC file to the network.
    Announce {
        /// Path to the IGC file to publish.
        file: PathBuf,

        /// Stay alive for N seconds after publishing so peers can fetch blobs.
        ///
        /// Defaults to 0 (exit immediately after publishing).  Set to a non-zero
        /// value in integration tests so indexers have time to download the blobs
        /// before the serving node closes.  Also prints a pre-publish pause of
        /// 2 s when set, giving bootstrapped indexers time to join the gossip
        /// swarm before the announcement is broadcast.
        #[arg(long, default_value = "0")]
        linger: u64,
    },

    /// Run an indexer node (Ctrl-C to stop).
    #[command(name = "runindex")]
    RunIndex {
        /// Fetch policy: index-only | eager | geo:<min_lat>,<max_lat>,<min_lon>,<max_lon>
        #[arg(long, default_value = "index-only")]
        policy: String,

        /// Comma-separated iroh node IDs (hex-encoded Ed25519 public keys)
        /// to bootstrap the gossip swarm from.
        #[arg(long, value_delimiter = ',')]
        bootstrap: Vec<String>,

        /// Pre-populate the address book for direct loopback connections.
        ///
        /// Format: `<node_id_hex>@<ip>:<port>`  e.g. `abc...@127.0.0.1:12345`
        ///
        /// Use together with `--bootstrap` in tests and private networks where
        /// relay-based peer discovery is unavailable or undesirable.
        #[arg(long)]
        peer_addr: Vec<String>,
    },

    /// Fetch a raw IGC blob by its BLAKE3 hash.
    Fetch {
        /// 64-char BLAKE3 hex hash of the IGC file.
        igc_hash: String,
        /// Output file path (default: <igc_hash>.igc).
        #[arg(long, short)]
        out: Option<PathBuf>,
    },

    /// Inspect an IGC file or metadata blob offline (no node required).
    Inspect {
        /// Path to an `.igc` file or `meta.json` metadata blob.
        file: PathBuf,
    },

    /// List all flights in the local index.
    List,

    /// Inspect pilot identity and pilot-auth-did governance state offline.
    #[command(name = "pilot-auth-status")]
    PilotAuthStatus,

    /// Inspect and validate a `did:key` value offline.
    #[command(name = "did-key-inspect")]
    DidKeyInspect {
        /// The `did:key` value to inspect.
        did: String,
    },

    /// Create and persist the initial pilot-auth-did-record offline.
    #[command(name = "pilot-auth-issue-initial")]
    PilotAuthIssueInitial {
        /// Override created_at. Defaults to current UTC time truncated to seconds.
        #[arg(long)]
        created_at: Option<String>,
    },

    /// Rotate the active pilot_auth_did key and persist the new governance record offline.
    #[command(name = "pilot-auth-rotate")]
    PilotAuthRotate {
        /// Override created_at. Defaults to current UTC time truncated to seconds.
        #[arg(long)]
        created_at: Option<String>,
    },

    /// Parse and print a pilot-auth-did record JSON file offline.
    #[command(name = "pilot-auth-record-inspect")]
    PilotAuthRecordInspect {
        /// Path to a JSON file containing a pilot-auth-did record.
        file: PathBuf,
    },

    /// Validate a pilot-auth-did record JSON file offline.
    #[command(name = "pilot-auth-record-verify")]
    PilotAuthRecordVerify {
        /// Path to a JSON file containing a pilot-auth-did record.
        file: PathBuf,
    },

    /// Issue a PilotProfileCredential VC-JWT offline using local authoritative state.
    #[command(name = "pilot-profile-issue")]
    PilotProfileIssue {
        /// Credential JWT ID.
        #[arg(long)]
        jti: String,

        /// Optional display name.
        #[arg(long)]
        name: Option<String>,

        /// Optional ISO 3166-1 alpha-2 country code.
        #[arg(long)]
        country: Option<String>,

        /// Optional audience claim.
        #[arg(long)]
        audience: Option<String>,

        /// Optional expiration window in seconds.
        #[arg(long)]
        expires_in_seconds: Option<u64>,

        /// Optional output path for the compact JWT.
        #[arg(long)]
        out: Option<PathBuf>,
    },

    /// Verify a PilotProfileCredential VC-JWT offline against local governance state.
    #[command(name = "pilot-profile-verify")]
    PilotProfileVerify {
        /// Path to a file containing a compact JWT, or the compact JWT itself.
        input: String,

        /// Optional audience expected to be present in the `aud` claim.
        #[arg(long)]
        expected_audience: Option<String>,
    },

    // ── Publication mode subcommands ──────────────────────────────────────────

    /// Publish a protected flight (sanitized public artifact + raw companion stored locally).
    #[command(name = "announce-protected")]
    AnnounceProtected {
        /// Path to the IGC file to publish.
        file: PathBuf,

        #[arg(long, default_value = "0")]
        linger: u64,
    },

    /// Publish a private flight (raw artifact announced, no public sanitized copy).
    #[command(name = "announce-private")]
    AnnouncePrivate {
        /// Path to the IGC file to publish.
        file: PathBuf,

        #[arg(long, default_value = "0")]
        linger: u64,
    },

    // ── Governance subcommands ────────────────────────────────────────────────

    /// Submit an ownership claim for a flight on the governance topic.
    #[command(name = "governance-claim")]
    GovernanceClaim {
        /// 64-char BLAKE3 hex hash of the flight.
        igc_hash: String,
    },

    /// Submit a publication-mode change record for a flight you own.
    #[command(name = "governance-mode-change")]
    GovernanceModeChange {
        /// 64-char BLAKE3 hex hash of the flight.
        igc_hash: String,

        /// New publication mode: public | protected | private.
        mode: String,

        /// Required when mode is "protected": 64-char BLAKE3 hex of the sanitized artifact.
        #[arg(long)]
        protected_hash: Option<String>,

        /// Optional: record_id of the mode record this supersedes.
        #[arg(long)]
        supersedes: Option<String>,
    },

    /// Submit a deletion request for a flight you own on the governance topic.
    #[command(name = "governance-delete")]
    GovernanceDelete {
        /// 64-char BLAKE3 hex hash of the flight.
        igc_hash: String,
    },

    /// Print local governance state for a flight (offline, no node required).
    #[command(name = "governance-flight-status")]
    GovernanceFlightStatus {
        /// 64-char BLAKE3 hex hash of the flight.
        igc_hash: String,
    },

    // ── Peer sync subcommands ─────────────────────────────────────────────────

    /// Pull pilot-auth-did governance records for all registered pilots from a peer node.
    #[command(name = "governance-sync-peer")]
    GovernanceSyncPeer {
        /// Hex-encoded Ed25519 public key of the peer node.
        peer_node_id: String,
    },
}

// ── Entry point ───────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("igc_net=info".parse()?),
        )
        .with_target(false)
        .init();

    let cli = Cli::parse();
    let data_dir = resolve_data_dir(cli.data_dir)?;

    match cli.command {
        Command::Announce { file, linger } => cmd_announce(data_dir, file, linger).await,
        Command::RunIndex {
            policy,
            bootstrap,
            peer_addr,
        } => cmd_runindex(data_dir, policy, bootstrap, peer_addr).await,
        Command::Fetch { igc_hash, out } => cmd_fetch(data_dir, igc_hash, out).await,
        Command::Inspect { file } => cmd_inspect(file),
        Command::List => cmd_list(data_dir),
        Command::PilotAuthStatus => cmd_pilot_auth_status(data_dir),
        Command::DidKeyInspect { did } => cmd_did_key_inspect(did),
        Command::PilotAuthIssueInitial { created_at } => {
            cmd_pilot_auth_issue_initial(data_dir, created_at)
        }
        Command::PilotAuthRotate { created_at } => cmd_pilot_auth_rotate(data_dir, created_at),
        Command::PilotAuthRecordInspect { file } => cmd_pilot_auth_record_inspect(file),
        Command::PilotAuthRecordVerify { file } => cmd_pilot_auth_record_verify(file),
        Command::PilotProfileIssue {
            jti,
            name,
            country,
            audience,
            expires_in_seconds,
            out,
        } => cmd_pilot_profile_issue(
            data_dir,
            PilotProfileIssueOptions {
                jti,
                name,
                country,
                audience,
                expires_in_seconds,
                out,
            },
        ),
        Command::PilotProfileVerify {
            input,
            expected_audience,
        } => cmd_pilot_profile_verify(data_dir, input, expected_audience),
        Command::AnnounceProtected { file, linger } => {
            cmd_announce_protected(data_dir, file, linger).await
        }
        Command::AnnouncePrivate { file, linger } => {
            cmd_announce_private(data_dir, file, linger).await
        }
        Command::GovernanceClaim { igc_hash } => cmd_governance_claim(data_dir, igc_hash).await,
        Command::GovernanceModeChange {
            igc_hash,
            mode,
            protected_hash,
            supersedes,
        } => cmd_governance_mode_change(data_dir, igc_hash, mode, protected_hash, supersedes).await,
        Command::GovernanceDelete { igc_hash } => {
            cmd_governance_delete(data_dir, igc_hash).await
        }
        Command::GovernanceFlightStatus { igc_hash } => {
            cmd_governance_flight_status(data_dir, igc_hash)
        }
        Command::GovernanceSyncPeer { peer_node_id } => {
            cmd_governance_sync_peer(data_dir, peer_node_id).await
        }
    }
}

// ── Subcommand implementations ────────────────────────────────────────────────

/// `igc-net announce <file.igc> [--linger <secs>]`
async fn cmd_announce(data_dir: PathBuf, file: PathBuf, linger: u64) -> Result<()> {
    let filename = file
        .file_name()
        .and_then(|n| n.to_str())
        .map(str::to_string);
    let bytes = std::fs::read(&file).with_context(|| format!("cannot read {}", file.display()))?;

    let node = IgcIrohNode::start(&data_dir).await?;

    // Print loopback addr early so test tooling can bootstrap indexers before
    // the announcement is broadcast.
    eprintln!("node_addr: {}", node.loopback_addr_str()?);

    // When --linger is set, pause before publishing so bootstrapped peers have
    // time to join the gossip swarm and receive the announcement.
    if linger > 0 {
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }

    let result = publish(&node, bytes, filename.as_deref()).await?;

    println!("igc_hash:    {}", result.igc_hash);
    println!("meta_hash:   {}", result.meta_hash);
    println!("igc_ticket:  {}", result.igc_ticket);
    println!("meta_ticket: {}", result.meta_ticket);

    if linger > 0 {
        tokio::time::sleep(std::time::Duration::from_secs(linger)).await;
    }

    node.close().await;
    Ok(())
}

/// `igc-net runindex [--policy ...] [--bootstrap <node_id>,...] [--peer-addr <addr>]`
async fn cmd_runindex(
    data_dir: PathBuf,
    policy_str: String,
    bootstrap_strs: Vec<String>,
    peer_addrs: Vec<String>,
) -> Result<()> {
    let policy = parse_policy(&policy_str)?;

    let bootstrap_keys: Vec<iroh::PublicKey> = bootstrap_strs
        .iter()
        .filter(|s| !s.is_empty())
        .map(|s| s.parse().context(format!("invalid bootstrap node ID: {s}")))
        .collect::<Result<_>>()?;

    let node = IgcIrohNode::start(&data_dir).await?;

    // Pre-populate address book for direct loopback connections (no relay needed).
    for addr_str in &peer_addrs {
        let ep_addr = parse_endpoint_addr(addr_str)?;
        node.add_peer_addr(ep_addr);
    }

    eprintln!("Running indexer — node_id: {}", node.node_id());
    eprintln!("node_addr: {}", node.loopback_addr_str()?);
    eprintln!("Announce topic: {}", hex::encode(announce_topic_id()));
    if !bootstrap_keys.is_empty() {
        eprintln!("Bootstrap peers: {}", bootstrap_keys.len());
    }
    eprintln!("Press Ctrl-C to stop.");

    tokio::select! {
        res = run_indexer(&node, IndexerConfig::simple(policy, bootstrap_keys)) => {
            res?;
        }
        _ = tokio::signal::ctrl_c() => {
            eprintln!("shutting down");
        }
    }
    node.close().await;
    Ok(())
}

/// `igc-net fetch <igc_hash> [--out <file>]`
async fn cmd_fetch(data_dir: PathBuf, igc_hash: String, out: Option<PathBuf>) -> Result<()> {
    anyhow::ensure!(igc_hash.len() == 64, "igc_hash must be 64 hex chars");

    let store = FlatFileStore::open(&data_dir);
    store.init().await?;

    if let Some(bytes) = store.get(&igc_hash).await? {
        let out_path = out.unwrap_or_else(|| PathBuf::from(format!("{igc_hash}.igc")));
        std::fs::write(&out_path, &bytes)
            .with_context(|| format!("cannot write {}", out_path.display()))?;
        println!("Written {} bytes to {}", bytes.len(), out_path.display());
    } else {
        anyhow::bail!(
            "Flight {igc_hash} not in local store.\n\
             Hint: run `igc-net runindex` to index remote flights."
        );
    }
    Ok(())
}

/// `igc-net inspect <file>`
fn cmd_inspect(file: PathBuf) -> Result<()> {
    let bytes = std::fs::read(&file).with_context(|| format!("cannot read {}", file.display()))?;

    let ext = file
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();

    if ext == "json" {
        let meta: FlightMetadata =
            serde_json::from_slice(&bytes).context("not a valid metadata JSON blob")?;
        meta.validate().context("metadata validation failed")?;
        println!("{}", serde_json::to_string_pretty(&meta)?);
    } else {
        let igc_hash = Blake3Hex::from_hash(blake3::hash(&bytes));
        println!("igc_hash (BLAKE3): {igc_hash}");

        let meta = FlightMetadata::from_igc_bytes(&bytes, igc_hash, None, None);
        println!("{}", serde_json::to_string_pretty(&meta)?);
    }
    Ok(())
}

/// `igc-net list`
fn cmd_list(data_dir: PathBuf) -> Result<()> {
    let store = FlatFileStore::open(&data_dir);

    let mut count = 0usize;
    for record in store.iter_index()? {
        let r = record?;
        println!("{}\t{}\t{}", r.igc_hash, r.meta_hash, r.recorded_at);
        count += 1;
    }
    eprintln!("{count} flight(s) in index");
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PilotProfileIssueOptions {
    jti: String,
    name: Option<String>,
    country: Option<String>,
    audience: Option<String>,
    expires_in_seconds: Option<u64>,
    out: Option<PathBuf>,
}

fn cmd_did_key_inspect(did: String) -> Result<()> {
    println!(
        "{}",
        serde_json::to_string_pretty(&did_key_inspect_json(&did)?)?
    );
    Ok(())
}

fn cmd_pilot_auth_status(data_dir: PathBuf) -> Result<()> {
    println!(
        "{}",
        serde_json::to_string_pretty(&pilot_auth_status_json(&data_dir)?)?
    );
    Ok(())
}

fn pilot_auth_status_json(data_dir: &std::path::Path) -> Result<serde_json::Value> {
    let node_secret_key = load_or_generate_node_secret_key(&data_dir)?;
    let (pilot_keys, pilot_id, identity) =
        load_single_registered_pilot(data_dir, &node_secret_key)?;
    let governance = GovernanceStore::for_data_dir(&data_dir);
    governance.init()?;

    let archived_pilot_auth_dids = pilot_keys.archived_pilot_auth_dids()?;
    let public_identity = identity.export_public_identity();
    let governance_state = governance.resolve_pilot_auth_did_state(&pilot_id)?;

    Ok(serde_json::json!({
        "pilot_id": pilot_id,
        "archived_pilot_auth_dids": archived_pilot_auth_dids,
        "public_identity": public_identity,
        "governance_state": {
            "status": pilot_auth_state_status_label(governance_state.status()),
            "authoritative": governance_state.authoritative,
            "tentative_record_ids": governance_state.tentative_record_ids,
        },
    }))
}

fn cmd_pilot_auth_issue_initial(data_dir: PathBuf, created_at: Option<String>) -> Result<()> {
    let node_secret_key = load_or_generate_node_secret_key(&data_dir)?;
    let (_pilot_keys, _pilot_id, identity) =
        load_single_registered_pilot(&data_dir, &node_secret_key)?;
    let governance = GovernanceStore::for_data_dir(&data_dir);
    governance.init()?;

    let record = issue_initial_pilot_auth_did_record(
        &governance,
        &identity,
        created_at.unwrap_or_else(canonical_utc_now),
    )?;
    governance.persist_pilot_auth_did_record(&record)?;
    println!("{}", serde_json::to_string_pretty(&record)?);
    Ok(())
}

fn cmd_pilot_auth_record_inspect(file: PathBuf) -> Result<()> {
    let record = load_pilot_auth_record(&file)?;
    println!("{}", serde_json::to_string_pretty(&record)?);
    Ok(())
}

fn cmd_pilot_auth_record_verify(file: PathBuf) -> Result<()> {
    let record = load_pilot_auth_record(&file)?;
    record.validate()?;
    let output = serde_json::json!({
        "status": "valid",
        "record": record,
    });
    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

fn cmd_pilot_profile_issue(data_dir: PathBuf, options: PilotProfileIssueOptions) -> Result<()> {
    let issued = issue_pilot_profile_jwt(&data_dir, &options, &SystemClock)?;
    if let Some(path) = &options.out {
        std::fs::write(path, issued.compact_jwt.as_bytes())
            .with_context(|| format!("cannot write {}", path.display()))?;
    }
    println!("{}", serde_json::to_string_pretty(&issued.json_output())?);
    Ok(())
}

fn cmd_pilot_profile_verify(
    data_dir: PathBuf,
    input: String,
    expected_audience: Option<String>,
) -> Result<()> {
    let governance = GovernanceStore::for_data_dir(&data_dir);
    governance.init()?;
    let compact = load_text_input(&input)?;
    let mut verifier = PilotProfileCredentialVerifier::new(&governance, &SystemClock);
    if let Some(expected) = expected_audience.as_deref() {
        verifier = verifier.with_expected_audience(expected);
    }
    let verification = verifier.verify(compact.trim());
    println!(
        "{}",
        serde_json::to_string_pretty(&pilot_profile_verification_json(verification))?
    );
    Ok(())
}

fn cmd_pilot_auth_rotate(data_dir: PathBuf, created_at: Option<String>) -> Result<()> {
    let node_secret_key = load_or_generate_node_secret_key(&data_dir)?;
    let (pilot_keys, _pilot_id, current_identity) =
        load_single_registered_pilot(&data_dir, &node_secret_key)?;
    let governance = GovernanceStore::for_data_dir(&data_dir);
    governance.init()?;

    let next_active_pilot_auth_secret_key =
        pilot_keys.generate_next_active_pilot_auth_secret_key(&node_secret_key)?;
    let record = rotate_pilot_auth_did_record(
        &governance,
        &current_identity,
        &next_active_pilot_auth_secret_key,
        created_at.unwrap_or_else(canonical_utc_now),
    )?;
    pilot_keys.replace_active_pilot_auth(&node_secret_key, &next_active_pilot_auth_secret_key)?;
    if let Err(persist_err) = governance.persist_pilot_auth_did_record(&record) {
        match pilot_keys.replace_active_pilot_auth(
            &node_secret_key,
            &current_identity.active_pilot_auth_secret_key(),
        ) {
            Ok(_) => return Err(persist_err.into()),
            Err(rollback_err) => {
                anyhow::bail!(
                    "rotation governance persist failed after key replacement ({persist_err}); rollback also failed ({rollback_err})"
                );
            }
        }
    }
    println!("{}", serde_json::to_string_pretty(&record)?);
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct IssuedPilotProfileCredential {
    compact_jwt: String,
    header: serde_json::Value,
    claims: serde_json::Value,
    written_to: Option<PathBuf>,
}

impl IssuedPilotProfileCredential {
    fn json_output(&self) -> serde_json::Value {
        serde_json::json!({
            "compact_jwt": self.compact_jwt,
            "header": self.header,
            "claims": self.claims,
            "written_to": self.written_to.as_ref().map(|path| path.display().to_string()),
        })
    }
}

fn did_key_inspect_json(did: &str) -> Result<serde_json::Value> {
    let did = DidKey::parse(did)?;
    Ok(serde_json::json!({
        "did": did,
        "method_specific_id": did.method_specific_id(),
        "kid": did.key_id(),
        "public_key_hex": did.public_key().to_string(),
    }))
}

fn load_pilot_auth_record(file: &std::path::Path) -> Result<PilotAuthDidRecord> {
    let bytes = std::fs::read(file).with_context(|| format!("cannot read {}", file.display()))?;
    serde_json::from_slice(&bytes).context("not a valid pilot-auth-did record JSON file")
}

fn issue_pilot_profile_jwt<C: igc_net::Clock>(
    data_dir: &std::path::Path,
    options: &PilotProfileIssueOptions,
    clock: &C,
) -> Result<IssuedPilotProfileCredential> {
    let node_secret_key = load_or_generate_node_secret_key(data_dir)?;
    let (_pilot_keys, _pilot_id, identity) =
        load_single_registered_pilot(data_dir, &node_secret_key)?;
    let governance = GovernanceStore::for_data_dir(data_dir);
    governance.init()?;

    let request = PilotProfileCredentialRequest {
        subject: PilotProfileCredentialSubjectDraft {
            name: options.name.clone(),
            country: options.country.clone(),
            additional_fields: BTreeMap::new(),
        },
        jti: options.jti.clone(),
        audience: options.audience.clone(),
        expires_in_seconds: options.expires_in_seconds,
    };
    let jwt = issue_pilot_profile_credential(&governance, &identity, request, clock)?;

    Ok(IssuedPilotProfileCredential {
        compact_jwt: jwt.compact().to_string(),
        header: serde_json::to_value(jwt.header())?,
        claims: serde_json::to_value(jwt.claims())?,
        written_to: options.out.clone(),
    })
}

fn pilot_profile_verification_json(
    verification: PilotProfileCredentialVerification,
) -> serde_json::Value {
    match verification {
        PilotProfileCredentialVerification::ValidAuthoritative(jwt) => serde_json::json!({
            "status": "valid_authoritative",
            "header": jwt.header(),
            "claims": jwt.claims(),
        }),
        PilotProfileCredentialVerification::Invalid { credential, error } => serde_json::json!({
            "status": "invalid",
            "error": error.to_string(),
            "header": credential.as_ref().map(|jwt| jwt.header()),
            "claims": credential.as_ref().map(|jwt| jwt.claims()),
        }),
        PilotProfileCredentialVerification::UnverifiableGovernanceIncomplete {
            credential,
            pilot_id,
        } => serde_json::json!({
            "status": "unverifiable_governance_incomplete",
            "pilot_id": pilot_id,
            "header": credential.header(),
            "claims": credential.claims(),
        }),
        PilotProfileCredentialVerification::UnverifiableGovernanceUnavailable {
            credential,
            pilot_id,
        } => serde_json::json!({
            "status": "unverifiable_governance_unavailable",
            "pilot_id": pilot_id,
            "header": credential.header(),
            "claims": credential.claims(),
        }),
        PilotProfileCredentialVerification::UnverifiableDidWebResolution { issuer, error } => {
            serde_json::json!({
                "status": "unverifiable_did_web_resolution",
                "issuer": issuer,
                "error": error.to_string(),
            })
        }
    }
}

fn load_text_input(input: &str) -> Result<String> {
    let path = PathBuf::from(input);
    if path.is_file() {
        return std::fs::read_to_string(&path)
            .with_context(|| format!("cannot read {}", path.display()));
    }
    Ok(input.to_string())
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn load_single_registered_pilot(
    data_dir: &std::path::Path,
    node_secret_key: &iroh::SecretKey,
) -> Result<(igc_net::PilotKeyStore, PilotId, PilotIdentity)> {
    let multi_pilot_keys = MultiPilotKeyStore::for_data_dir(data_dir);
    multi_pilot_keys.init()?;
    let pilots = multi_pilot_keys.list_pilots(node_secret_key)?;
    let pilot = match pilots.as_slice() {
        [] => {
            anyhow::bail!("no registered pilot found; register a pilot before using this command")
        }
        [pilot] => pilot,
        _ => anyhow::bail!(
            "multiple registered pilots found; this CLI command requires an explicit pilot selection flow"
        ),
    };
    let pilot_id = pilot.pilot_id.clone();
    let identity = multi_pilot_keys
        .load_pilot(&pilot_id, node_secret_key)?
        .context("registered pilot identity is missing")?;
    Ok((multi_pilot_keys.pilot_store(&pilot_id), pilot_id, identity))
}

fn resolve_data_dir(opt: Option<PathBuf>) -> Result<PathBuf> {
    // Check env var first
    if let Some(p) = opt {
        return Ok(p);
    }
    if let Some(val) = std::env::var_os("IGC_NET_DATA_DIR") {
        return Ok(PathBuf::from(val));
    }
    let home = std::env::var_os("HOME")
        .or_else(|| std::env::var_os("XDG_DATA_HOME"))
        .map(PathBuf::from)
        .context("cannot determine home directory; set --data-dir or IGC_NET_DATA_DIR")?;
    Ok(home.join(".igc-net"))
}

fn canonical_utc_now() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

fn load_or_generate_node_secret_key(data_dir: &std::path::Path) -> Result<iroh::SecretKey> {
    std::fs::create_dir_all(data_dir)
        .with_context(|| format!("cannot create {}", data_dir.display()))?;
    let store = FlatFileStore::open(data_dir);
    match store.load_key_bytes()? {
        Some(bytes) => Ok(iroh::SecretKey::from_bytes(&bytes)),
        None => {
            let mut rng = rand::rng();
            let secret_key = iroh::SecretKey::generate(&mut rng);
            let bytes = secret_key.to_bytes();
            store.save_key_bytes(&bytes)?;
            Ok(secret_key)
        }
    }
}

fn pilot_auth_state_status_label(status: PilotAuthDidStateStatus) -> &'static str {
    match status {
        PilotAuthDidStateStatus::Absent => "absent",
        PilotAuthDidStateStatus::Tentative => "tentative",
        PilotAuthDidStateStatus::Authoritative => "authoritative",
    }
}

fn parse_endpoint_addr(s: &str) -> Result<iroh::EndpointAddr> {
    let (node_id_str, socket_str) = s
        .split_once('@')
        .context("--peer-addr must be in format <node_id_hex>@<ip>:<port>")?;
    let node_id: iroh::PublicKey = node_id_str
        .parse()
        .context("invalid node_id in --peer-addr")?;
    let socket_addr: std::net::SocketAddr = socket_str
        .parse()
        .context("invalid socket address in --peer-addr")?;
    Ok(iroh::EndpointAddr::new(node_id).with_ip_addr(socket_addr))
}

fn parse_publication_mode(s: &str) -> Result<PublicationMode> {
    match s {
        "public" => Ok(PublicationMode::Public),
        "protected" => Ok(PublicationMode::Protected),
        "private" => Ok(PublicationMode::Private),
        _ => anyhow::bail!("unknown mode {s:?}; use public, protected, or private"),
    }
}

/// `igc-net announce-protected <file.igc> [--linger <secs>]`
async fn cmd_announce_protected(data_dir: PathBuf, file: PathBuf, linger: u64) -> Result<()> {
    let bytes = std::fs::read(&file).with_context(|| format!("cannot read {}", file.display()))?;
    let node = IgcIrohNode::start(&data_dir).await?;
    eprintln!("node_addr: {}", node.loopback_addr_str()?);
    if linger > 0 {
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
    let result = publish_protected(&node, bytes).await?;
    println!("raw_igc_hash:         {}", result.raw_igc_hash);
    println!("protected_hash:       {}", result.protected_hash);
    println!("protected_ticket:     {}", result.protected_ticket);
    println!("raw_companion_ticket: {}", result.raw_companion_ticket);
    println!("g_record_present:     {}", result.g_record_present);
    if linger > 0 {
        tokio::time::sleep(std::time::Duration::from_secs(linger)).await;
    }
    node.close().await;
    Ok(())
}

/// `igc-net announce-private <file.igc> [--linger <secs>]`
async fn cmd_announce_private(data_dir: PathBuf, file: PathBuf, linger: u64) -> Result<()> {
    let bytes = std::fs::read(&file).with_context(|| format!("cannot read {}", file.display()))?;
    let node = IgcIrohNode::start(&data_dir).await?;
    eprintln!("node_addr: {}", node.loopback_addr_str()?);
    if linger > 0 {
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
    let result = publish_private(&node, bytes).await?;
    println!("raw_igc_hash:     {}", result.raw_igc_hash);
    println!("raw_igc_ticket:   {}", result.raw_igc_ticket);
    println!("g_record_present: {}", result.g_record_present);
    if linger > 0 {
        tokio::time::sleep(std::time::Duration::from_secs(linger)).await;
    }
    node.close().await;
    Ok(())
}

/// `igc-net governance-claim <igc_hash>`
async fn cmd_governance_claim(data_dir: PathBuf, igc_hash_str: String) -> Result<()> {
    let igc_hash = igc_hash_str
        .parse::<Blake3Hex>()
        .with_context(|| format!("invalid igc_hash: {igc_hash_str:?}"))?;
    let node_secret_key = load_or_generate_node_secret_key(&data_dir)?;
    let (_, _, identity) = load_single_registered_pilot(&data_dir, &node_secret_key)?;
    let node = IgcIrohNode::start(&data_dir).await?;
    let record = OwnerClaimRecord::issue(
        &identity.pilot_id_secret_key(),
        igc_hash,
        canonical_utc_now(),
        Vec::new(),
    )?;
    node.governance_store().persist_owner_claim_record(&record)?;
    node.broadcast_governance_record(&record).await?;
    println!("{}", serde_json::to_string_pretty(&record)?);
    node.close().await;
    Ok(())
}

/// `igc-net governance-mode-change <igc_hash> <mode> [--protected-hash <hex>] [--supersedes <id>]`
async fn cmd_governance_mode_change(
    data_dir: PathBuf,
    igc_hash_str: String,
    mode_str: String,
    protected_hash_str: Option<String>,
    supersedes_str: Option<String>,
) -> Result<()> {
    let igc_hash = igc_hash_str
        .parse::<Blake3Hex>()
        .with_context(|| format!("invalid igc_hash: {igc_hash_str:?}"))?;
    let mode = parse_publication_mode(&mode_str)?;
    let protected_hash = protected_hash_str
        .map(|s| {
            s.parse::<Blake3Hex>()
                .with_context(|| format!("invalid --protected-hash: {s:?}"))
        })
        .transpose()?;
    let supersedes = supersedes_str
        .map(|s| {
            s.parse::<Blake3Hex>()
                .with_context(|| format!("invalid --supersedes: {s:?}"))
        })
        .transpose()?;

    if matches!(mode, PublicationMode::Protected) && protected_hash.is_none() {
        anyhow::bail!("--protected-hash <hex> is required when mode is \"protected\"");
    }

    let node_secret_key = load_or_generate_node_secret_key(&data_dir)?;
    let (_, _, identity) = load_single_registered_pilot(&data_dir, &node_secret_key)?;
    let node = IgcIrohNode::start(&data_dir).await?;
    let record = PublicationModeRecord::issue(
        &identity.pilot_id_secret_key(),
        igc_hash,
        mode,
        protected_hash,
        supersedes,
        canonical_utc_now(),
    )?;
    node.governance_store()
        .persist_publication_mode_record(&record)?;
    node.broadcast_governance_record(&record).await?;
    println!("{}", serde_json::to_string_pretty(&record)?);
    node.close().await;
    Ok(())
}

/// `igc-net governance-delete <igc_hash>`
async fn cmd_governance_delete(data_dir: PathBuf, igc_hash_str: String) -> Result<()> {
    let igc_hash = igc_hash_str
        .parse::<Blake3Hex>()
        .with_context(|| format!("invalid igc_hash: {igc_hash_str:?}"))?;
    let node_secret_key = load_or_generate_node_secret_key(&data_dir)?;
    let (_, _, identity) = load_single_registered_pilot(&data_dir, &node_secret_key)?;
    let node = IgcIrohNode::start(&data_dir).await?;
    let record = DeletionRequestRecord::issue(
        &identity.pilot_id_secret_key(),
        igc_hash,
        canonical_utc_now(),
    )?;
    node.governance_store()
        .persist_deletion_request_record(&record)?;
    node.broadcast_governance_record(&record).await?;
    println!("{}", serde_json::to_string_pretty(&record)?);
    node.close().await;
    Ok(())
}

/// `igc-net governance-flight-status <igc_hash>`
fn cmd_governance_flight_status(data_dir: PathBuf, igc_hash_str: String) -> Result<()> {
    let igc_hash = igc_hash_str
        .parse::<Blake3Hex>()
        .with_context(|| format!("invalid igc_hash: {igc_hash_str:?}"))?;
    let governance = GovernanceStore::for_data_dir(&data_dir);
    governance.init()?;
    let output = match governance.resolve_flight_governance_state(&igc_hash)? {
        Some(state) => serde_json::to_value(&state)?,
        None => serde_json::json!({ "status": "unknown" }),
    };
    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

/// `igc-net governance-sync-peer <peer_node_id>`
async fn cmd_governance_sync_peer(data_dir: PathBuf, peer_node_id_str: String) -> Result<()> {
    let peer: iroh::PublicKey = peer_node_id_str
        .parse()
        .with_context(|| format!("invalid peer node ID: {peer_node_id_str:?}"))?;
    let node_secret_key = load_or_generate_node_secret_key(&data_dir)?;
    let multi_pilot_keys = MultiPilotKeyStore::for_data_dir(&data_dir);
    multi_pilot_keys.init()?;
    let pilots = multi_pilot_keys.list_pilots(&node_secret_key)?;
    anyhow::ensure!(
        !pilots.is_empty(),
        "no registered pilots; register a pilot before syncing"
    );
    let node = IgcIrohNode::start(&data_dir).await?;
    let mut total_synced = 0usize;
    for pilot_info in &pilots {
        let synced = node
            .sync_pilot_auth_did_from_peer(peer, &pilot_info.pilot_id)
            .await
            .with_context(|| format!("sync failed for pilot {}", pilot_info.pilot_id))?;
        eprintln!(
            "pilot {}: {} record(s) synced",
            pilot_info.pilot_id, synced
        );
        total_synced += synced;
    }
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "peer": peer_node_id_str,
            "pilots_synced": pilots.len(),
            "total_records_synced": total_synced,
        }))?
    );
    node.close().await;
    Ok(())
}

fn parse_policy(s: &str) -> Result<FetchPolicy> {
    match s {
        "index-only" => Ok(FetchPolicy::IndexOnly),
        "eager" => Ok(FetchPolicy::Eager),
        _ if s.starts_with("geo:") => {
            let parts: Vec<&str> = s[4..].split(',').collect();
            anyhow::ensure!(
                parts.len() == 4,
                "geo policy format: geo:<min_lat>,<max_lat>,<min_lon>,<max_lon>"
            );
            let p = |v: &str| v.trim().parse::<f64>().context("expected float");
            Ok(FetchPolicy::GeoFiltered {
                min_lat: p(parts[0])?,
                max_lat: p(parts[1])?,
                min_lon: p(parts[2])?,
                max_lon: p(parts[3])?,
            })
        }
        _ => anyhow::bail!("unknown policy {s:?}; use index-only, eager, or geo:<bbox>"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use igc_net::{
        FixedClock, HighTrustVerificationError, PilotAuthDidRecord, PilotProfileCredentialError,
        PilotProfileCredentialVerification,
    };

    fn deterministic_secret_key(byte: u8) -> iroh::SecretKey {
        iroh::SecretKey::from_bytes(&[byte; 32])
    }

    #[test]
    fn did_key_inspect_reports_public_key_and_kid() {
        let secret_key = deterministic_secret_key(7);
        let did = DidKey::from_public_key(secret_key.public());

        let inspected = did_key_inspect_json(did.as_str()).unwrap();
        assert_eq!(inspected["did"], serde_json::json!(did.as_str()));
        assert_eq!(inspected["kid"], serde_json::json!(did.key_id()));
        assert_eq!(
            inspected["public_key_hex"],
            serde_json::json!(secret_key.public().to_string())
        );
    }

    #[test]
    fn pilot_auth_record_verify_accepts_valid_record_file() {
        let dir = tempfile::tempdir().unwrap();
        let record = PilotAuthDidRecord::issue(
            &deterministic_secret_key(21),
            DidKey::from_public_key(deterministic_secret_key(22).public()),
            None,
            "2026-05-01T09:14:00Z",
        )
        .unwrap();
        let path = dir.path().join("record.json");
        std::fs::write(&path, serde_json::to_vec_pretty(&record).unwrap()).unwrap();

        let loaded = load_pilot_auth_record(&path).unwrap();
        loaded.validate().unwrap();
        assert_eq!(loaded.record_id, record.record_id);
    }

    #[test]
    fn pilot_profile_issue_and_verify_flow_detects_rotation_staleness() {
        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path();
        let node_secret_key = load_or_generate_node_secret_key(data_dir).unwrap();
        let multi_pilot_keys = MultiPilotKeyStore::for_data_dir(data_dir);
        multi_pilot_keys.init().unwrap();
        let governance = GovernanceStore::for_data_dir(data_dir);
        governance.init().unwrap();

        let identity = multi_pilot_keys
            .generate_pilot("Alice Example", Some("NO".to_string()), &node_secret_key)
            .unwrap();
        let pilot_keys = multi_pilot_keys.pilot_store(&identity.pilot_id());
        let initial =
            issue_initial_pilot_auth_did_record(&governance, &identity, "2026-05-01T09:14:00Z")
                .unwrap();
        governance.persist_pilot_auth_did_record(&initial).unwrap();

        let issued = issue_pilot_profile_jwt(
            data_dir,
            &PilotProfileIssueOptions {
                jti: "urn:uuid:test-phase-9".to_string(),
                name: Some("Alice Example".to_string()),
                country: Some("NO".to_string()),
                audience: Some("portal.example".to_string()),
                expires_in_seconds: Some(600),
                out: None,
            },
            &FixedClock::new(1_745_572_800),
        )
        .unwrap();

        let valid =
            PilotProfileCredentialVerifier::new(&governance, &FixedClock::new(1_745_572_800))
                .with_expected_audience("portal.example")
                .verify(&issued.compact_jwt);
        assert!(matches!(
            valid,
            PilotProfileCredentialVerification::ValidAuthoritative(_)
        ));

        let next = pilot_keys
            .generate_next_active_pilot_auth_secret_key(&node_secret_key)
            .unwrap();
        let rotated =
            rotate_pilot_auth_did_record(&governance, &identity, &next, "2026-05-01T10:14:00Z")
                .unwrap();
        governance.persist_pilot_auth_did_record(&rotated).unwrap();

        let stale =
            PilotProfileCredentialVerifier::new(&governance, &FixedClock::new(1_745_572_800))
                .with_expected_audience("portal.example")
                .verify(&issued.compact_jwt);
        assert!(matches!(
            stale,
            PilotProfileCredentialVerification::Invalid {
                error: HighTrustVerificationError::Credential(
                    PilotProfileCredentialError::HistoricalCredential { .. }
                ),
                ..
            }
        ));
    }

    #[test]
    fn pilot_auth_status_reports_archived_pilot_auth_dids() {
        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path();
        let node_secret_key = load_or_generate_node_secret_key(data_dir).unwrap();
        let multi_pilot_keys = MultiPilotKeyStore::for_data_dir(data_dir);
        multi_pilot_keys.init().unwrap();
        let governance = GovernanceStore::for_data_dir(data_dir);
        governance.init().unwrap();

        let identity = multi_pilot_keys
            .generate_pilot("Alice Example", Some("NO".to_string()), &node_secret_key)
            .unwrap();
        let pilot_keys = multi_pilot_keys.pilot_store(&identity.pilot_id());
        let initial =
            issue_initial_pilot_auth_did_record(&governance, &identity, "2026-05-01T09:14:00Z")
                .unwrap();
        governance.persist_pilot_auth_did_record(&initial).unwrap();

        let next = pilot_keys
            .generate_next_active_pilot_auth_secret_key(&node_secret_key)
            .unwrap();
        let rotated =
            rotate_pilot_auth_did_record(&governance, &identity, &next, "2026-05-01T10:14:00Z")
                .unwrap();
        pilot_keys
            .replace_active_pilot_auth(&node_secret_key, &next)
            .unwrap();
        governance.persist_pilot_auth_did_record(&rotated).unwrap();

        let status = pilot_auth_status_json(data_dir).unwrap();
        assert_eq!(
            status["archived_pilot_auth_dids"],
            serde_json::json!([identity.active_pilot_auth_did().as_str()])
        );
    }
}
