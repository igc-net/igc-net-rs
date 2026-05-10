pub mod access;
pub(crate) mod artifact_announcement;
pub mod follow;
pub mod follow_store;
pub mod governance;
pub mod group;
pub mod group_store;
pub mod id;
pub mod identity;
pub mod igc;
pub mod indexer;
pub mod keys;
pub mod metadata;
pub mod node;
pub mod publish;
pub mod store;
pub mod topic;
pub(crate) mod util;
pub mod verify;

pub use access::{
    ArtifactClass, FetchProof, FetchProofError, GroupFetchProof, GroupFetchProofError, SeqNumStore,
    SeqNumStoreError, sign_fetch_proof, sign_group_fetch_proof, verify_fetch_proof,
    verify_group_fetch_proof,
};
pub use follow::{FollowRecord, FollowRecordError, UnfollowRecord};
pub use follow_store::{FollowStore, FollowStoreError};
pub use group::{
    GroupCreationRecord, GroupRecordError, GroupType, PrivateGroupMemberAddRecord,
    PrivateGroupMemberRemoveRecord, PublicGroupAcceptRecord, PublicGroupInviteRecord,
    PublicGroupLeaveRecord,
};
pub use group_store::{GroupMembership, GroupStore, GroupStoreError};
pub use governance::{
    ClaimApprovalRecord, ClaimChallengeRecord, ClaimResolutionOutcome, ClaimResolutionRecord,
    DeletionRequestRecord, FlightGovernanceRecordError, FlightGovernanceState,
    FlightGovernanceStatus, GovernanceLookup, GovernanceRecord, GovernanceRecordParseError,
    GovernanceSelectionError, GovernanceStore, GovernanceStoreError, IdentityRecoveryBasis,
    IdentityRecoveryRecord, OwnerClaimRecord, PilotAuthDidGossipAnnouncement, PilotAuthDidRecord,
    PilotAuthDidRecordError, PilotAuthDidState, PilotAuthDidStateStatus, PilotAuthDidSyncError,
    PilotAuthDidSyncRequest, PilotAuthDidSyncResponse, PilotAuthDidWorkflowError,
    PrivateAccessRotationRecord, PrivateAccessRotationRecordError, PrivateAccessRotationState,
    PrivateAccessRotationStateStatus, PublicationModeRecord, ResolverProfile, RosterUpdateAction,
    RosterUpdateRecord, issue_initial_pilot_auth_did_record, rotate_pilot_auth_did_record,
};
pub use id::{Blake3Hex, GroupId, IdentifierError, NodeIdHex, PilotId};
#[cfg(feature = "did-web")]
pub use identity::ReqwestDidWebResolver;
pub use identity::{
    Clock, DidKey, DidKeyError, DidWeb, DidWebError, DidWebResolutionError, DidWebResolver,
    FixedClock, JwtAudience, NoDidWebResolver, PilotProfileCredentialClaims,
    PilotProfileCredentialError, PilotProfileCredentialJoseHeader, PilotProfileCredentialJwt,
    PilotProfileCredentialRequest, PilotProfileCredentialSubject,
    PilotProfileCredentialSubjectDraft, PilotProfileCredentialVc, ResolvedDidWebVerificationMethod,
    SystemClock, issue_pilot_profile_credential, verify_pilot_profile_credential,
};
pub use igc::g_record_present;
pub use indexer::{FetchPolicy, IndexerConfig, IndexerError, RateLimitConfig, run_indexer};
pub use keys::{
    MultiPilotKeyStore, PilotCredentialFile, PilotCredentialStore, PilotIdentity, PilotKeyStore,
    PilotKeyStoreError, PilotKeyStoreStatus, PilotProfile, PilotPublicIdentity,
    PilotPublicIdentityWithProfile, PrivateAccessKeyStore,
};
pub use metadata::{BoundingBox, FlightMetadata, MetadataError};
pub use node::{IgcIrohNode, NodeError};
pub use publish::{
    PrivatePublishResult, ProtectedPublishResult, PublishError, PublishResult, publish,
    publish_private, publish_protected, sanitize_protected_igc,
};
pub use store::{
    ArtifactRegistryRecord, FlatFileStore, IndexRecord, IndexRecordSource, PublicationMode,
    StoreError,
};
pub use topic::{
    ANALYTICS_TOPIC_STR, ANNOUNCE_TOPIC_STR, GOVERNANCE_TOPIC_STR,
    PILOT_AUTH_DID_GOVERNANCE_TOPIC_STR, analytics_topic_id, announce_topic_id,
    governance_topic_id, pilot_auth_did_governance_topic_id,
};
pub use verify::{
    HighTrustVerificationError, PilotProfileCredentialVerification, PilotProfileCredentialVerifier,
    verify_pilot_profile_credential_high_trust,
};
