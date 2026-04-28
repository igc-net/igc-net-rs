use std::collections::{BTreeMap, HashMap};

use crate::id::{Blake3Hex, PilotId};

use super::record::{PilotAuthDidRecord, PrivateAccessRotationRecord};
use super::state::{PilotAuthDidState, PrivateAccessRotationState};

#[derive(Debug, thiserror::Error)]
pub enum GovernanceSelectionError {
    #[error("record {record_id} does not belong to pilot_id {expected}")]
    MixedPilot {
        expected: PilotId,
        record_id: Blake3Hex,
    },
    #[error("duplicate record_id encountered during selection: {0}")]
    DuplicateRecordId(Blake3Hex),
}

trait SupersessionRecord: Clone {
    fn pilot_id(&self) -> &PilotId;
    fn record_id(&self) -> &Blake3Hex;
    fn supersedes(&self) -> &Option<Blake3Hex>;
}

impl SupersessionRecord for PilotAuthDidRecord {
    fn pilot_id(&self) -> &PilotId {
        &self.pilot_id
    }

    fn record_id(&self) -> &Blake3Hex {
        &self.record_id
    }

    fn supersedes(&self) -> &Option<Blake3Hex> {
        &self.supersedes
    }
}

impl SupersessionRecord for PrivateAccessRotationRecord {
    fn pilot_id(&self) -> &PilotId {
        &self.pilot_id
    }

    fn record_id(&self) -> &Blake3Hex {
        &self.record_id
    }

    fn supersedes(&self) -> &Option<Blake3Hex> {
        &self.supersedes
    }
}

pub fn select_pilot_auth_did_state(
    pilot_id: &PilotId,
    records: &[PilotAuthDidRecord],
) -> Result<PilotAuthDidState, GovernanceSelectionError> {
    if records.is_empty() {
        return Ok(PilotAuthDidState::absent(pilot_id.clone()));
    }

    let selected = select_supersession_tip(pilot_id, records)?;

    Ok(PilotAuthDidState {
        pilot_id: pilot_id.clone(),
        authoritative: selected.authoritative,
        tentative_record_ids: selected.tentative_record_ids,
    })
}

pub fn select_private_access_rotation_state(
    pilot_id: &PilotId,
    records: &[PrivateAccessRotationRecord],
) -> Result<PrivateAccessRotationState, GovernanceSelectionError> {
    if records.is_empty() {
        return Ok(PrivateAccessRotationState::absent(pilot_id.clone()));
    }

    let selected = select_supersession_tip(pilot_id, records)?;

    Ok(PrivateAccessRotationState {
        pilot_id: pilot_id.clone(),
        authoritative: selected.authoritative,
        tentative_record_ids: selected.tentative_record_ids,
    })
}

struct SelectedSupersessionTip<R> {
    authoritative: Option<R>,
    tentative_record_ids: Vec<Blake3Hex>,
}

fn select_supersession_tip<R>(
    pilot_id: &PilotId,
    records: &[R],
) -> Result<SelectedSupersessionTip<R>, GovernanceSelectionError>
where
    R: SupersessionRecord,
{
    let mut record_map = HashMap::with_capacity(records.len());
    for record in records {
        if record.pilot_id() != pilot_id {
            return Err(GovernanceSelectionError::MixedPilot {
                expected: pilot_id.clone(),
                record_id: record.record_id().clone(),
            });
        }
        if record_map
            .insert(record.record_id().clone(), record.clone())
            .is_some()
        {
            return Err(GovernanceSelectionError::DuplicateRecordId(
                record.record_id().clone(),
            ));
        }
    }

    let mut completeness = HashMap::<Blake3Hex, bool>::with_capacity(record_map.len());
    for record_id in record_map.keys() {
        compute_completeness(record_id, &record_map, &mut completeness);
    }

    let mut children = BTreeMap::<Option<Blake3Hex>, Vec<R>>::new();
    for record in record_map.values() {
        children
            .entry(record.supersedes().clone())
            .or_default()
            .push(record.clone());
    }
    for siblings in children.values_mut() {
        siblings.sort_by(|left, right| left.record_id().cmp(right.record_id()));
    }

    let authoritative = select_authoritative_tip(&children, &completeness);
    let mut tentative_record_ids = record_map
        .values()
        .filter(|record| {
            !completeness
                .get(record.record_id())
                .copied()
                .unwrap_or(false)
        })
        .map(|record| record.record_id().clone())
        .collect::<Vec<_>>();
    tentative_record_ids.sort();

    Ok(SelectedSupersessionTip {
        authoritative,
        tentative_record_ids,
    })
}

fn compute_completeness<R>(
    record_id: &Blake3Hex,
    records: &HashMap<Blake3Hex, R>,
    cache: &mut HashMap<Blake3Hex, bool>,
) -> bool
where
    R: SupersessionRecord,
{
    if let Some(result) = cache.get(record_id) {
        return *result;
    }

    let result = match records.get(record_id) {
        Some(record) => match record.supersedes() {
            None => true,
            Some(parent_id) => match records.get(parent_id) {
                Some(_) => compute_completeness(parent_id, records, cache),
                None => false,
            },
        },
        None => false,
    };
    cache.insert(record_id.clone(), result);
    result
}

fn select_authoritative_tip<R>(
    children: &BTreeMap<Option<Blake3Hex>, Vec<R>>,
    completeness: &HashMap<Blake3Hex, bool>,
) -> Option<R>
where
    R: SupersessionRecord,
{
    let mut current = children.get(&None).and_then(|roots| {
        roots
            .iter()
            .find(|record| {
                completeness
                    .get(record.record_id())
                    .copied()
                    .unwrap_or(false)
            })
            .cloned()
    });

    loop {
        let record = current?;
        let next = children
            .get(&Some(record.record_id().clone()))
            .and_then(|candidates| {
                candidates
                    .iter()
                    .find(|candidate| {
                        completeness
                            .get(candidate.record_id())
                            .copied()
                            .unwrap_or(false)
                    })
                    .cloned()
            });
        match next {
            Some(next_record) => current = Some(next_record),
            None => return Some(record),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::governance::record::PilotAuthDidRecord;
    use crate::identity::DidKey;

    use super::*;

    fn deterministic_secret_key(byte: u8) -> iroh::SecretKey {
        iroh::SecretKey::from_bytes(&[byte; 32])
    }

    #[test]
    fn rauth_06_selects_chain_tip_as_authority() {
        let root_secret = deterministic_secret_key(7);
        let initial_did = DidKey::from_public_key(deterministic_secret_key(8).public());
        let rotated_did = DidKey::from_public_key(deterministic_secret_key(9).public());

        let initial =
            PilotAuthDidRecord::issue(&root_secret, initial_did, None, "2026-05-01T09:14:00Z")
                .unwrap();
        let rotated = PilotAuthDidRecord::issue(
            &root_secret,
            rotated_did,
            Some(initial.record_id.clone()),
            "2026-05-01T09:15:00Z",
        )
        .unwrap();

        let state =
            select_pilot_auth_did_state(&initial.pilot_id, &[initial.clone(), rotated.clone()])
                .unwrap();
        assert_eq!(state.authoritative.unwrap().record_id, rotated.record_id);
        assert!(state.tentative_record_ids.is_empty());
    }

    #[test]
    fn rauth_07_lexicographically_smallest_concurrent_record_wins() {
        let root_secret = deterministic_secret_key(11);
        let initial = PilotAuthDidRecord::issue(
            &root_secret,
            DidKey::from_public_key(deterministic_secret_key(12).public()),
            None,
            "2026-05-01T09:14:00Z",
        )
        .unwrap();
        let contender_a = PilotAuthDidRecord::issue(
            &root_secret,
            DidKey::from_public_key(deterministic_secret_key(13).public()),
            Some(initial.record_id.clone()),
            "2026-05-01T09:15:00Z",
        )
        .unwrap();
        let contender_b = PilotAuthDidRecord::issue(
            &root_secret,
            DidKey::from_public_key(deterministic_secret_key(14).public()),
            Some(initial.record_id.clone()),
            "2026-05-01T09:16:00Z",
        )
        .unwrap();

        let expected_winner = if contender_a.record_id < contender_b.record_id {
            contender_a.record_id.clone()
        } else {
            contender_b.record_id.clone()
        };

        let pilot_id = initial.pilot_id.clone();
        let state =
            select_pilot_auth_did_state(&pilot_id, &[initial, contender_a, contender_b]).unwrap();
        assert_eq!(state.authoritative.unwrap().record_id, expected_winner);
    }

    #[test]
    fn rauth_08_unknown_ancestor_produces_tentative_state() {
        let root_secret = deterministic_secret_key(21);
        let record = PilotAuthDidRecord::issue(
            &root_secret,
            DidKey::from_public_key(deterministic_secret_key(22).public()),
            Some(Blake3Hex::parse("a".repeat(64)).unwrap()),
            "2026-05-01T09:14:00Z",
        )
        .unwrap();

        let state =
            select_pilot_auth_did_state(&record.pilot_id, std::slice::from_ref(&record)).unwrap();
        assert!(state.authoritative.is_none());
        assert_eq!(state.tentative_record_ids, vec![record.record_id.clone()]);
    }

    #[test]
    fn rauth_08_previous_authority_remains_when_new_record_is_incomplete() {
        let root_secret = deterministic_secret_key(31);
        let authoritative = PilotAuthDidRecord::issue(
            &root_secret,
            DidKey::from_public_key(deterministic_secret_key(32).public()),
            None,
            "2026-05-01T09:14:00Z",
        )
        .unwrap();
        let incomplete = PilotAuthDidRecord::issue(
            &root_secret,
            DidKey::from_public_key(deterministic_secret_key(33).public()),
            Some(Blake3Hex::parse("b".repeat(64)).unwrap()),
            "2026-05-01T09:15:00Z",
        )
        .unwrap();

        let state = select_pilot_auth_did_state(
            &authoritative.pilot_id,
            &[authoritative.clone(), incomplete.clone()],
        )
        .unwrap();
        assert_eq!(
            state.authoritative.unwrap().record_id,
            authoritative.record_id
        );
        assert_eq!(state.tentative_record_ids, vec![incomplete.record_id]);
    }
}
