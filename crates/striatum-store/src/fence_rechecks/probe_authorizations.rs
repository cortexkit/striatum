use super::*;

impl AccountTx<'_> {
    /// Every owner holding a probe authorization, without joining admissions:
    /// a caller that holds the admission rows in memory decides which owners
    /// are expired unstarted admissions, then passes them to
    /// [`Self::release_admission_probes`].
    pub fn probe_authorization_owners(&self) -> RouteStateResult<Vec<String>> {
        #[cfg(any(test, feature = "test-support"))]
        let _writer_entry = crate::writer_entry_trace::enter(
            "account_tx::fence_rechecks::probe_authorization_owners",
        );
        if !enabled(self.conn).map_err(err)? {
            return Ok(Vec::new());
        }
        self.conn
            .prepare("SELECT DISTINCT owner FROM probe_authorization ORDER BY owner")
            .map_err(err)?
            .query_map([], |row| row.get(0))
            .map_err(err)?
            .collect::<rusqlite::Result<Vec<String>>>()
            .map_err(err)
    }

    /// SQL-backed reference read for current test authorizations on these exact
    /// origins. Live requests use the published block state's matching read.
    pub fn probe_authorizations_for_origins(
        &self,
        origins: &[FenceRecheck],
    ) -> RouteStateResult<Vec<crate::block_state::ProbeAuthorization>> {
        if origins.is_empty() || !enabled(self.conn).map_err(err)? {
            return Ok(Vec::new());
        }
        let mut statement = self.conn.prepare(
            "SELECT a.owner,a.kind,a.origin_key,a.evidence,a.evidence_at,a.claimed_at,a.claim_generation
             FROM probe_authorization a JOIN block r
               ON r.kind=a.kind AND r.origin_key=a.origin_key
               AND r.evidence=a.evidence AND r.evidence_at=a.evidence_at
                AND r.claim_generation=a.claim_generation AND r.cleared_at IS NULL
                AND r.is_origin=1 AND r.managed=1
               AND (r.claim_owner IS NULL OR r.claim_owner=a.owner)
             WHERE a.kind=?1 AND a.origin_key=?2 AND a.evidence=?3 AND a.evidence_at=?4",
        ).map_err(err)?;
        let mut rows = Vec::new();
        for origin in origins {
            rows.extend(
                statement
                    .query_map(
                        params![
                            origin.kind,
                            origin.origin_key,
                            origin.evidence,
                            origin.evidence_at
                        ],
                        |row| {
                            Ok(crate::block_state::ProbeAuthorization {
                                owner: row.get(0)?,
                                kind: row.get(1)?,
                                origin_key: row.get(2)?,
                                evidence: row.get(3)?,
                                evidence_at: row.get(4)?,
                                claimed_at: row.get(5)?,
                                claim_generation: row.get(6)?,
                            })
                        },
                    )
                    .map_err(err)?
                    .collect::<rusqlite::Result<Vec<_>>>()
                    .map_err(err)?,
            );
        }
        rows.sort_by(|a, b| {
            (&a.owner, &a.kind, &a.origin_key).cmp(&(&b.owner, &b.kind, &b.origin_key))
        });
        rows.dedup();
        Ok(rows)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn probe_authorizations_for_origins_match_sql_and_reject_stale_proofs() {
        let scratch = cortexkit_test_support::ScratchDir::new("probe-authorization-read");
        let now = 1_900_000_000_000;
        let store = crate::legacy_fixture_inputs::migrated_parity_store(
            &scratch.path().join("store.db"),
            now,
        );
        let origin = store
            .account_read(|tx| tx.load_block_state()?.fence_rechecks(now))
            .unwrap()
            .into_iter()
            .find(|origin| !origin.quota && origin.due_at() <= now && origin.claim_owner.is_none())
            .unwrap();
        store.account_transaction(|tx| {
            assert!(tx.grant_fence_rechecks(std::slice::from_ref(&origin), now, Some("read-test"))?);
            tx.conn.execute(
                "INSERT INTO probe_authorization(owner,kind,origin_key,evidence,evidence_at,claimed_at,claim_generation)
                 SELECT 'stale-test',kind,origin_key,evidence,evidence_at,claimed_at,claim_generation-1
                 FROM probe_authorization WHERE owner='read-test'",
                [],
            ).map_err(crate::err)?;
            Ok(())
        }).unwrap();
        let check = |expected: usize| {
            let (memory, sql) = store
                .account_read(|tx| {
                    let blocks = tx.load_block_state()?;
                    Ok((
                        blocks.probe_authorizations_for_origins(std::slice::from_ref(&origin)),
                        tx.probe_authorizations_for_origins(std::slice::from_ref(&origin))?,
                    ))
                })
                .unwrap();
            assert_eq!(memory, sql);
            assert_eq!(memory.len(), expected, "{memory:?}");
            if expected != 0 {
                assert_eq!(memory[0].owner, "read-test");
            }
        };
        check(1);
        let mut unrelated = origin.clone();
        unrelated.evidence.push_str("-newer-evidence");
        store
            .account_read(|tx| {
                assert!(tx
                    .load_block_state()?
                    .probe_authorizations_for_origins(std::slice::from_ref(&unrelated))
                    .is_empty());
                assert!(tx
                    .probe_authorizations_for_origins(std::slice::from_ref(&unrelated))?
                    .is_empty());
                Ok(())
            })
            .unwrap();
        store.account_transaction(|tx| {
            tx.conn.execute("UPDATE block SET claim_generation=claim_generation+1 WHERE kind=?1 AND origin_key=?2 AND is_origin=1 AND managed=1",
                rusqlite::params![origin.kind, origin.origin_key]).map_err(crate::err)?;
            Ok(())
        }).unwrap();
        check(0);
        store.account_transaction(|tx| {
            tx.conn.execute("UPDATE block SET claim_generation=claim_generation-1,cleared_at=?3 WHERE kind=?1 AND origin_key=?2 AND is_origin=1 AND managed=1",
                rusqlite::params![origin.kind, origin.origin_key, now + 1]).map_err(crate::err)?;
            Ok(())
        }).unwrap();
        check(0);
    }
}
