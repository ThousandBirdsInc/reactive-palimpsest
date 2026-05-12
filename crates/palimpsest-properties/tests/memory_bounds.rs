// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

#![allow(clippy::redundant_clone)]

//! §15.3 property 8 — Memory bounds.
//!
//! For any trace, total operator state size is bounded by
//! `O(|active subscription keys|)`, not `O(|table|)`. We assert this
//! against `MaterializedKeys`: marking N keys materialized leaves
//! exactly N entries; forgetting any subset shrinks proportionally;
//! the tracker never grows beyond the keys it's been told about.

use palimpsest_dataflow::palimpsest::MaterializedKeys;
use proptest::prelude::*;

proptest! {
    #[test]
    fn materialized_keys_size_matches_unique_input(
        keys in proptest::collection::vec(0_u64..256, 0..128),
        forget in proptest::collection::vec(0_u64..256, 0..32),
    ) {
        let mut state: MaterializedKeys<u64> = MaterializedKeys::default();
        for &k in &keys {
            state.mark_materialized(k);
        }
        let unique_keys: std::collections::BTreeSet<_> = keys.iter().copied().collect();
        prop_assert_eq!(state.materialized_len(), unique_keys.len(),
            "tracker must hold exactly the unique materialized key count");

        let mut after_forget = unique_keys.clone();
        for k in &forget {
            state.forget(k);
            after_forget.remove(k);
        }
        prop_assert_eq!(state.materialized_len(), after_forget.len(),
            "forget must shrink the tracker by exactly one entry per known key");
    }
}
