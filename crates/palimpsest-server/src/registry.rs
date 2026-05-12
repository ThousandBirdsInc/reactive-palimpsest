// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Subscription registry keyed by both server-assigned id and the
//! client-supplied `(connection, client_id)` pair.

use std::collections::{btree_map::Entry, BTreeMap};

use crate::{
    error::RouterError,
    subscription::{ClientSubscriptionId, ConnectionId, Subscription, SubscriptionId},
};

/// Two-level index over active subscriptions.
///
/// * Forward lookup by `SubscriptionId` is the common path used by the
///   ack/diff/teardown handlers.
/// * Reverse lookup by `(ConnectionId, ClientSubscriptionId)` enforces
///   client-side idempotency: a duplicate `subscribe` for the same
///   client label is rejected with `DuplicateClientSubscriptionId`.
#[derive(Debug, Default)]
#[allow(clippy::struct_field_names)]
pub struct SubscriptionRegistry {
    by_id: BTreeMap<SubscriptionId, Subscription>,
    by_client_id: BTreeMap<(ConnectionId, ClientSubscriptionId), SubscriptionId>,
    by_connection: BTreeMap<ConnectionId, Vec<SubscriptionId>>,
}

impl SubscriptionRegistry {
    /// Creates an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts a new subscription. Errors if `(connection, client_id)`
    /// already maps to another subscription.
    pub fn insert(&mut self, subscription: Subscription) -> Result<(), RouterError> {
        let key = (subscription.connection, subscription.client_id.clone());
        match self.by_client_id.entry(key) {
            Entry::Vacant(slot) => {
                slot.insert(subscription.id);
            }
            Entry::Occupied(_) => {
                return Err(RouterError::DuplicateClientSubscriptionId);
            }
        }
        self.by_connection
            .entry(subscription.connection)
            .or_default()
            .push(subscription.id);
        self.by_id.insert(subscription.id, subscription);
        Ok(())
    }

    /// Looks up a subscription by server id.
    #[must_use]
    pub fn get(&self, id: SubscriptionId) -> Option<&Subscription> {
        self.by_id.get(&id)
    }

    /// Looks up a subscription by server id (mut).
    pub fn get_mut(&mut self, id: SubscriptionId) -> Option<&mut Subscription> {
        self.by_id.get_mut(&id)
    }

    /// Resolves the server id for a `(connection, client_id)` pair.
    #[must_use]
    pub fn resolve(
        &self,
        connection: ConnectionId,
        client_id: &ClientSubscriptionId,
    ) -> Option<SubscriptionId> {
        // BTreeMap lookups need a borrowed key tuple matching the stored
        // owned tuple. We materialize a transient tuple by cloning the
        // client id; subscription churn is two orders of magnitude
        // rarer than ack/diff traffic, so the clone is fine here.
        self.by_client_id
            .get(&(connection, client_id.clone()))
            .copied()
    }

    /// Removes a subscription, returning the dropped record.
    pub fn remove(&mut self, id: SubscriptionId) -> Option<Subscription> {
        let subscription = self.by_id.remove(&id)?;
        self.by_client_id
            .remove(&(subscription.connection, subscription.client_id.clone()));
        if let Some(list) = self.by_connection.get_mut(&subscription.connection) {
            list.retain(|item| *item != id);
            if list.is_empty() {
                self.by_connection.remove(&subscription.connection);
            }
        }
        Some(subscription)
    }

    /// Returns every subscription id owned by `connection`.
    #[must_use]
    pub fn connection_subscriptions(&self, connection: ConnectionId) -> Vec<SubscriptionId> {
        self.by_connection
            .get(&connection)
            .cloned()
            .unwrap_or_default()
    }

    /// Total active subscriptions.
    #[must_use]
    pub fn len(&self) -> usize {
        self.by_id.len()
    }

    /// True when no subscriptions are active.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.by_id.is_empty()
    }

    /// Iterates all subscriptions in canonical id order.
    pub fn iter(&self) -> impl Iterator<Item = (SubscriptionId, &Subscription)> {
        self.by_id.iter().map(|(id, sub)| (*id, sub))
    }
}

#[cfg(test)]
mod tests {
    use palimpsest_dataflow::palimpsest::Lsn;
    use palimpsest_permissions::UserContext;

    use super::{ClientSubscriptionId, ConnectionId, SubscriptionRegistry};
    use crate::{
        error::RouterError,
        subscription::{QueryId, SchemaId, Subscription, SubscriptionId, SubscriptionState},
    };

    fn dummy(id: SubscriptionId, connection: ConnectionId, client_id: &str) -> Subscription {
        Subscription {
            id,
            client_id: ClientSubscriptionId::new(client_id),
            connection,
            query: QueryId::new("q"),
            user_ctx: UserContext::new(std::iter::empty()),
            schema_id: SchemaId::new(1),
            snapshot_lsn: Lsn::new(0),
            cursor_lsn: Lsn::new(0),
            state: SubscriptionState::Seeding,
        }
    }

    #[test]
    fn insert_then_resolve_by_client_id() {
        let mut registry = SubscriptionRegistry::new();
        let sub = dummy(SubscriptionId::new(1), ConnectionId::new(10), "label");
        registry.insert(sub).unwrap();

        let resolved = registry.resolve(ConnectionId::new(10), &ClientSubscriptionId::new("label"));
        assert_eq!(resolved, Some(SubscriptionId::new(1)));
    }

    #[test]
    fn duplicate_client_id_is_rejected() {
        let mut registry = SubscriptionRegistry::new();
        registry
            .insert(dummy(SubscriptionId::new(1), ConnectionId::new(10), "x"))
            .unwrap();
        let err = registry
            .insert(dummy(SubscriptionId::new(2), ConnectionId::new(10), "x"))
            .unwrap_err();
        assert!(matches!(err, RouterError::DuplicateClientSubscriptionId));
    }

    #[test]
    fn same_label_on_different_connection_is_allowed() {
        let mut registry = SubscriptionRegistry::new();
        registry
            .insert(dummy(SubscriptionId::new(1), ConnectionId::new(10), "x"))
            .unwrap();
        registry
            .insert(dummy(SubscriptionId::new(2), ConnectionId::new(11), "x"))
            .unwrap();
        assert_eq!(registry.len(), 2);
    }

    #[test]
    fn remove_drops_both_indexes() {
        let mut registry = SubscriptionRegistry::new();
        let sub = dummy(SubscriptionId::new(1), ConnectionId::new(10), "x");
        registry.insert(sub).unwrap();
        registry.remove(SubscriptionId::new(1));
        assert_eq!(registry.len(), 0);
        assert!(registry
            .resolve(ConnectionId::new(10), &ClientSubscriptionId::new("x"))
            .is_none());
        assert!(registry
            .connection_subscriptions(ConnectionId::new(10))
            .is_empty());
    }

    #[test]
    fn connection_subscriptions_returns_all_owned() {
        let mut registry = SubscriptionRegistry::new();
        registry
            .insert(dummy(SubscriptionId::new(1), ConnectionId::new(7), "a"))
            .unwrap();
        registry
            .insert(dummy(SubscriptionId::new(2), ConnectionId::new(7), "b"))
            .unwrap();
        registry
            .insert(dummy(SubscriptionId::new(3), ConnectionId::new(8), "c"))
            .unwrap();

        let mut owned = registry.connection_subscriptions(ConnectionId::new(7));
        owned.sort();
        assert_eq!(owned, vec![SubscriptionId::new(1), SubscriptionId::new(2)]);
    }
}
