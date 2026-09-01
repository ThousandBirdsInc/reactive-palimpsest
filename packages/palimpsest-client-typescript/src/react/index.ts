// React hook surface. Importing from "@1kbirds/palimpsest-client/react" pulls
// in `react` as a peer dependency.

export {
  usePalimpsestClient,
  type ClientStatus,
  type UsePalimpsestClientResult,
} from "./usePalimpsestClient.js";
export {
  usePalimpsestNamedSubscription,
  usePalimpsestSubscription,
  type SubscriptionStatus,
  type UseNamedSubscriptionOptions,
  type UseSubscriptionOptions,
  type UseSubscriptionResult,
} from "./usePalimpsestSubscription.js";
export {
  subscribeReplicaEvents,
  useLocalQuery,
  type LocalQueryStatus,
  type UseLocalQueryOptions,
  type UseLocalQueryResult,
} from "./useLocalQuery.js";
