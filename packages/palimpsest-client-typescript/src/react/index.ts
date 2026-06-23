// React hook surface. Importing from "@1kbirds/palimpsest-client/react" pulls
// in `react` as a peer dependency.

export {
  usePalimpsestClient,
  type ClientStatus,
  type UsePalimpsestClientResult,
} from "./usePalimpsestClient.js";
export {
  usePalimpsestSubscription,
  type SubscriptionStatus,
  type UseSubscriptionOptions,
  type UseSubscriptionResult,
} from "./usePalimpsestSubscription.js";
