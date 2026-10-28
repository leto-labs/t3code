import type { ProviderInstanceId, ResolvedProviderInstanceEnvironment } from "@t3tools/contracts";
import type { ProviderInstance } from "@t3tools/provider-core/server/driver";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import type * as Scope from "effect/Scope";

/** Lazy access lets a composite instance resolve peers after registry reconciliation commits. */
export class ProviderInstanceLookup extends Context.Reference<{
  readonly get: (id: ProviderInstanceId) => Effect.Effect<ProviderInstance | undefined>;
  /** The environment the live instance was built with, its secret references already read. */
  readonly environment: (
    id: ProviderInstanceId,
  ) => Effect.Effect<ResolvedProviderInstanceEnvironment | undefined>;
  readonly changes: Stream.Stream<void>;
  readonly subscribe: Effect.Effect<Stream.Stream<void>, never, Scope.Scope>;
}>("t3/provider/ProviderInstanceLookup", {
  defaultValue: () => ({
    get: () => Effect.void.pipe(Effect.as(undefined as ProviderInstance | undefined)),
    environment: () => Effect.void.pipe(Effect.as(undefined)),
    changes: Stream.empty,
    subscribe: Effect.succeed(Stream.empty),
  }),
}) {}
