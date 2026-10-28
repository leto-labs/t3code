import { TextGenerationError } from "@t3tools/contracts";
import type { ProviderInstance } from "@t3tools/provider-core/server/driver";
import * as Effect from "effect/Effect";

const unsupported = (operation: TextGenerationError["operation"]) => () =>
  Effect.fail(
    new TextGenerationError({
      operation,
      detail: "Choose a physical provider for text generation.",
    }),
  );

/** Auto only routes durable agent sessions; background title/commit helpers use an explicitly chosen provider. */
export const autoPoolTextGeneration: ProviderInstance["textGeneration"] = {
  generateCommitMessage: unsupported("generateCommitMessage"),
  generatePrContent: unsupported("generatePrContent"),
  generateTranscriptionPostProcessing: unsupported("generateTranscriptionPostProcessing"),
  generateBranchName: unsupported("generateBranchName"),
  generateThreadTitle: unsupported("generateThreadTitle"),
  generateReviewGuide: unsupported("generateReviewGuide"),
};
