import { extractJson } from "./extract-json";
import { mockStream, type MockBehavior, type MockState } from "./anthropic-mock";

export interface GenerateInput {
  /** Drives the mock streaming client (see anthropic-mock.ts). */
  behavior: MockBehavior;
  /** Hands the finished draft to the next pipeline stage. May reject. */
  advanceToNextStage: () => Promise<void>;
  /** Returns true once the draft passes review. Scripted by callers/tests. */
  reviewPasses: (attempt: number) => boolean;
}

export interface GenerateResult {
  status: "ok" | "error";
  attempts: number;
}

const MAX_REVISIONS = 3;
const MAX_STREAM_ATTEMPTS = 3;

function canRetryStream(error: unknown): boolean {
  return (
    error instanceof Error &&
    ("status" in error ? error.status === 429 : true)
  );
}

async function streamAndExtractJson(
  behavior: MockBehavior,
  state: MockState,
): Promise<boolean> {
  for (let attempt = 1; attempt <= MAX_STREAM_ATTEMPTS; attempt += 1) {
    try {
      const text = await mockStream(behavior, state);
      extractJson(text);
      return true;
    } catch (error) {
      if (attempt === MAX_STREAM_ATTEMPTS || !canRetryStream(error)) {
        return false;
      }
    }
  }

  return false;
}

/**
 * Runs one content-generation pass: stream a draft, extract it, revise until it
 * passes review, then hand off to the next stage.
 *
 * This is a faithful (stripped-down) reproduction of the real pipeline — and it
 * ships with three real bugs from that pipeline. Your job is to fix them so the
 * test suite passes. See the README for the symptoms. (Do not edit the tests.)
 */
export async function generate(input: GenerateInput): Promise<GenerateResult> {
  const state: MockState = { calls: 0 };

  // The model call can fail transiently (rate limits) or return a truncated
  // stream. Retry a small number of times before failing the run.
  if (!(await streamAndExtractJson(input.behavior, state))) {
    return { status: "error", attempts: 0 };
  }

  // Revise until the draft passes review.
  let attempt = 0;
  let passedReview = input.reviewPasses(attempt);
  while (!passedReview && attempt < MAX_REVISIONS) {
    attempt += 1;
    passedReview = input.reviewPasses(attempt);
  }

  if (!passedReview) {
    return { status: "error", attempts: attempt };
  }

  // Kick off the next stage and return.
  try {
    await input.advanceToNextStage();
  } catch {
    return { status: "error", attempts: attempt };
  }

  return { status: "ok", attempts: attempt };
}

export { MAX_REVISIONS };
