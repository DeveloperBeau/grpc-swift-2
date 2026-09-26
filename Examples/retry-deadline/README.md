# grpc-swift-2 retry vs. deadline repro

Two tiny standalone Swift programs that show a bug in `RetryExecutor.execute` (in
`GRPCCore`, `Sources/GRPCCore/Call/Client/Internal/ClientRPCExecutor+RetryExecutor.swift`)
and confirm a fix for it.

## What this solves

An app making a retried RPC while its connection is down or not yet ready will run
into exactly the kind of failure these programs simulate: the stream never opens.
A correct client already knows the call failed the moment the transport says so, so
waiting out the deadline afterward is pure delay - something that should fail or
retry right away instead hangs for the full timeout. Separately, a transient
`.unavailable` failure is exactly what a retry policy exists for. If the executor
never applies that policy to a pre-stream failure, a call that would have succeeded
on a second attempt fails outright after just one. The fix makes such calls fail
immediately when the error is not retryable, and actually retry when it is.

## The bug

When `ClientTransport.withStream` throws before a stream opens, the attempt loop in
`RetryExecutor.execute` catches the error and does only this:

```swift
} catch {
  return .failure(error)
}
```

Two things are missing:

1. **No `group.cancelAll()`.** `execute` runs a `withTaskGroup` that also holds a
   child task sleeping until the overall deadline. Returning from the closure
   without cancelling that task doesn't cancel it: `withTaskGroup` still waits for
   every child task to finish before it returns. So even though the attempt failed
   immediately, the whole call sits there until the deadline task's sleep finishes,
   which takes as long as the configured timeout.
2. **No check against the retry policy.** The catch block returns the failure
   straight away, without ever looking at `self.policy.retryableStatusCodes` or
   `attempt < self.policy.maxAttempts`. So a pre-stream failure gets exactly one
   attempt, no matter how the retry policy is configured, and no matter whether the
   status code (for example `.unavailable`) is one the policy says to retry.

The fix in this repository adds the missing retry check, then calls
`group.cancelAll()` before returning the failure: if the error is retryable and
there are attempts left, it backs off and retries instead of returning, and it
cancels the deadline task on every path that actually gives up.

## Layout

- `bug/` - depends on upstream `grpc/grpc-swift-2` pinned to revision
  `ac33066eb6edb1a21a6ca172ea8184a9b06f3cc7` (2.4.3, unpatched).
- `fix/` - depends on this repository's own checkout (`path: "../../.."`), so it
  runs whatever revision is checked out here.

Both programs are otherwise the same SwiftPM executable: a `Package.swift` and one
`Sources/<target>/main.swift`. The `main.swift` files are byte-for-byte identical
(a diff between them is empty); only the dependency each `Package.swift` resolves
to differs. They are separate packages with separate `.build` directories, so
resolving one never disturbs the other.

`main.swift` defines a `ClientTransport` whose `withStream` always throws an
`RPCError` before a stream opens, counts attempts, and runs a `GRPCClient` against
it with a retry policy (max 3 attempts, retryable on `.unavailable`, a short
backoff) and a 10 second timeout, for two scenarios:

1. A non-retryable failure (`.failedPrecondition`): a correct client gives up after
   one attempt and returns right away.
2. A retryable failure (`.unavailable`): a correct client retries up to 3 times,
   with every attempt failing fast, so it finishes in well under 10 seconds.

Each program prints elapsed time and attempt count for both scenarios, with a
`BUG:` or `OK:` verdict line for each fact.

## Running it

```
cd bug && swift run
```

```
cd fix && swift run
```

## Observed output

`bug/` (upstream 2.4.3):

```
gRPC retry vs. deadline repro
==============================

Setup: a client transport whose withStream throws before any stream opens, standing
in for a connection that cannot be established. Every call below runs with a retry
policy (max 3 attempts, retryable on .unavailable, short backoff) and a 10 second
timeout.

What this solves: an app making a retried RPC while its connection is down or not
yet ready hits exactly this kind of pre-stream failure. A correct client already
knows the call failed the moment the transport says so, so waiting out the deadline
afterward is pure delay - something that should fail or retry right away instead
hangs for the full timeout. Separately, a transient .unavailable failure is exactly
what the retry policy exists for; if the executor never applies that policy to a
pre-stream failure, a call that would have succeeded on a second attempt fails
outright after just one. A fixed executor fails immediately when the error is not
retryable, and actually retries when it is.

Scenario 1: non-retryable failure (.failedPrecondition)
A correct client sees that this status is not in the retry policy's list, gives
up after the single attempt, and returns right away - it has no reason to wait
out a 10 second deadline it never needed.
  elapsed: 10.006s
  attempts: 1
  BUG: failed after 10.0s (expected ~0s)
  OK: 1 attempt(s) (matches policy)

Scenario 2: retryable failure (.unavailable)
The retry policy allows up to 3 attempts for .unavailable, so a correct client
retries twice more after the first pre-stream failure. Each attempt fails fast
(no stream ever opens), so the whole call should finish well before the 10
second deadline.
  elapsed: 10.001s
  attempts: 1
  BUG: failed after 10.0s (expected ~0s)
  BUG: 1 attempt(s) (policy allows 3)
```

`fix/` (this repository):

```
gRPC retry vs. deadline repro
==============================

Setup: a client transport whose withStream throws before any stream opens, standing
in for a connection that cannot be established. Every call below runs with a retry
policy (max 3 attempts, retryable on .unavailable, short backoff) and a 10 second
timeout.

What this solves: an app making a retried RPC while its connection is down or not
yet ready hits exactly this kind of pre-stream failure. A correct client already
knows the call failed the moment the transport says so, so waiting out the deadline
afterward is pure delay - something that should fail or retry right away instead
hangs for the full timeout. Separately, a transient .unavailable failure is exactly
what the retry policy exists for; if the executor never applies that policy to a
pre-stream failure, a call that would have succeeded on a second attempt fails
outright after just one. A fixed executor fails immediately when the error is not
retryable, and actually retries when it is.

Scenario 1: non-retryable failure (.failedPrecondition)
A correct client sees that this status is not in the retry policy's list, gives
up after the single attempt, and returns right away - it has no reason to wait
out a 10 second deadline it never needed.
  elapsed: 0.001s
  attempts: 1
  OK: failed after 0.001s (~0s expected)
  OK: 1 attempt(s) (matches policy)

Scenario 2: retryable failure (.unavailable)
The retry policy allows up to 3 attempts for .unavailable, so a correct client
retries twice more after the first pre-stream failure. Each attempt fails fast
(no stream ever opens), so the whole call should finish well before the 10
second deadline.
  elapsed: 0.091s
  attempts: 3
  OK: failed after 0.091s (~0s expected)
  OK: 3 attempt(s) (matches policy)
```

Scenario 1's timing is the only thing that differs between the two, since
`.failedPrecondition` isn't retryable in either version, so both take exactly one
attempt. Scenario 2 differs on both counts: upstream stops after one attempt and
still takes 10 seconds to report it, while the fix retries all 3 attempts and
finishes in well under a tenth of a second.
