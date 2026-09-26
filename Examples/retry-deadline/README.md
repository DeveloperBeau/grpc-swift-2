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

The patch is 19 lines in the `catch` block:

```swift
} catch {
  let isRetryableStatusCode: Bool
  if let rpcError = error as? RPCError {
    isRetryableStatusCode = self.policy.retryableStatusCodes.contains(Status.Code(rpcError.code))
  } else {
    isRetryableStatusCode = false
  }

  if isRetryableStatusCode, attempt < self.policy.maxAttempts,
    !(self.transport.retryThrottle?.recordFailure() ?? false)
  {
    try? await Task.sleep(
      until: .now.advanced(by: delayIterator.next()!),
      tolerance: .zero,
      clock: .continuous
    )
    continue
  }

  group.cancelAll()
  return .failure(error)
}
```

## Can existing configuration avoid the problem?

Before patching a dependency, it is worth asking whether an existing setting
already avoids the problem. The sections below cover each option and what it costs.
Everything was checked against 2.4.3, which is the latest release as of September
2026. The last upstream change to `ClientRPCExecutor+RetryExecutor.swift` is #46
("Return deadlineExceeded when client timeout fires"), and no open issue covers this.

### "Upgrade grpc-swift-2."

There is nothing to upgrade to. 2.4.3 is the newest release and still has this code.

### "Turn retries off for that method."

Without an execution policy, the call goes through the one-shot executor. Its
`withDeadline` cancels the deadline task once the work finishes, so a pre-stream
failure returns at once. This removes the hang, but it also removes retries for
that method entirely, including the retries after the stream opens that work
correctly today.

### "Use a hedging policy instead of a retry policy."

The hedging executor does handle it correctly: its `.noStreamAvailable` case calls
`group.cancelAll()` and returns (`ClientRPCExecutor+HedgingExecutor.swift`). That is
good evidence the retry executor's catch block is an oversight rather than a design
choice. But hedging is not a substitute for retries. It sends parallel attempts, is
only safe for idempotent methods, and on a pre-stream failure it gives up rather
than trying again.

### "Set a shorter timeout."

In the buggy version the hang lasts exactly as long as the timeout. A shorter
timeout shortens the hang, but it also cuts off calls that are legitimately slow,
and the call still makes only one attempt. The timeout ends up tuned around a bug
instead of around the method.

### "Don't set a timeout at all."

`execute` only adds the deadline task when a deadline exists, so with no timeout
there is no hang. There is still no retry, though, and every call becomes
unbounded, which is worse than the bug.

### "Set `waitForReady: true`."

That changes what happens when the connection is not ready yet: the call queues
instead of failing. It does not help a channel that is shutting down or a transport
that is closed. Those still throw before the stream opens, and hit the same catch
block. It also waits when you may want the call to fail fast. It is a legitimate
per-method choice, but not a fix for the executor.

### "The transport already retries stream creation."

The NIO transport does retry some outcomes itself: `GRPCChannel.withStream` loops
on `.tryAgain`. The cases it gives up on (`.stopTrying`, such as a channel that is
shutting down, or transient failure without `waitForReady`) are the ones that reach
the executor. The in-process transport retries nothing. No transport setting keeps
these failures away from the retry executor's catch block.

### "Put the handling in an interceptor."

Interceptors run inside `ClientRPCExecutor._execute`, which is only called from
inside the `withStream` closure. A failure before the stream opens never reaches an
interceptor, so no interceptor can observe it.

## Questions about the patch itself

### "Just add `group.cancelAll()`. The retry part is a separate change."

That is true: the two halves are independent, and the one-line `cancelAll()` on its
own closes the hang. On its own, though, it turns a slow failure into a fast one and
still ignores the retry policy. With `.unavailable` in `retryableStatusCodes` and
`maxAttempts: 3`, the call still makes one attempt. `.unavailable` is exactly what
the NIO transport throws before a stream opens. The two halves are kept apart in
the patch, so upstream can take either one.

### "Why not reuse `executeAttempt`'s retry check?"

`executeAttempt` only runs once a stream exists. It receives the stream and its
context as arguments, so it cannot be called for a failure that happens before
there is a stream. The patch copies its decision instead:

- retryable status code;
- attempts left;
- a retry throttle that allows another failure (`recordFailure()`);
- the same backoff sequence.

Server pushback is not consulted, because before a stream opens there is no server
response to carry it.

### "A non-`RPCError` should be retried too."

The retry policy is expressed in status codes, and an error that is not an
`RPCError` has none. The patch treats it as not retryable, which is what the
policy can actually express. `ClientRPCExecutorTests+Retries.swift` pins this with
`testRetryUnableToOpenStreamWithANonRPCErrorReturnsAfterOneAttempt`.

### Known limitation

Between pre-stream attempts, the patch does not check the deadline, and neither does
the existing post-stream retry path while it sleeps a backoff. If the deadline passes
during a run of pre-stream failures, the call still makes its remaining attempts.
It then reports the last error rather than `.deadlineExceeded`. The overrun is
bounded by `maxAttempts` and the policy's `maxBackoff`. Checking the deadline before
`continue` would close it, and upstream may prefer that.

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
