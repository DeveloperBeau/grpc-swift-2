# grpc-swift-2 retry vs. deadline repro

Two small Swift programs. One shows a bug in `RetryExecutor.execute` (`GRPCCore`,
`Sources/GRPCCore/Call/Client/Internal/ClientRPCExecutor+RetryExecutor.swift`); the
other runs the same code against the fix.

## What this solves

A retried RPC made while the connection is down, or not ready yet, fails before its
stream opens. That is the failure these programs simulate. The client knows the
call has failed as soon as the transport throws, yet it waits for the full timeout
before reporting it.

The second problem is quieter. A transient `.unavailable` is the case a retry
policy is for, but the executor never applies the policy to a failure before the
stream opens. A call that would have succeeded on its second attempt fails after
its first.

With the fix, such a call fails at once when the error is not retryable, and
retries when it is.

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
   child task sleeping until the overall deadline. Returning from the closure does
   not end that task, and `withTaskGroup` waits for every child task before it
   returns. The attempt failed at once, but the call waits out the deadline task's
   sleep, which is the full configured timeout.
2. **No check against the retry policy.** The catch block returns the failure
   without looking at `self.policy.retryableStatusCodes` or
   `attempt < self.policy.maxAttempts`. A pre-stream failure gets one attempt
   whatever the policy says, even for a code such as `.unavailable` that the
   policy lists as retryable.

The fix adds the missing retry check. When the error is retryable and attempts
remain, the loop backs off and tries again. When it gives up, it calls
`group.cancelAll()` first, so the deadline task ends with it.

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

Each option below uses what grpc-swift-2 already offers, with what it costs.
All of them were checked against 2.4.3 on 26 September 2026.

### "Upgrade grpc-swift-2."

There is nothing to upgrade to. 2.4.3 is the newest release, and upstream `main`
still points at the 2.4.3 commit, so no unreleased change is waiting either. The
last change to `ClientRPCExecutor+RetryExecutor.swift` was #46 ("Return
deadlineExceeded when client timeout fires"), which shipped before 2.4.3. No
issue reports this bug. The one open pull request that touches the file, #51
(a draft adding client diagnostics), edits the same `catch` block to record the
error and leaves `return .failure(error)` as it is.

### "Turn retries off for that method."

Without an execution policy, the call goes through the one-shot executor. Its
`withDeadline` cancels the deadline task once the work finishes, so a pre-stream
failure returns at once. This removes the hang, but it also removes retries for
that method entirely, including the retries after the stream opens that work
correctly today.

### "Use a hedging policy instead of a retry policy."

The hedging executor gets this case right. Its `.noStreamAvailable` case calls
`group.cancelAll()` and returns (`ClientRPCExecutor+HedgingExecutor.swift`), which
suggests the retry executor's missing call is an oversight. Hedging has different
semantics, though. It sends attempts in parallel, is only safe for idempotent
methods, and gives up on a pre-stream failure without trying again.

### "Set a shorter timeout."

The hang lasts as long as the timeout, so a shorter timeout shortens it. It also
cuts off calls that are slow for good reasons, and the call still makes one
attempt.

### "Don't set a timeout at all."

`execute` only adds the deadline task when a deadline exists, so with no timeout
there is no hang. There is still no retry, though, and every call becomes
unbounded, which is worse than the bug.

### "Set `waitForReady: true`."

With `waitForReady`, a call made before the connection is ready queues until it is.
A channel that is shutting down, or a transport that is closed, still throws before
the stream opens and lands in the same catch block. Queueing also costs the fast
failure some callers want. It suits some methods, and leaves the executor as it is.

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

The two halves are independent, and `cancelAll()` alone closes the hang. What it
leaves is a fast failure that still ignores the policy: with `.unavailable` in
`retryableStatusCodes` and `maxAttempts: 3`, the call makes one attempt. The NIO
transport throws `.unavailable` in several of its pre-stream cases. The patch keeps
the two halves separate, so upstream can take either one.

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
`RPCError` has none, so the policy has no way to name it. The patch treats it as
not retryable. `ClientRPCExecutorTests+Retries.swift` pins this with
`testRetryUnableToOpenStreamWithANonRPCErrorReturnsAfterOneAttempt`.

### Known limitation

Between pre-stream attempts, the patch does not check the deadline, and neither does
the existing post-stream retry path while it sleeps a backoff. If the deadline passes
during a run of pre-stream failures, the call still makes its remaining attempts.
It then reports the last error, not `.deadlineExceeded`. The overrun is
bounded by `maxAttempts` and the policy's `maxBackoff`. Checking the deadline before
`continue` would close it, and upstream may prefer that.

## Layout

- `bug/` resolves upstream `grpc/grpc-swift-2` at revision
  `ac33066eb6edb1a21a6ca172ea8184a9b06f3cc7`, which is 2.4.3 without the fix.
- `fix/` resolves this repository's own checkout (`path: "../../.."`), so it runs
  whatever revision is checked out here.

Each is a SwiftPM executable with one `Package.swift` and one `main.swift`. The two
`main.swift` files are byte-for-byte identical; only the dependency differs. Each
package has its own `.build` directory, so resolving one never disturbs the other.

`main.swift` defines a `ClientTransport` whose `withStream` always throws an
`RPCError` before a stream opens, and counts attempts. It runs a `GRPCClient` over
that transport with a retry policy (3 attempts, retrying `.unavailable`, short
backoff) and a 10 second timeout, in two scenarios:

1. `.failedPrecondition`, which the policy does not retry. The client should
   return after one attempt, with no wait.
2. `.unavailable`, which the policy retries. The client should make 3 attempts,
   each failing at once, and finish well inside the timeout.

For each scenario the program prints the elapsed time and the attempt count, each
followed by a `BUG:` or `OK:` line.

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

The transport's withStream throws before any stream opens, the way a connection
that cannot be established does. Every call uses a retry policy of 3 attempts,
retrying on .unavailable with a short backoff, and a 10 second timeout.

Scenario 1: non-retryable failure (.failedPrecondition)
The policy does not retry this code, so the client should return the failure
after one attempt, with no wait.
  elapsed: 10.005s
  attempts: 1
  BUG: failed after 10.0s (expected ~0s)
  OK: 1 attempt(s) (matches policy)

Scenario 2: retryable failure (.unavailable)
The policy retries this code, so the client should make all 3 attempts. Each one
fails at once, so the call should end well inside the 10 second timeout.
  elapsed: 10.005s
  attempts: 1
  BUG: failed after 10.0s (expected ~0s)
  BUG: 1 attempt(s) (policy allows 3)

```

`fix/` (this repository):

```
gRPC retry vs. deadline repro
==============================

The transport's withStream throws before any stream opens, the way a connection
that cannot be established does. Every call uses a retry policy of 3 attempts,
retrying on .unavailable with a short backoff, and a 10 second timeout.

Scenario 1: non-retryable failure (.failedPrecondition)
The policy does not retry this code, so the client should return the failure
after one attempt, with no wait.
  elapsed: 0.001s
  attempts: 1
  OK: failed after 0.001s (~0s expected)
  OK: 1 attempt(s) (matches policy)

Scenario 2: retryable failure (.unavailable)
The policy retries this code, so the client should make all 3 attempts. Each one
fails at once, so the call should end well inside the 10 second timeout.
  elapsed: 0.046s
  attempts: 3
  OK: failed after 0.046s (~0s expected)
  OK: 3 attempt(s) (matches policy)

```

In scenario 1 only the timing differs: `.failedPrecondition` is not retryable in
either version, so both make one attempt. Upstream takes the full 10 seconds to
report it; the fix reports it in about a millisecond.

Scenario 2 differs on both counts. Upstream stops after one attempt and still waits
10 seconds. The fix makes all 3 attempts and finishes in well under a tenth of a
second.
