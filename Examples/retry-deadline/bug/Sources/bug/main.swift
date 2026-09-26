import Foundation
import GRPCCore

// This file is intentionally identical between the bug/ and fix/ demos. The only
// difference between the two programs is which revision of GRPCCore their
// Package.swift resolves to. See the top-level README for what changed between
// the two revisions.

// A transport whose `withStream` always throws before a stream opens, standing in for a
// connection that could not be established (a dial failure, a closed socket, a connect
// timeout that fires before the handshake finishes). Counts every attempt made against it.
// GRPCClient.runConnections() calls connect() and treats its return as "the transport shut
// down": once it returns, the client marks itself stopped and rejects further RPCs. A real
// transport's connect() stays suspended for as long as the connection is meant to live, so
// this fake one does the same, waiting for beginGracefulShutdown() before it returns.
final class ShutdownSignal: @unchecked Sendable {
    private let stream: AsyncStream<Void>
    private let continuation: AsyncStream<Void>.Continuation

    init() {
        var continuation: AsyncStream<Void>.Continuation!
        self.stream = AsyncStream { continuation = $0 }
        self.continuation = continuation
    }

    func waitForShutdown() async {
        for await _ in stream {}
    }

    func shutdown() {
        continuation.finish()
    }
}

struct FailBeforeStreamTransport: ClientTransport {
    typealias Bytes = [UInt8]

    let code: RPCError.Code
    let retryThrottle: RetryThrottle?
    let attemptCounter = AttemptCounter()
    let shutdownSignal = ShutdownSignal()

    init(code: RPCError.Code) {
        self.code = code
        self.retryThrottle = RetryThrottle(maxTokens: 10, tokenRatio: 0.1)
    }

    func connect() async throws {
        await shutdownSignal.waitForShutdown()
    }

    func beginGracefulShutdown() {
        shutdownSignal.shutdown()
    }

    func config(forMethod descriptor: MethodDescriptor) -> MethodConfig? { nil }

    func withStream<T: Sendable>(
        descriptor: MethodDescriptor,
        options: CallOptions,
        _ closure: (RPCStream<Inbound, Outbound>, ClientContext) async throws -> T
    ) async throws -> T {
        await attemptCounter.increment()
        throw RPCError(code: code, message: "the stream could not be opened")
    }
}

actor AttemptCounter {
    private(set) var count = 0
    func increment() { count += 1 }
}

struct RawBytesSerializer: MessageSerializer {
    func serialize<Bytes: GRPCContiguousBytes>(_ message: [UInt8]) throws -> Bytes {
        Bytes(message)
    }
}

struct RawBytesDeserializer: MessageDeserializer {
    func deserialize<Bytes: GRPCContiguousBytes>(_ serializedMessageBytes: Bytes) throws -> [UInt8] {
        serializedMessageBytes.withUnsafeBytes { Array($0) }
    }
}

struct ScenarioResult {
    let elapsedSeconds: Double
    let attempts: Int
}

func runScenario(code: RPCError.Code, maxAttempts: Int) async -> ScenarioResult {
    let transport = FailBeforeStreamTransport(code: code)

    var options = CallOptions.defaults
    options.timeout = .seconds(10)
    options.executionPolicy = .retry(
        RetryPolicy(
            maxAttempts: maxAttempts,
            initialBackoff: .milliseconds(50),
            maxBackoff: .milliseconds(200),
            backoffMultiplier: 1.6,
            retryableStatusCodes: [.unavailable]
        )
    )

    let clock = ContinuousClock()
    let start = clock.now
    do {
        try await withGRPCClient(transport: transport) { client in
            try await client.unary(
                request: ClientRequest(message: [1, 2, 3]),
                descriptor: MethodDescriptor(fullyQualifiedService: "demo.Service", method: "Call"),
                serializer: RawBytesSerializer(),
                deserializer: RawBytesDeserializer(),
                options: options
            ) { _ in }
        }
    } catch {
        // Expected: every scenario here ends in failure, since the transport never opens a
        // stream. The interesting facts are how long it took and how many attempts ran.
    }
    let elapsed = start.duration(to: clock.now)
    let components = elapsed.components
    let elapsedSeconds = Double(components.seconds) + Double(components.attoseconds) / 1e18
    let attempts = await transport.attemptCounter.count
    return ScenarioResult(elapsedSeconds: elapsedSeconds, attempts: attempts)
}

func printVerdicts(result: ScenarioResult, expectedAttempts: Int, maxAttempts: Int) {
    print(String(format: "  elapsed: %.3fs", result.elapsedSeconds))
    print("  attempts: \(result.attempts)")

    if result.elapsedSeconds > 5.0 {
        print(String(format: "  BUG: failed after %.1fs (expected ~0s)", result.elapsedSeconds))
    } else {
        print(String(format: "  OK: failed after %.3fs (~0s expected)", result.elapsedSeconds))
    }

    if result.attempts < expectedAttempts {
        print("  BUG: \(result.attempts) attempt(s) (policy allows \(maxAttempts))")
    } else {
        print("  OK: \(result.attempts) attempt(s) (matches policy)")
    }
    print("")
}

print("""
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

""")

print("Scenario 1: non-retryable failure (.failedPrecondition)")
print("A correct client sees that this status is not in the retry policy's list, gives")
print("up after the single attempt, and returns right away - it has no reason to wait")
print("out a 10 second deadline it never needed.")
let scenario1 = await runScenario(code: .failedPrecondition, maxAttempts: 3)
printVerdicts(result: scenario1, expectedAttempts: 1, maxAttempts: 3)

print("Scenario 2: retryable failure (.unavailable)")
print("The retry policy allows up to 3 attempts for .unavailable, so a correct client")
print("retries twice more after the first pre-stream failure. Each attempt fails fast")
print("(no stream ever opens), so the whole call should finish well before the 10")
print("second deadline.")
let scenario2 = await runScenario(code: .unavailable, maxAttempts: 3)
printVerdicts(result: scenario2, expectedAttempts: 3, maxAttempts: 3)
