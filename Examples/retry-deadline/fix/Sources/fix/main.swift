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
        // Every scenario fails, because the transport never opens a stream. What the
        // scenario measures is how long the failure took and how many attempts ran.
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

The transport's withStream throws before any stream opens, the way a connection
that cannot be established does. Every call uses a retry policy of 3 attempts,
retrying on .unavailable with a short backoff, and a 10 second timeout.

""")

print("Scenario 1: non-retryable failure (.failedPrecondition)")
print("The policy does not retry this code, so the client should return the failure")
print("after one attempt, with no wait.")
let scenario1 = await runScenario(code: .failedPrecondition, maxAttempts: 3)
printVerdicts(result: scenario1, expectedAttempts: 1, maxAttempts: 3)

print("Scenario 2: retryable failure (.unavailable)")
print("The policy retries this code, so the client should make all 3 attempts. Each one")
print("fails at once, so the call should end well inside the 10 second timeout.")
let scenario2 = await runScenario(code: .unavailable, maxAttempts: 3)
printVerdicts(result: scenario2, expectedAttempts: 3, maxAttempts: 3)
