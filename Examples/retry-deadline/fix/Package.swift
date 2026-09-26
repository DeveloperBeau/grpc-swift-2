// swift-tools-version:6.1
import PackageDescription

let package = Package(
    name: "grpc-retry-deadline-repro-fix",
    platforms: [.macOS(.v15)],
    dependencies: [
        .package(name: "grpc-swift-2", path: "../../..")
    ],
    targets: [
        .executableTarget(
            name: "fix",
            dependencies: [
                .product(name: "GRPCCore", package: "grpc-swift-2")
            ]
        )
    ]
)
