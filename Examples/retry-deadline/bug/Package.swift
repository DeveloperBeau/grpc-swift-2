// swift-tools-version:6.1
import PackageDescription

let package = Package(
    name: "grpc-retry-deadline-repro-bug",
    platforms: [.macOS(.v15)],
    dependencies: [
        .package(
            url: "https://github.com/grpc/grpc-swift-2.git",
            revision: "ac33066eb6edb1a21a6ca172ea8184a9b06f3cc7"
        )
    ],
    targets: [
        .executableTarget(
            name: "bug",
            dependencies: [
                .product(name: "GRPCCore", package: "grpc-swift-2")
            ]
        )
    ]
)
