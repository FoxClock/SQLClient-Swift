// swift-tools-version:5.9
import PackageDescription

let package = Package(
    name: "SQLClient-Swift",
    platforms: [
        .macOS(.v13),
        .iOS(.v13)
    ],
    products: [
        .library(name: "SQLClient", targets: ["SQLClient"]),
    ],
    targets: [
        .systemLibrary(
            name: "CSybdb",
            pkgConfig: "freetds",
            providers: [
                .brew(["freetds"]),
                .apt(["freetds-dev"])
            ]
        ),
        .target(
            name: "SQLClient",
            dependencies: ["CSybdb"],
            linkerSettings: [
                .unsafeFlags(["-L/opt/homebrew/opt/freetds/lib"], .when(platforms: [.macOS])),
                .linkedLibrary("sybdb"),
                .linkedLibrary("iconv", .when(platforms: [.macOS]))
            ]
        )
    ]
)
