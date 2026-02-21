# SQLClient-Swift

A Swift wrapper around FreeTDS (db-lib) for connecting to Microsoft SQL Server from macOS, iOS, and Linux.

## Features

- **Async/Await Support**: Modern Swift concurrency support.
- **FreeTDS 1.x Compatible**: Supports modern SQL Server features like `DATETIME2`, `NVARCHAR(MAX)`, and encryption.
- **Robust Connection Handling**: Optimized for both Homebrew (macOS) and APT (Linux) builds of FreeTDS.
- **Backward Compatible**: Works with older FreeTDS versions by gracefully falling back on advanced features.

## Requirements

### macOS
- Install FreeTDS via Homebrew: `brew install freetds`

### Linux (Ubuntu/Debian)
- Install FreeTDS development files: `sudo apt-get install freetds-dev freetds-bin`

## Installation

Add this package to your `Package.swift`:

```swift
dependencies: [
    .package(url: "https://github.com/YOUR_USERNAME/SQLClient-Swift.git", from: "1.0.0")
]
```

And add it to your target:

```swift
targets: [
    .target(
        name: "YourApp",
        dependencies: [
            .product(name: "SQLClient", package: "SQLClient-Swift")
        ]
    )
]
```

## Usage

### Simple Connection

```swift
import SQLClient

let client = SQLClient.shared
let connected = await client.connect(
    server: "your-server.com:1433",
    username: "sa",
    password: "your-password",
    database: "MyDatabase"
)

if connected {
    let results = await client.execute("SELECT * FROM Users")
    print(results)
}
```

### Advanced Connection Options

```swift
var options = SQLClientConnectionOptions(
    server: "your-server.com",
    username: "sa",
    password: "your-password",
    database: "MyDatabase"
)
options.port = 1433
options.encryption = .require
options.loginTimeout = 10

let connected = await client.connect(options: options)
```

## Environment Variables

You can set the `TDSVER` environment variable to control the protocol version (defaults to `7.4`).

```bash
export TDSVER=7.4
```

## License

MIT License. See [LICENSE](LICENSE) for details.
