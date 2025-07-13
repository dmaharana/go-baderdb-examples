# BadgerDB CLI

A simple command-line tool to inspect BadgerDB key-value stores. This tool allows you to either get a summary of all key prefixes in the database or view the contents of keys with a specific prefix.

## Features

- View a summary of all key prefixes and their counts
- Inspect key-value pairs with a specific prefix
- Read-only mode to safely explore databases
- Simple command-line interface

## Installation

1. Make sure you have Go installed (version 1.16 or later)
2. Clone this repository
3. Build the tool:
   ```bash
   go build -o badger-cli
   ```

## Usage

### View Database Summary

To get a summary of all key prefixes in the database:

```bash
./badger-cli -db /path/to/your/db -cmd summary
```

This will show:
- First few keys of each prefix (up to 3)
- A count of keys for each prefix

### View Specific Prefix Contents

To view all key-value pairs with a specific prefix:

```bash
./badger-cli -db /path/to/your/db -cmd view -prefix your_prefix
```

### Command Line Options

| Flag     | Default      | Description                                      |
|----------|--------------|--------------------------------------------------|
| `-db`    | "/path/to/db" | Path to the BadgerDB database directory          |
| `-cmd`   | "summary"    | Command to execute: 'summary' or 'view'          |
| `-prefix`| ""           | Key prefix to view (required for 'view' command) |

## Examples

1. **Basic usage**:
   ```bash
   ./badger-cli -db /var/data/myapp/db
   ```

2. **View specific prefix**:
   ```bash
   ./badger-cli -db /var/data/myapp/db -cmd view -prefix user:
   ```

## Building from Source

```bash
git clone https://github.com/yourusername/badger-cli.git
cd badger-cli
go build -o badger-cli
```

## License

MIT
