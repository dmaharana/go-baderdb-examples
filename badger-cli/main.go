package main

import (
    "flag"
    "fmt"
    "log"
    "strings"
    "github.com/dgraph-io/badger/v3"
)

func main() {
    // Parse command line flags
    dbPath := flag.String("db", "/path/to/db", "path to the BadgerDB database directory")
    command := flag.String("cmd", "summary", "command to execute: 'summary' or 'view'")
    prefix := flag.String("prefix", "", "key prefix to view (required for 'view' command)")
    flag.Parse()

    db, err := badger.Open(badger.DefaultOptions(*dbPath).WithReadOnly(true))
    if err != nil {
        log.Fatalf("Failed to open database: %v", err)
    }
    defer db.Close()

    switch *command {
    case "summary":
        showDatabaseSummary(db)
    case "view":
        if *prefix == "" {
            log.Fatal("Please specify a prefix using -prefix flag")
        }
        viewTableContents(db, *prefix)
    default:
        log.Fatalf("Unknown command: %s. Use 'summary' or 'view'", *command)
    }
}

func showDatabaseSummary(db *badger.DB) {
    prefixes := make(map[string]int)
    
    err := db.View(func(txn *badger.Txn) error {
        opts := badger.DefaultIteratorOptions
        opts.PrefetchValues = false // Only need keys
        it := txn.NewIterator(opts)
        defer it.Close()
        
        for it.Rewind(); it.Valid(); it.Next() {
            key := string(it.Item().Key())
            
            // Extract potential table name (everything before first : or /)
            var prefix string
            if idx := strings.Index(key, ":"); idx != -1 {
                prefix = key[:idx]
            } else if idx := strings.Index(key, "/"); idx != -1 {
                prefix = key[:idx]
            } else {
                prefix = "no_prefix"
            }
            
            prefixes[prefix]++
            
            // Print first few keys to understand structure
            if prefixes[prefix] <= 3 {
                fmt.Printf("Key: %s\n", key)
            }
        }
        return nil
    })
    
    if err != nil {
        log.Fatalf("Error scanning database: %v", err)
    }
    
    fmt.Println("\nKey prefixes summary:")
    for prefix, count := range prefixes {
        fmt.Printf("%s: %d keys\n", prefix, count)
    }
}

// viewTableContents shows all key-value pairs with the given prefix
func viewTableContents(db *badger.DB, prefix string) {
    fmt.Printf("\nContents of prefix '%s':\n", prefix)
    count := 0
    
    err := db.View(func(txn *badger.Txn) error {
        opts := badger.DefaultIteratorOptions
        opts.Prefix = []byte(prefix)
        it := txn.NewIterator(opts)
        defer it.Close()
        
        for it.Rewind(); it.Valid(); it.Next() {
            item := it.Item()
            key := string(item.Key())
            val, err := item.ValueCopy(nil)
            if err != nil {
                fmt.Printf("Error reading value for key %s: %v\n", key, err)
                continue
            }
            fmt.Printf("Key: %s\nValue: %s\n\n", key, val)
            count++
        }
        return nil
    })
    
    if err != nil {
        log.Fatalf("Error reading from database: %v", err)
    }
    
    if count == 0 {
        fmt.Println("No keys found with the specified prefix")
    } else {
        fmt.Printf("Found %d keys with prefix '%s'\n", count, prefix)
    }
}
