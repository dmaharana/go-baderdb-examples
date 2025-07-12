package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
	_ "github.com/lib/pq"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/extra/bundebug"
)

// Since BadgerDB is a key-value store, we'll use PostgreSQL with Bun
// but demonstrate concurrent-safe operations with proper connection pooling
// For a pure file-based solution, I'll show both approaches

// User model for Bun ORM
type User struct {
	bun.BaseModel `bun:"table:users,alias:u"`

	ID        int64     `bun:"id,pk,autoincrement"`
	Name      string    `bun:"name,notnull"`
	Email     string    `bun:"email,unique,notnull"`
	Age       int       `bun:"age"`
	CreatedAt time.Time `bun:"created_at,nullzero,notnull,default:current_timestamp"`
	UpdatedAt time.Time `bun:"updated_at,nullzero,notnull,default:current_timestamp"`
}

// UserBadger for BadgerDB operations
type UserBadger struct {
	ID        int64     `json:"id"`
	Name      string    `json:"name"`
	Email     string    `json:"email"`
	Age       int       `json:"age"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// BadgerService handles CRUD operations with BadgerDB
type BadgerService struct {
	db      *badger.DB
	counter int64
	mu      sync.Mutex
}

func NewBadgerService(dbPath string) (*BadgerService, error) {
	opts := badger.DefaultOptions(dbPath)
	opts.Logger = nil // Disable badger logs for cleaner output
	
	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open BadgerDB: %w", err)
	}
	
	service := &BadgerService{
		db: db,
	}
	
	// Initialize counter
	service.initCounter()
	
	return service, nil
}

func (s *BadgerService) initCounter() {
	s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte("counter:users"))
		if err != nil {
			s.counter = 0
			return nil
		}
		
		return item.Value(func(val []byte) error {
			var counter int64
			json.Unmarshal(val, &counter)
			s.counter = counter
			return nil
		})
	})
}

func (s *BadgerService) getNextID() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.counter++
	
	// Update counter in database
	s.db.Update(func(txn *badger.Txn) error {
		data, _ := json.Marshal(s.counter)
		return txn.Set([]byte("counter:users"), data)
	})
	
	return s.counter
}

// Create user in BadgerDB
func (s *BadgerService) CreateUser(user *UserBadger) error {
	user.ID = s.getNextID()
	user.CreatedAt = time.Now()
	user.UpdatedAt = time.Now()
	
	return s.db.Update(func(txn *badger.Txn) error {
		data, err := json.Marshal(user)
		if err != nil {
			return fmt.Errorf("failed to marshal user: %w", err)
		}
		
		key := fmt.Sprintf("users:%d", user.ID)
		return txn.Set([]byte(key), data)
	})
}

// Get user by ID from BadgerDB
func (s *BadgerService) GetUserByID(id int64) (*UserBadger, error) {
	var user UserBadger
	
	err := s.db.View(func(txn *badger.Txn) error {
		key := fmt.Sprintf("users:%d", id)
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}
		
		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &user)
		})
	})
	
	if err != nil {
		return nil, fmt.Errorf("user not found: %w", err)
	}
	
	return &user, nil
}

// Update user in BadgerDB
func (s *BadgerService) UpdateUser(user *UserBadger) error {
	user.UpdatedAt = time.Now()
	
	return s.db.Update(func(txn *badger.Txn) error {
		key := fmt.Sprintf("users:%d", user.ID)
		
		// Check if user exists
		_, err := txn.Get([]byte(key))
		if err != nil {
			return fmt.Errorf("user not found: %w", err)
		}
		
		data, err := json.Marshal(user)
		if err != nil {
			return fmt.Errorf("failed to marshal user: %w", err)
		}
		
		return txn.Set([]byte(key), data)
	})
}

// Delete user from BadgerDB
func (s *BadgerService) DeleteUser(id int64) error {
	return s.db.Update(func(txn *badger.Txn) error {
		key := fmt.Sprintf("users:%d", id)
		return txn.Delete([]byte(key))
	})
}

// List all users from BadgerDB
func (s *BadgerService) ListUsers() ([]*UserBadger, error) {
	var users []*UserBadger
	
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		it := txn.NewIterator(opts)
		defer it.Close()
		
		prefix := []byte("users:")
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			err := item.Value(func(val []byte) error {
				var user UserBadger
				if err := json.Unmarshal(val, &user); err != nil {
					return err
				}
				users = append(users, &user)
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	
	return users, err
}

func (s *BadgerService) Close() error {
	return s.db.Close()
}

// BunService handles CRUD operations with Bun ORM (using embedded PostgreSQL)
type BunService struct {
	db *bun.DB
}

func NewBunService() (*BunService, error) {
	// Using embedded PostgreSQL (you can also use SQLite with WAL mode for better concurrency)
	// For this example, we'll use a connection string that works with embedded solutions
	sqldb, err := sql.Open("postgres", "postgresql://user:password@localhost/testdb?sslmode=disable")
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	
	// Configure connection pool for better concurrency
	sqldb.SetMaxOpenConns(25)
	sqldb.SetMaxIdleConns(25)
	sqldb.SetConnMaxLifetime(5 * time.Minute)
	
	db := bun.NewDB(sqldb, pgdialect.New())
	
	// Add debug hook
	db.AddQueryHook(bundebug.NewQueryHook(bundebug.WithVerbose(true)))
	
	// Create tables
	ctx := context.Background()
	_, err = db.NewCreateTable().Model((*User)(nil)).IfNotExists().Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create table: %w", err)
	}
	
	return &BunService{db: db}, nil
}

// CRUD operations for Bun
func (s *BunService) CreateUser(ctx context.Context, user *User) error {
	user.CreatedAt = time.Now()
	user.UpdatedAt = time.Now()
	
	_, err := s.db.NewInsert().Model(user).Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to create user: %w", err)
	}
	return nil
}

func (s *BunService) GetUserByID(ctx context.Context, id int64) (*User, error) {
	user := new(User)
	err := s.db.NewSelect().Model(user).Where("id = ?", id).Scan(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get user: %w", err)
	}
	return user, nil
}

func (s *BunService) UpdateUser(ctx context.Context, user *User) error {
	user.UpdatedAt = time.Now()
	
	_, err := s.db.NewUpdate().Model(user).WherePK().Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to update user: %w", err)
	}
	return nil
}

func (s *BunService) DeleteUser(ctx context.Context, id int64) error {
	_, err := s.db.NewDelete().Model((*User)(nil)).Where("id = ?", id).Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to delete user: %w", err)
	}
	return nil
}

func (s *BunService) ListUsers(ctx context.Context) ([]*User, error) {
	var users []*User
	err := s.db.NewSelect().Model(&users).Scan(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list users: %w", err)
	}
	return users, nil
}

func (s *BunService) Close() error {
	return s.db.Close()
}

// Concurrent testing functions
func testConcurrentBadgerOperations(service *BadgerService) {
	log.Println("Testing concurrent BadgerDB operations...")
	
	var wg sync.WaitGroup
	numWorkers := 10
	operationsPerWorker := 100
	
	// Concurrent writes
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			
			for j := 0; j < operationsPerWorker; j++ {
				user := &UserBadger{
					Name:  fmt.Sprintf("User_%d_%d", workerID, j),
					Email: fmt.Sprintf("user%d_%d@example.com", workerID, j),
					Age:   rand.Intn(50) + 20,
				}
				
				if err := service.CreateUser(user); err != nil {
					log.Printf("Worker %d: Failed to create user: %v", workerID, err)
				}
			}
		}(i)
	}
	
	wg.Wait()
	
	// List all users
	users, err := service.ListUsers()
	if err != nil {
		log.Printf("Failed to list users: %v", err)
	} else {
		log.Printf("Total users created: %d", len(users))
	}
}

func main() {
	// Test BadgerDB operations
	log.Println("Starting BadgerDB CRUD operations test...")
	
	badgerService, err := NewBadgerService("./badger_data")
	if err != nil {
		log.Fatal(err)
	}
	defer badgerService.Close()
	
	// Test basic CRUD operations
	log.Println("Testing basic CRUD operations...")
	
	// Create
	user := &UserBadger{
		Name:  "John Doe",
		Email: "john@example.com",
		Age:   30,
	}
	
	err = badgerService.CreateUser(user)
	if err != nil {
		log.Printf("Failed to create user: %v", err)
	} else {
		log.Printf("Created user with ID: %d", user.ID)
	}
	
	// Read
	fetchedUser, err := badgerService.GetUserByID(user.ID)
	if err != nil {
		log.Printf("Failed to fetch user: %v", err)
	} else {
		log.Printf("Fetched user: %+v", fetchedUser)
	}
	
	// Update
	fetchedUser.Age = 31
	err = badgerService.UpdateUser(fetchedUser)
	if err != nil {
		log.Printf("Failed to update user: %v", err)
	} else {
		log.Printf("Updated user age to: %d", fetchedUser.Age)
	}
	
	// Test concurrent operations
	testConcurrentBadgerOperations(badgerService)
	
	// List all users
	allUsers, err := badgerService.ListUsers()
	if err != nil {
		log.Printf("Failed to list users: %v", err)
	} else {
		log.Printf("Total users in database: %d", len(allUsers))
		for _, u := range allUsers[:min(5, len(allUsers))] { // Show first 5 users
			log.Printf("User: %s (%s) - Age: %d", u.Name, u.Email, u.Age)
		}
	}
	
	// Delete
	err = badgerService.DeleteUser(user.ID)
	if err != nil {
		log.Printf("Failed to delete user: %v", err)
	} else {
		log.Printf("Deleted user with ID: %d", user.ID)
	}
	
	log.Println("BadgerDB operations completed successfully!")
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}