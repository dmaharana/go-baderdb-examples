package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// User represents a user entity
type User struct {
	ID        int64     `json:"id"`
	Name      string    `json:"name"`
	Email     string    `json:"email"`
	CompanyID int64     `json:"company_id"`
	CreatedAt time.Time `json:"created_at"`
}

// Company represents a company entity
type Company struct {
	ID        int64     `json:"id"`
	Name      string    `json:"name"`
	Industry  string    `json:"industry"`
	CreatedAt time.Time `json:"created_at"`
}

// Order represents an order entity
type Order struct {
	ID        int64     `json:"id"`
	UserID    int64     `json:"user_id"`
	ProductID int64     `json:"product_id"`
	Quantity  int       `json:"quantity"`
	Amount    float64   `json:"amount"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
}

// Product represents a product entity
type Product struct {
	ID          int64   `json:"id"`
	Name        string  `json:"name"`
	Price       float64 `json:"price"`
	CategoryID  int64   `json:"category_id"`
	CompanyID   int64   `json:"company_id"`
	Description string  `json:"description"`
}

// Category represents a product category
type Category struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
}

// Joined result structures
type UserWithCompany struct {
	User    User    `json:"user"`
	Company Company `json:"company"`
}

type OrderWithDetails struct {
	Order    Order   `json:"order"`
	User     User    `json:"user"`
	Product  Product `json:"product"`
	Category Category `json:"category"`
}

type CompanyStats struct {
	Company     Company `json:"company"`
	UserCount   int     `json:"user_count"`
	OrderCount  int     `json:"order_count"`
	TotalRevenue float64 `json:"total_revenue"`
}

// BadgerService handles all database operations
type BadgerService struct {
	db       *badger.DB
	counters map[string]int64
	mu       sync.RWMutex
}

func NewBadgerService(dbPath string) (*BadgerService, error) {
	opts := badger.DefaultOptions(dbPath)
	opts.Logger = nil
	
	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open BadgerDB: %w", err)
	}
	
	service := &BadgerService{
		db:       db,
		counters: make(map[string]int64),
	}
	
	// Initialize counters
	service.initCounters()
	
	return service, nil
}

func (s *BadgerService) initCounters() {
	entities := []string{"users", "companies", "orders", "products", "categories"}
	
	for _, entity := range entities {
		s.db.View(func(txn *badger.Txn) error {
			key := fmt.Sprintf("counter:%s", entity)
			item, err := txn.Get([]byte(key))
			if err != nil {
				s.counters[entity] = 0
				return nil
			}
			
			return item.Value(func(val []byte) error {
				var counter int64
				json.Unmarshal(val, &counter)
				s.counters[entity] = counter
				return nil
			})
		})
	}
}

func (s *BadgerService) getNextID(entity string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	s.counters[entity]++
	
	// Update counter in database
	s.db.Update(func(txn *badger.Txn) error {
		key := fmt.Sprintf("counter:%s", entity)
		data, _ := json.Marshal(s.counters[entity])
		return txn.Set([]byte(key), data)
	})
	
	return s.counters[entity]
}

// Generic CRUD operations
func (s *BadgerService) create(entity string, id int64, data interface{}) error {
	return s.db.Update(func(txn *badger.Txn) error {
		jsonData, err := json.Marshal(data)
		if err != nil {
			return err
		}
		
		key := fmt.Sprintf("%s:%d", entity, id)
		return txn.Set([]byte(key), jsonData)
	})
}

func (s *BadgerService) get(entity string, id int64, result interface{}) error {
	return s.db.View(func(txn *badger.Txn) error {
		key := fmt.Sprintf("%s:%d", entity, id)
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}
		
		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, result)
		})
	})
}

func (s *BadgerService) list(entity string, result interface{}) error {
	return s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		it := txn.NewIterator(opts)
		defer it.Close()
		
		prefix := []byte(fmt.Sprintf("%s:", entity))
		items := []json.RawMessage{}
		
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			err := item.Value(func(val []byte) error {
				items = append(items, json.RawMessage(val))
				return nil
			})
			if err != nil {
				return err
			}
		}
		
		// Convert to the expected slice type
		jsonData, err := json.Marshal(items)
		if err != nil {
			return err
		}
		
		return json.Unmarshal(jsonData, result)
	})
}

// Entity-specific operations
func (s *BadgerService) CreateUser(user *User) error {
	user.ID = s.getNextID("users")
	user.CreatedAt = time.Now()
	return s.create("users", user.ID, user)
}

func (s *BadgerService) CreateCompany(company *Company) error {
	company.ID = s.getNextID("companies")
	company.CreatedAt = time.Now()
	return s.create("companies", company.ID, company)
}

func (s *BadgerService) CreateOrder(order *Order) error {
	order.ID = s.getNextID("orders")
	order.CreatedAt = time.Now()
	return s.create("orders", order.ID, order)
}

func (s *BadgerService) CreateProduct(product *Product) error {
	product.ID = s.getNextID("products")
	return s.create("products", product.ID, product)
}

func (s *BadgerService) CreateCategory(category *Category) error {
	category.ID = s.getNextID("categories")
	return s.create("categories", category.ID, category)
}

// Join-like operations

// 1. Simple 1:1 Join - Users with their Companies
func (s *BadgerService) GetUsersWithCompanies() ([]UserWithCompany, error) {
	var users []User
	err := s.list("users", &users)
	if err != nil {
		return nil, err
	}
	
	var results []UserWithCompany
	
	for _, user := range users {
		var company Company
		err := s.get("companies", user.CompanyID, &company)
		if err != nil {
			continue // Skip if company not found
		}
		
		results = append(results, UserWithCompany{
			User:    user,
			Company: company,
		})
	}
	
	return results, nil
}

// 2. Complex Multi-table Join - Orders with User, Product, and Category details
func (s *BadgerService) GetOrdersWithDetails() ([]OrderWithDetails, error) {
	var orders []Order
	err := s.list("orders", &orders)
	if err != nil {
		return nil, err
	}
	
	var results []OrderWithDetails
	
	for _, order := range orders {
		var user User
		var product Product
		var category Category
		
		// Get user
		if err := s.get("users", order.UserID, &user); err != nil {
			continue
		}
		
		// Get product
		if err := s.get("products", order.ProductID, &product); err != nil {
			continue
		}
		
		// Get category
		if err := s.get("categories", product.CategoryID, &category); err != nil {
			continue
		}
		
		results = append(results, OrderWithDetails{
			Order:    order,
			User:     user,
			Product:  product,
			Category: category,
		})
	}
	
	return results, nil
}

// 3. Aggregation with Grouping - Company statistics
func (s *BadgerService) GetCompanyStats() ([]CompanyStats, error) {
	var companies []Company
	err := s.list("companies", &companies)
	if err != nil {
		return nil, err
	}
	
	var users []User
	err = s.list("users", &users)
	if err != nil {
		return nil, err
	}
	
	var orders []Order
	err = s.list("orders", &orders)
	if err != nil {
		return nil, err
	}
	
	// Group users by company
	usersByCompany := make(map[int64][]User)
	for _, user := range users {
		usersByCompany[user.CompanyID] = append(usersByCompany[user.CompanyID], user)
	}
	
	// Group orders by company (through users)
	ordersByCompany := make(map[int64][]Order)
	for _, order := range orders {
		for _, user := range users {
			if user.ID == order.UserID {
				ordersByCompany[user.CompanyID] = append(ordersByCompany[user.CompanyID], order)
				break
			}
		}
	}
	
	var results []CompanyStats
	for _, company := range companies {
		stats := CompanyStats{
			Company:   company,
			UserCount: len(usersByCompany[company.ID]),
		}
		
		// Calculate order count and total revenue
		companyOrders := ordersByCompany[company.ID]
		stats.OrderCount = len(companyOrders)
		
		for _, order := range companyOrders {
			stats.TotalRevenue += order.Amount
		}
		
		results = append(results, stats)
	}
	
	return results, nil
}

// 4. Filtered Join - Get orders for a specific user with product details
func (s *BadgerService) GetUserOrdersWithProducts(userID int64) ([]OrderWithDetails, error) {
	var orders []Order
	err := s.list("orders", &orders)
	if err != nil {
		return nil, err
	}
	
	var results []OrderWithDetails
	
	// Get user once
	var user User
	if err := s.get("users", userID, &user); err != nil {
		return nil, fmt.Errorf("user not found: %w", err)
	}
	
	for _, order := range orders {
		if order.UserID != userID {
			continue
		}
		
		var product Product
		var category Category
		
		// Get product
		if err := s.get("products", order.ProductID, &product); err != nil {
			continue
		}
		
		// Get category
		if err := s.get("categories", product.CategoryID, &category); err != nil {
			continue
		}
		
		results = append(results, OrderWithDetails{
			Order:    order,
			User:     user,
			Product:  product,
			Category: category,
		})
	}
	
	return results, nil
}

// 5. Advanced query - Top selling products by category
func (s *BadgerService) GetTopSellingProductsByCategory() (map[string][]struct {
	Product     Product `json:"product"`
	TotalOrders int     `json:"total_orders"`
	TotalRevenue float64 `json:"total_revenue"`
}, error) {
	var orders []Order
	err := s.list("orders", &orders)
	if err != nil {
		return nil, err
	}
	
	var products []Product
	err = s.list("products", &products)
	if err != nil {
		return nil, err
	}
	
	var categories []Category
	err = s.list("categories", &categories)
	if err != nil {
		return nil, err
	}
	
	// Create lookup maps
	productMap := make(map[int64]Product)
	for _, product := range products {
		productMap[product.ID] = product
	}
	
	categoryMap := make(map[int64]Category)
	for _, category := range categories {
		categoryMap[category.ID] = category
	}
	
	// Aggregate orders by product
	productStats := make(map[int64]struct {
		Product      Product
		TotalOrders  int
		TotalRevenue float64
	})
	
	for _, order := range orders {
		product, exists := productMap[order.ProductID]
		if !exists {
			continue
		}
		
		stats := productStats[product.ID]
		stats.Product = product
		stats.TotalOrders++
		stats.TotalRevenue += order.Amount
		productStats[product.ID] = stats
	}
	
	// Group by category
	result := make(map[string][]struct {
		Product     Product `json:"product"`
		TotalOrders int     `json:"total_orders"`
		TotalRevenue float64 `json:"total_revenue"`
	})
	
	for _, stats := range productStats {
		category := categoryMap[stats.Product.CategoryID]
		categoryName := category.Name
		
		result[categoryName] = append(result[categoryName], struct {
			Product     Product `json:"product"`
			TotalOrders int     `json:"total_orders"`
			TotalRevenue float64 `json:"total_revenue"`
		}{
			Product:     stats.Product,
			TotalOrders: stats.TotalOrders,
			TotalRevenue: stats.TotalRevenue,
		})
	}
	
	// Sort by total revenue within each category
	for categoryName := range result {
		sort.Slice(result[categoryName], func(i, j int) bool {
			return result[categoryName][i].TotalRevenue > result[categoryName][j].TotalRevenue
		})
	}
	
	return result, nil
}

func (s *BadgerService) Close() error {
	return s.db.Close()
}

// Demo functions
func setupTestData(service *BadgerService) {
	// Create categories
	categories := []Category{
		{Name: "Electronics"},
		{Name: "Books"},
		{Name: "Clothing"},
	}
	
	for _, category := range categories {
		service.CreateCategory(&category)
	}
	
	// Create companies
	companies := []Company{
		{Name: "Tech Corp", Industry: "Technology"},
		{Name: "Fashion Ltd", Industry: "Fashion"},
		{Name: "Book Store Inc", Industry: "Retail"},
	}
	
	for _, company := range companies {
		service.CreateCompany(&company)
	}
	
	// Create users
	users := []User{
		{Name: "Alice Smith", Email: "alice@example.com", CompanyID: 1},
		{Name: "Bob Johnson", Email: "bob@example.com", CompanyID: 2},
		{Name: "Charlie Brown", Email: "charlie@example.com", CompanyID: 1},
	}
	
	for _, user := range users {
		service.CreateUser(&user)
	}
	
	// Create products
	products := []Product{
		{Name: "Laptop", Price: 999.99, CategoryID: 1, CompanyID: 1, Description: "High-performance laptop"},
		{Name: "Programming Book", Price: 49.99, CategoryID: 2, CompanyID: 3, Description: "Learn Go programming"},
		{Name: "T-Shirt", Price: 19.99, CategoryID: 3, CompanyID: 2, Description: "Cotton t-shirt"},
	}
	
	for _, product := range products {
		service.CreateProduct(&product)
	}
	
	// Create orders
	orders := []Order{
		{UserID: 1, ProductID: 1, Quantity: 1, Amount: 999.99, Status: "completed"},
		{UserID: 2, ProductID: 3, Quantity: 2, Amount: 39.98, Status: "completed"},
		{UserID: 1, ProductID: 2, Quantity: 1, Amount: 49.99, Status: "pending"},
		{UserID: 3, ProductID: 1, Quantity: 1, Amount: 999.99, Status: "completed"},
	}
	
	for _, order := range orders {
		service.CreateOrder(&order)
	}
}

func main() {
	service, err := NewBadgerService("./multi_table_data")
	if err != nil {
		log.Fatal(err)
	}
	defer service.Close()
	
	// Setup test data
	log.Println("Setting up test data...")
	setupTestData(service)
	
	// Demo 1: Users with Companies
	log.Println("\n=== Users with Companies ===")
	usersWithCompanies, err := service.GetUsersWithCompanies()
	if err != nil {
		log.Printf("Error: %v", err)
	} else {
		for _, uc := range usersWithCompanies {
			log.Printf("User: %s works at %s (%s)", uc.User.Name, uc.Company.Name, uc.Company.Industry)
		}
	}
	
	// Demo 2: Orders with full details
	log.Println("\n=== Orders with Details ===")
	orderDetails, err := service.GetOrdersWithDetails()
	if err != nil {
		log.Printf("Error: %v", err)
	} else {
		for _, od := range orderDetails {
			log.Printf("Order #%d: %s bought %s (%s) - $%.2f [%s]",
				od.Order.ID, od.User.Name, od.Product.Name, od.Category.Name, od.Order.Amount, od.Order.Status)
		}
	}
	
	// Demo 3: Company statistics
	log.Println("\n=== Company Statistics ===")
	companyStats, err := service.GetCompanyStats()
	if err != nil {
		log.Printf("Error: %v", err)
	} else {
		for _, stats := range companyStats {
			log.Printf("Company: %s | Users: %d | Orders: %d | Revenue: $%.2f",
				stats.Company.Name, stats.UserCount, stats.OrderCount, stats.TotalRevenue)
		}
	}
	
	// Demo 4: User-specific orders
	log.Println("\n=== Alice's Orders ===")
	aliceOrders, err := service.GetUserOrdersWithProducts(1)
	if err != nil {
		log.Printf("Error: %v", err)
	} else {
		for _, order := range aliceOrders {
			log.Printf("Alice ordered: %s - $%.2f [%s]",
				order.Product.Name, order.Order.Amount, order.Order.Status)
		}
	}
	
	// Demo 5: Top selling products by category
	log.Println("\n=== Top Selling Products by Category ===")
	topProducts, err := service.GetTopSellingProductsByCategory()
	if err != nil {
		log.Printf("Error: %v", err)
	} else {
		for categoryName, products := range topProducts {
			log.Printf("Category: %s", categoryName)
			for _, product := range products {
				log.Printf("  - %s: %d orders, $%.2f revenue",
					product.Product.Name, product.TotalOrders, product.TotalRevenue)
			}
		}
	}
}
