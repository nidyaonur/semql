package semql

import (
	"context"
	"crypto/tls"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// ClickHouseDB represents a ClickHouse database connection
type ClickHouseDB struct {
	conn *sql.DB
}

// NewClickHouseDB creates a new ClickHouse connection
func NewClickHouseDB(host string, port int, database, username, password string) (*ClickHouseDB, error) {
	addr := fmt.Sprintf("%s:%d", host, port)
	if port == 0 { // Default port if not specified
		addr = fmt.Sprintf("%s:9000", host) // Default ClickHouse TCP port
	}

	conn := clickhouse.OpenDB(&clickhouse.Options{
		Addr: []string{addr}, // Use provided host and port
		Auth: clickhouse.Auth{
			Database: database, // Use provided database
			Username: username, // Use provided username
			Password: password, // Use provided password
		},
		TLS: &tls.Config{
			InsecureSkipVerify: true, // Consider making this configurable
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		DialTimeout: time.Second * 30,
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		Debug:                true,
		BlockBufferSize:      10,
		MaxCompressionBuffer: 10240,
		ClientInfo: clickhouse.ClientInfo{ // optional, please see Client info section in the README.md
			Products: []struct {
				Name    string
				Version string
			}{
				{Name: "my-app", Version: "0.1"},
			},
		},
	})
	conn.SetMaxIdleConns(5)
	conn.SetMaxOpenConns(10)
	conn.SetConnMaxLifetime(time.Hour)

	// Ping to verify connection
	if err := conn.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping clickhouse: %w", err)
	}

	return &ClickHouseDB{conn: conn}, nil
}

// NewClickHouseDBWithConn creates a new ClickHouseDB instance using an existing connection
func NewClickHouseDBWithConn(conn *sql.DB) *ClickHouseDB {
	return &ClickHouseDB{conn: conn}
}

// Close closes the database connection
func (db *ClickHouseDB) Close() error {
	return db.conn.Close()
}

// GetConn returns the underlying ClickHouse connection
func (db *ClickHouseDB) GetConn() *sql.DB {
	return db.conn
}

// Execute executes a query built by QueryBuilder and returns the raw rows
func (db *ClickHouseDB) Execute(ctx context.Context, qb *QueryBuilder) (*sql.Rows, error) {
	query, args, err := qb.BuildSQL()
	if err != nil {
		return nil, fmt.Errorf("failed to build SQL: %w", err)
	}
	return db.conn.Query(query, args...)
}

// Query executes a query built by QueryBuilder and scans the results into the provided slice of structs
// The destination must be a pointer to a slice of structs
func (db *ClickHouseDB) Query(ctx context.Context, qb *QueryBuilder, dest interface{}) error {
	query, args, err := qb.BuildSQL()
	if err != nil {
		return fmt.Errorf("failed to build SQL: %w", err)
	}

	// Execute the query
	rows, err := db.conn.Query(query, args...)
	if err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	return scanRowsIntoStruct(rows, dest)
}

// scanRowsIntoStruct scans rows into a slice of structs
// The destination must be a pointer to a slice of structs
func scanRowsIntoStruct(rows *sql.Rows, dest interface{}) error {
	// Check if dest is a pointer to a slice
	destValue := reflect.ValueOf(dest)
	if destValue.Kind() != reflect.Ptr || destValue.Elem().Kind() != reflect.Slice {
		return fmt.Errorf("destination must be a pointer to a slice, got %T", dest)
	}

	// Get the slice and its element type
	sliceValue := destValue.Elem()
	elementType := sliceValue.Type().Elem()

	// Check if the slice element is a struct
	if elementType.Kind() != reflect.Struct {
		return fmt.Errorf("slice elements must be structs, got %s", elementType)
	}

	// Get column names
	columnNames, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("failed to get column names: %w", err)
	}

	// Map column names to struct field indexes
	fieldMap := make(map[string]int)
	for i := 0; i < elementType.NumField(); i++ {
		field := elementType.Field(i)

		// Check for db tag
		tag := field.Tag.Get("db")
		if tag != "" {
			if tag != "-" {
				// Split the tag to handle options like 'column_name,omitempty'
				parts := strings.Split(tag, ",")
				fieldMap[parts[0]] = i
			}
			continue
		}

		// Use field name as a fallback
		fieldMap[field.Name] = i

		// Also try lowercase field name
		fieldMap[strings.ToLower(field.Name)] = i
	}

	// Scan each row into a struct
	for rows.Next() {
		// Create a new struct instance
		newElem := reflect.New(elementType).Elem()

		// Create scan destinations
		scanDest := make([]interface{}, len(columnNames))
		for i, colName := range columnNames {
			fieldIdx, ok := fieldMap[colName]
			if !ok {
				// Try case-insensitive match as a fallback
				for fieldName, idx := range fieldMap {
					if strings.EqualFold(fieldName, colName) {
						fieldIdx = idx
						ok = true
						break
					}
				}
			}

			if ok {
				field := newElem.Field(fieldIdx)
				if field.CanAddr() {
					scanDest[i] = field.Addr().Interface()
				} else {
					return fmt.Errorf("cannot get address of field %s", colName)
				}
			} else {
				// If we don't have a corresponding field, use a placeholder
				var placeholder sql.RawBytes
				scanDest[i] = &placeholder
			}
		}

		// Scan the row
		if err := rows.Scan(scanDest...); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		// Append the struct to the result slice
		sliceValue.Set(reflect.Append(sliceValue, newElem))
	}

	return rows.Err()
}
