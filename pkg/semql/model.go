package semql

import (
	"time"
)

// TableConfig defines the configuration for a table in the semantic layer
type TableConfig struct {
	Name       string            // Physical table name in ClickHouse (can include database prefix db.table)
	Alias      string            // Optional alias for the table
	Dimensions []*DimensionField // List of dimension fields
	Metrics    []*MetricField    // List of metric fields
	Joins      []*JoinConfig     // List of joins with other tables
	TimeField  string            // Name of the time field in the table (for time-based queries)
	TimeZone   *time.Location    // Default timezone for time-based operations on this table
}

// DimensionField represents a dimension in a table
type DimensionField struct {
	Name        string   // Dimension name (used as alias)
	Column      string   // Physical column name or SQL expression
	Type        string   // Data type
	Description string   // Optional description
	Primary     bool     // Whether this is a primary key
	JoinPath    []string // Join path to reach this field (e.g., ["advertisers"] or ["campaigns", "advertisers"])
}

// MetricField represents a metric in a table
type MetricField struct {
	Name        string   // Metric name (used as alias)
	Column      string   // Physical column name or SQL expression
	Type        string   // Data type
	Description string   // Optional description
	Format      string   // Optional formatting hint
	JoinPath    []string // Join path to reach this field (e.g., ["advertisers"] or ["campaigns", "advertisers"])
}

// JoinConfig defines how tables are joined
type JoinConfig struct {
	Table      string   // Reference to another table name
	Type       JoinType // Type of join (LEFT, INNER, etc.)
	Conditions []JoinCondition
}

// JoinCondition defines a join condition between two tables
type JoinCondition struct {
	LeftField  string // Field from left table
	RightField string // Field from right table
	Operator   string // Operator for join (default "=")
}

// JoinType represents the type of SQL join
type JoinType string

// Join type constants
const (
	InnerJoin JoinType = "INNER JOIN"
	LeftJoin  JoinType = "LEFT JOIN"
	RightJoin JoinType = "RIGHT JOIN"
	FullJoin  JoinType = "FULL JOIN"
)

// Schema holds the entire schema configuration with all tables
type Schema struct {
	Tables map[string]*TableConfig
}

// NewSchema creates a new empty schema
func NewSchema() *Schema {
	return &Schema{
		Tables: make(map[string]*TableConfig),
	}
}

// AddTable adds a table configuration to the schema
func (s *Schema) AddTable(table *TableConfig) {
	s.Tables[table.Name] = table
}
