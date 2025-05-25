# SemQL: Semantic Query Layer for ClickHouse

SemQL is a lightweight semantic layer for ClickHouse that helps you define table relationships, dimensions, and metrics, and then automatically generates optimized SQL queries with proper joins based on the fields you select.

## Features

- **Schema Definition**: Define tables, dimensions, metrics, and relationships in a structured way
- **Automatic Joins**: The query builder automatically adds necessary joins based on selected fields using JoinPath
- **Timezone Support**: Built-in timezone handling for time-based queries with timezone conversion
- **Time Granularity**: Support for different time granularities (hour, day, week, month, year)
- **Calculated Metrics**: Support for calculated metrics with custom SQL expressions
- **Filtering & Pagination**: Easy-to-use filtering, sorting, and pagination with Between, Where, OrderBy, and Limit
- **Type-Safe Results**: Results are mapped to Go structs with proper field types
- **Custom Connections**: Support for custom ClickHouse connections with advanced configuration

## Installation

```bash
go get github.com/nidyaonur/semql
```

## Quick Start

Here's a simple example of how to use SemQL:

```go
package main

import (
	"context"
	"fmt"
	"time"
	"github.com/nidyaonur/semql/pkg/semql"
)

// Define your result struct
type MovieStats struct {
	TimePeriod  time.Time `db:"time_period"`
	MovieName   string    `db:"movieName"`
	CinemaName  string    `db:"cinemaName"`
	Watches     int64     `db:"sumWatch"`
	Events      int64     `db:"sumEvent"`
	Revenue     float64   `db:"revenue"`
	EventRate   float64   `db:"eventRate"`
}

func main() {
	// Set up a schema with tables, dimensions, metrics, and relationships
	schema := setupSchema()
	
	// Create a query builder starting from movies table
	query := semql.NewQueryBuilder(schema, "facts.movies")
	
	// Define what we want in our query - automatic joins will be added
	query.Select("movieName", "cinemaName", "sumWatch", "sumEvent", "revenue", "eventRate")
	query.Where("sumWatch", ">", 1000)
	query.OrderBy("eventRate", "DESC")
	query.Limit(10)
	
	// Get the SQL query (for debugging or direct use)
	sql, args, err := query.BuildSQL()
	if err != nil {
		panic(err)
	}
	fmt.Println(sql)
	
	// Or execute it directly with the ClickHouse driver
	db, _ := semql.NewClickHouseDB("localhost", 9000, "default", "default", "")
	defer db.Close()
	
	var results []MovieStats
	if err := db.Query(context.Background(), query, &results); err != nil {
		panic(err)
	}
	
	// Process results
	for _, stat := range results {
		fmt.Printf("%s (at %s): %.2f%% Event Rate\n", 
			stat.MovieName, stat.CinemaName, stat.EventRate)
	}
}
```

## Schema Definition

Define your tables, dimensions, metrics, and relationships using JoinPath for cross-table fields:

```go
func setupSchema() *semql.Schema {
	schema := semql.NewSchema()
	
	// Load timezone for time-based operations
	tz, _ := time.LoadLocation("Europe/Istanbul")
	
	// Define the movies table
	moviesTable := &semql.TableConfig{
		Name:  "facts.movies",
		Alias: "m",
		Dimensions: []*semql.DimensionField{
			{
				Name:        "movieId",
				Column:      "id",
				Type:        "Int32",
				Description: "Unique identifier for the movie",
				Primary:     true,
			},
			{
				Name:        "movieName",
				Column:      "name",
				Type:        "String",
				Description: "Name of the movie",
			},
			{
				Name:        "cinemaId",
				Column:      "cinema_id",
				Type:        "Int32",
				Description: "ID of the cinema showing the movie",
			},
			// Cross-table dimension using JoinPath
			{
				Name:        "cinemaName",
				Column:      "name",
				Type:        "String",
				Description: "Name of the cinema",
				JoinPath:    []string{"facts.cinemas"}, // This field comes from cinemas table
			},
			// Time granularity dimensions with timezone support
			{
				Name:        "timeDay",
				Column:      "toStartOfDay(toTimeZone(sh.time, '{{timezone}}'))",
				Type:        "Date",
				Description: "Time aggregated to start of day",
				JoinPath:    []string{"stats.stats_hourly"},
			},
			{
				Name:        "timeWeek",
				Column:      "toMonday(toTimeZone(sh.time, '{{timezone}}'))",
				Type:        "Date",
				Description: "Time aggregated to start of week (Monday)",
				JoinPath:    []string{"stats.stats_hourly"},
			},
		},
		Metrics: []*semql.MetricField{
			{
				Name:        "budget",
				Column:      "budget",
				Type:        "Float64",
				Description: "Movie production budget",
				Format:      "currency",
			},
			// Cross-table metrics using JoinPath
			{
				Name:        "sumWatch",
				Column:      "sum_watch",
				Type:        "Int64",
				Description: "Sum of movie watches",
				JoinPath:    []string{"stats.stats_hourly"}, // This field comes from stats_hourly table
			},
			{
				Name:        "sumEvent",
				Column:      "sum_event",
				Type:        "Int64",
				Description: "Sum of events during movie",
				JoinPath:    []string{"stats.stats_hourly"},
			},
			// Calculated metrics with custom expressions
			{
				Name:        "eventRate",
				Column:      "(SUM(sh.sum_event) / SUM(sh.sum_watch)) * 100",
				Type:        "Float64",
				Description: "Event rate (percentage)",
				Format:      "percentage",
				JoinPath:    []string{"stats.stats_hourly"},
			},
		},
		// Define table relationships
		Joins: []*semql.JoinConfig{
			{
				Table: "facts.cinemas",
				Type:  semql.LeftJoin,
				Conditions: []semql.JoinCondition{
					{LeftField: "cinema_id", Operator: "=", RightField: "id"},
				},
			},
			{
				Table: "stats.stats_hourly",
				Type:  semql.LeftJoin,
				Conditions: []semql.JoinCondition{
					{LeftField: "id", Operator: "=", RightField: "movie_id"},
				},
			},
		},
	}
	
	// Define the stats_hourly table with timezone support
	statsHourlyTable := &semql.TableConfig{
		Name:      "stats.stats_hourly",
		Alias:     "sh",
		TimeField: "time",
		TimeZone:  tz, // Set default timezone for this table
		Dimensions: []*semql.DimensionField{
			{
				Name:        "movieId",
				Column:      "movie_id",
				Type:        "Int32",
				Description: "ID of the movie",
			},
			// Time granularity dimensions
			{
				Name:        "timeHour",
				Column:      "toStartOfHour(toTimeZone(sh.time, '{{timezone}}'))",
				Type:        "DateTime",
				Description: "Time aggregated to start of hour",
			},
			{
				Name:        "timeDay",
				Column:      "toStartOfDay(toTimeZone(sh.time, '{{timezone}}'))",
				Type:        "Date",
				Description: "Time aggregated to start of day",
			},
		},
		Metrics: []*semql.MetricField{
			{
				Name:        "sumWatch",
				Column:      "sum_watch",
				Type:        "Int64",
				Description: "Sum of movie watches",
			},
			{
				Name:        "sumEvent",
				Column:      "sum_event",
				Type:        "Int64",
				Description: "Sum of events during movie",
			},
		},
		Joins: []*semql.JoinConfig{
			{
				Table: "facts.movies",
				Type:  semql.LeftJoin,
				Conditions: []semql.JoinCondition{
					{LeftField: "movie_id", Operator: "=", RightField: "id"},
				},
			},
		},
	}
	
	// Add tables to the schema
	schema.AddTable(moviesTable)
	schema.AddTable(statsHourlyTable)
	
	return schema
}
```

## Time-Based Queries with Timezone Support

SemQL provides robust support for time-based queries with different granularities and timezone handling:

```go
// Daily stats for the last 7 days
startDate := time.Now().AddDate(0, 0, -7)
endDate := time.Now()

query := semql.NewQueryBuilder(schema, "stats.stats_hourly")
query.Select("movieId", "timeDay", "sumWatch", "sumEvent", "revenue", "eventRate")
query.Between(startDate, endDate)
query.Where("movieId", "=", 123)

// Monthly stats with timezone
tz, _ := time.LoadLocation("Europe/Istanbul")
query2 := semql.NewQueryBuilder(schema, "facts.movies")
query2.Select("timeMonth", "cinemaName", "sumWatch", "sumEvent", "revenue", "eventRate")
query2.Between(startDate, endDate)
query2.WithTimezone(tz) // Operate in Istanbul timezone context

// Hourly stats for the last 24 hours
startDate = time.Now().Add(-24 * time.Hour)
endDate = time.Now()

query3 := semql.NewQueryBuilder(schema, "stats.stats_hourly")
query3.Select("timeHour", "sumWatch", "sumEvent", "eventRate")
query3.Between(startDate, endDate)
query3.OrderBy("timeHour", "ASC")
```

## Query Building Features

SemQL provides a fluent API for building complex queries:

```go
// Basic query with automatic joins
query := semql.NewQueryBuilder(schema, "facts.movies")
query.Select("movieName", "cinemaName", "sumWatch", "eventRate")
// Automatically joins cinemas table for cinemaName and stats_hourly for sumWatch

// Query with filtering and sorting
query := semql.NewQueryBuilder(schema, "facts.movies")
query.Select("movieName", "cinemaName", "sumWatch", "sumEvent", "revenue", "eventRate")
query.Where("sumWatch", ">", 1000)
query.OrderBy("eventRate", "DESC")
query.Limit(10)

// Time range queries
startDate := time.Now().AddDate(0, 0, -30)
endDate := time.Now()

query := semql.NewQueryBuilder(schema, "stats.stats_hourly")
query.Select("timeDay", "movieName", "sumWatch", "sumEvent")
query.Between(startDate, endDate)
query.Where("movieId", "=", 5938)
query.OrderBy("sumWatch", "DESC")
query.Limit(20)
```

## Custom ClickHouse Connection

You can use custom ClickHouse connections with advanced configuration:

```go
import "github.com/ClickHouse/clickhouse-go/v2"

// Create a custom ClickHouse connection
conn := clickhouse.OpenDB(&clickhouse.Options{
	Addr: []string{"127.0.0.1:9000"},
	Auth: clickhouse.Auth{
		Database: "default",
		Username: "default",
		Password: "",
	},
	Settings: clickhouse.Settings{
		"max_execution_time": 60,
	},
	DialTimeout: time.Second * 30,
	Compression: &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	},
})

// Create semql.ClickHouseDB with the existing connection
db := semql.NewClickHouseDBWithConn(conn)

// Use it for queries as usual
query := semql.NewQueryBuilder(schema, "stats.stats_hourly")
query.Select("movieId", "sumWatch", "sumEvent", "eventRate")
query.Limit(10)

var results []MovieStats
err := db.Query(context.Background(), query, &results)
```

## Working with Results

Map query results to Go structs with appropriate db tags:

```go
// Define your result struct
type MovieStats struct {
	TimePeriod  time.Time `db:"time_period"`
	MovieName   string    `db:"movieName"`
	CinemaName  string    `db:"cinemaName"`
	Watches     int64     `db:"sumWatch"`
	Events      int64     `db:"sumEvent"`
	Revenue     float64   `db:"revenue"`
	EventRate   float64   `db:"eventRate"`
}

// Execute the query and scan results
var stats []MovieStats
if err := db.Query(ctx, query, &stats); err != nil {
	// handle error
}

// Process the results
for _, stat := range stats {
	fmt.Printf("%s | %s | %s | Watches: %d | Events: %d | Event Rate: %.2f%%\n",
		stat.TimePeriod.Format("2006-01-02"),
		stat.CinemaName,
		stat.MovieName,
		stat.Watches,
		stat.Events,
		stat.EventRate)
}
```

## Key Concepts

### JoinPath
Use `JoinPath` in dimension and metric definitions to specify which table a field comes from. SemQL automatically adds the necessary joins.

### Time Granularity
Built-in support for different time granularities with timezone conversion:
- `timeHour`: Hourly aggregation
- `timeDay`: Daily aggregation  
- `timeWeek`: Weekly aggregation (Monday start)
- `timeMonth`: Monthly aggregation
- `timeYear`: Yearly aggregation

### Timezone Support
- Set default timezone per table with `TimeZone`
- Override timezone per query with `WithTimezone()`
- Automatic timezone conversion in time dimension columns

### Automatic Joins
When you select fields that require joins (indicated by `JoinPath`), SemQL automatically adds the necessary JOIN clauses to your query.

## License

This project is licensed under the Apache License 2.0 - see the LICENSE file for details.