package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/nidyaonur/semql/pkg/semql"
)

// Example data models that can be used with the query results
type Movie struct {
	ID        int       `db:"id"`
	Name      string    `db:"name"`
	CinemaID  int       `db:"cinema_id"`
	StartDate time.Time `db:"start_date"`
	EndDate   time.Time `db:"end_date"`
	Budget    float64   `db:"budget"`
}

type Cinema struct {
	ID      int     `db:"id"`
	Name    string  `db:"name"`
	Balance float64 `db:"balance"`
}

type StatsHourly struct {
	Time     time.Time `db:"time"`
	CinemaID int       `db:"cinema_id"`
	MovieID  int       `db:"movie_id"`
	Watches  int64     `db:"sum_watch"`
	Events   int64     `db:"sum_event"`
	Revenue  float64   `db:"sum_revenue"`
}

// Time series aggregated stats
type TimeSeriesStats struct {
	TimePeriod time.Time `db:"time_period"`
	MovieName  string    `db:"movieName"`
	CinemaName string    `db:"cinemaName"`
	Watches    int64     `db:"sumWatch"`
	Events     int64     `db:"sumEvent"`
	Revenue    float64   `db:"revenue"`
	EventRate  float64   `db:"eventRate"`
}

// ExampleSetup demonstrates how to set up a schema with tables, dimensions, metrics, and relationships
func ExampleSetup() *semql.Schema {
	// Create a new schema
	schema := semql.NewSchema()

	// Get some time zones for examples
	tz, _ := time.LoadLocation("Europe/Istanbul")
	// tokyoTz, _ := time.LoadLocation("Asia/Tokyo")

	// Define the movies table (in PostgreSQL)
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
			{
				Name:        "cinemaName",
				Column:      "name",
				Type:        "String",
				Description: "Name of the cinema",
				JoinPath:    []string{"facts.cinemas"}, // This field comes from cinemas table
			},
			{
				Name:        "startDate",
				Column:      "start_date",
				Type:        "Date",
				Description: "Start date of the movie screening",
			},
			{
				Name:        "endDate",
				Column:      "end_date",
				Type:        "Date",
				Description: "End date of the movie screening",
			},
			// Time granularity dimensions that reference the time field from stats_hourly
			{
				Name:        "timeHour",
				Column:      "toStartOfHour(toTimeZone(sh.time, '{{timezone}}'))",
				Type:        "DateTime",
				Description: "Time aggregated to start of hour",
				JoinPath:    []string{"stats.stats_hourly"},
			},
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
			{
				Name:        "timeMonth",
				Column:      "toStartOfMonth(toTimeZone(sh.time, '{{timezone}}'))",
				Type:        "Date",
				Description: "Time aggregated to start of month",
				JoinPath:    []string{"stats.stats_hourly"},
			},
			{
				Name:        "timeYear",
				Column:      "toYear(toTimeZone(sh.time, '{{timezone}}'))",
				Type:        "UInt16",
				Description: "Time aggregated to year",
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
			{
				Name:        "balance",
				Column:      "balance",
				Type:        "Float64",
				Description: "Cinema balance",
				Format:      "currency",
				JoinPath:    []string{"facts.cinemas"}, // This field comes from cinemas table
			},
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
				JoinPath:    []string{"stats.stats_hourly"}, // This field comes from stats_hourly table
			},
			{
				Name:        "revenue",
				Column:      "sum_revenue",
				Type:        "Float64",
				Description: "Sum of revenue",
				Format:      "currency",
				JoinPath:    []string{"stats.stats_hourly"}, // This field comes from stats_hourly table
			},
			{
				Name:        "eventRate",
				Column:      "(SUM(sh.sum_event) / SUM(sh.sum_watch)) * 100",
				Type:        "Float64",
				Description: "Event rate (percentage)",
				Format:      "percentage",
				JoinPath:    []string{"stats.stats_hourly"}, // This expression uses stats_hourly table
			},
			{
				Name:        "revenuePerWatch",
				Column:      "SUM(sh.sum_revenue) / NULLIF(SUM(sh.sum_watch), 0)",
				Type:        "Float64",
				Description: "Revenue per watch",
				Format:      "currency",
				JoinPath:    []string{"stats.stats_hourly"}, // This expression uses stats_hourly table
			},
		},
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

	// Define the cinemas table (in PostgreSQL)
	cinemasTable := &semql.TableConfig{
		Name:  "facts.cinemas",
		Alias: "c",
		Dimensions: []*semql.DimensionField{
			{
				Name:        "cinemaId",
				Column:      "id",
				Type:        "Int32",
				Description: "Unique identifier for the cinema",
				Primary:     true,
			},
			{
				Name:        "cinemaName",
				Column:      "name",
				Type:        "String",
				Description: "Name of the cinema",
			},
		},
		Metrics: []*semql.MetricField{
			{
				Name:        "balance",
				Column:      "balance",
				Type:        "Float64",
				Description: "Cinema balance",
				Format:      "currency",
			},
		},
		Joins: []*semql.JoinConfig{},
	}

	// Define the stats_hourly table (in ClickHouse) with timezone support
	statsHourlyTable := &semql.TableConfig{
		Name:      "stats.stats_hourly",
		Alias:     "sh",
		TimeField: "time",
		TimeZone:  tz, // Set default timezone for this table to Istanbul
		Dimensions: []*semql.DimensionField{
			{
				Name:        "cinemaId",
				Column:      "cinema_id",
				Type:        "Int32",
				Description: "ID of the cinema",
			},
			{
				Name:        "movieId",
				Column:      "movie_id",
				Type:        "Int32",
				Description: "ID of the movie",
			},
			{
				Name:        "movieName",
				Column:      "name",
				Type:        "String",
				Description: "Name of the movie",
				JoinPath:    []string{"facts.movies"}, // This field comes from movies table
			},
			{
				Name:        "cinemaName",
				Column:      "name",
				Type:        "String",
				Description: "Name of the cinema",
				JoinPath:    []string{"facts.cinemas"}, // This field comes from cinemas table
			},
			// Time granularity dimensions using this table's time field directly
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
			{
				Name:        "timeWeek",
				Column:      "toMonday(toTimeZone(sh.time, '{{timezone}}'))",
				Type:        "Date",
				Description: "Time aggregated to start of week (Monday)",
			},
			{
				Name:        "timeMonth",
				Column:      "toStartOfMonth(toTimeZone(sh.time, '{{timezone}}'))",
				Type:        "Date",
				Description: "Time aggregated to start of month",
			},
			{
				Name:        "timeYear",
				Column:      "toYear(toTimeZone(sh.time, '{{timezone}}'))",
				Type:        "UInt16",
				Description: "Time aggregated to year",
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
			{
				Name:        "revenue",
				Column:      "sum_revenue",
				Type:        "Float64",
				Description: "Sum of revenue",
				Format:      "currency",
			},
			{
				Name:        "eventRate",
				Column:      "(SUM(sh.sum_event) / SUM(sh.sum_watch)) * 100",
				Type:        "Float64",
				Description: "Event rate (percentage)",
				Format:      "percentage",
			},
			{
				Name:        "revenuePerWatch",
				Column:      "SUM(sh.sum_revenue) / NULLIF(SUM(sh.sum_watch), 0)",
				Type:        "Float64",
				Description: "Revenue per watch",
				Format:      "currency",
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
			{
				Table: "facts.cinemas",
				Type:  semql.LeftJoin,
				Conditions: []semql.JoinCondition{
					{LeftField: "cinema_id", Operator: "=", RightField: "id"},
				},
			},
		},
	}

	// Add tables to the schema
	schema.AddTable(moviesTable)
	schema.AddTable(cinemasTable)
	schema.AddTable(statsHourlyTable)

	return schema
}

// ExampleQueries demonstrates how to build and execute queries using the schema
func ExampleQueries(schema *semql.Schema) {
	// Example 1: Simple query with dimensions and metrics from the movies table
	query1 := semql.NewQueryBuilder(schema, "facts.movies")
	query1.Select("movieName", "startDate", "endDate", "budget")
	sql1, args1, err1 := query1.BuildSQL()
	if err1 != nil {
		log.Fatalf("Query 1 failed: %v", err1)
	}

	fmt.Println("Example 1 - Simple Query (Movies only):")
	fmt.Println(sql1)
	fmt.Println("Args:", args1)
	fmt.Println()

	// Example 2: Query with automatic join to cinemas
	query2 := semql.NewQueryBuilder(schema, "facts.movies")
	query2.Select("movieName", "cinemaName", "budget", "balance")
	sql2, args2, err2 := query2.BuildSQL()
	if err2 != nil {
		log.Fatalf("Query 2 failed: %v", err2)
	}

	fmt.Println("Example 2 - Query with automatic cinema join:")
	fmt.Println(sql2)
	fmt.Println("Args:", args2)
	fmt.Println()

	// Example 3: Query with automatic join to stats_hourly
	query3 := semql.NewQueryBuilder(schema, "facts.movies")
	query3.Select("movieName", "cinemaName", "sumWatch", "sumEvent", "revenue", "eventRate")
	query3.Where("sumWatch", ">", 1000)
	query3.OrderBy("eventRate", "DESC")
	query3.Limit(10)
	sql3, args3, err3 := query3.BuildSQL()
	if err3 != nil {
		log.Fatalf("Query 3 failed: %v", err3)
	}

	fmt.Println("Example 3 - Query with stats_hourly metrics:")
	fmt.Println(sql3)
	fmt.Println("Args:", args3)
	fmt.Println()
}

// ExampleTimeBasedQueries demonstrates how to build and execute time-based queries with different granularities
func ExampleTimeBasedQueries(schema *semql.Schema) {
	// Example 1: Daily stats for a movie over the last 7 days
	startDate := time.Now().AddDate(0, 0, -7)
	endDate := time.Now()

	query1 := semql.NewQueryBuilder(schema, "stats.stats_hourly")
	query1.Select("movieId", "timeDay", "sumWatch", "sumEvent", "revenue", "eventRate")
	query1.Between(startDate, endDate)
	query1.Where("movieId", "=", 123)
	sql1, args1, err1 := query1.BuildSQL()
	if err1 != nil {
		log.Fatalf("Time Query 1 failed: %v", err1)
	}

	fmt.Println("Example 1 - Daily stats for a movie:")
	fmt.Println(sql1)
	fmt.Println("Args:", args1)
	fmt.Println()

	// Example 2: Monthly stats aggregated by cinema
	startDate = time.Date(time.Now().Year(), 1, 1, 0, 0, 0, 0, time.UTC)
	endDate = time.Now()

	query2 := semql.NewQueryBuilder(schema, "facts.movies")
	query2.Select("timeMonth", "cinemaName", "sumWatch", "sumEvent", "revenue", "eventRate")
	query2.Between(startDate, endDate)
	sql2, args2, err2 := query2.BuildSQL()
	if err2 != nil {
		log.Fatalf("Time Query 2 failed: %v", err2)
	}

	fmt.Println("Example 2 - Monthly stats by cinema:")
	fmt.Println(sql2)
	fmt.Println("Args:", args2)
	fmt.Println()

	// Example 3: Hourly stats for the last 24 hours
	startDate = time.Now().Add(-24 * time.Hour)
	endDate = time.Now()

	query3 := semql.NewQueryBuilder(schema, "stats.stats_hourly")
	query3.Select("timeHour", "sumWatch", "sumEvent", "eventRate")
	query3.Between(startDate, endDate)
	query3.OrderBy("timeHour", "ASC")
	sql3, args3, err3 := query3.BuildSQL()
	if err3 != nil {
		log.Fatalf("Time Query 3 failed: %v", err3)
	}

	fmt.Println("Example 3 - Hourly stats for the last 24 hours:")
	fmt.Println(sql3)
	fmt.Println("Args:", args3)
	fmt.Println()

	// Example 4: Weekly performance comparison between movies
	startDate = time.Now().AddDate(0, 0, -30) // Last 30 days
	endDate = time.Now()

	query4 := semql.NewQueryBuilder(schema, "facts.movies")
	query4.Select("timeWeek", "movieName", "sumWatch", "sumEvent", "revenue", "eventRate")
	query4.Between(startDate, endDate)
	query4.OrderBy("movieName", "ASC")
	query4.OrderBy("timeWeek", "ASC")
	sql4, args4, err4 := query4.BuildSQL()
	if err4 != nil {
		log.Fatalf("Time Query 4 failed: %v", err4)
	}

	fmt.Println("Example 4 - Weekly performance by movie:")
	fmt.Println(sql4)
	fmt.Println("Args:", args4)
	fmt.Println()
}

// ExampleExecuteTimeQuery demonstrates executing a time-based query
func ExampleExecuteTimeQuery(schema *semql.Schema) {
	// Initialize ClickHouse connection
	db, err := semql.NewClickHouseDB("localhost", 9000, "default", "default", "")
	if err != nil {
		log.Fatalf("Failed to connect to ClickHouse: %v", err)
	}
	defer db.Close()

	// Create a query for daily stats over the last 30 days
	startDate := time.Now().AddDate(0, 0, -30)
	endDate := time.Now()

	// Load Istanbul timezone as stats_hourly data is associated with it
	tz, err := time.LoadLocation("Europe/Istanbul")
	if err != nil {
		log.Fatalf("Failed to load timezone: %v", err)
	}

	query := semql.NewQueryBuilder(schema, "facts.movies")
	query.Select("movieName", "cinemaName", "timeDay", "sumWatch", "sumEvent", "revenue", "eventRate")
	query.Between(startDate, endDate)
	query.WithTimezone(tz) // Operate in Istanbul timezone context

	// Execute the query
	var results []TimeSeriesStats
	ctx := context.Background()
	err = db.Query(ctx, query, &results)
	if err != nil {
		log.Fatalf("Failed to execute time-based query: %v", err)
	}

	// Process and display results
	fmt.Println("Daily Movie Performance:")
	fmt.Println("=======================")

	for _, stat := range results {
		fmt.Printf("%s | %s | %s | Watches: %d | Events: %d | Event Rate: %.2f%%\n",
			stat.TimePeriod.Format("2006-01-02"),
			stat.CinemaName,
			stat.MovieName,
			stat.Watches,
			stat.Events,
			stat.EventRate)
	}
}

// ExampleTimezoneQueries demonstrates timezone support in queries
func ExampleTimezoneQueries(schema *semql.Schema) {
	// Load some example time zones
	tz, _ := time.LoadLocation("Europe/Istanbul")
	londonTz, _ := time.LoadLocation("Europe/London")
	tokyoTz, _ := time.LoadLocation("Asia/Tokyo")

	// Example 1: Query with Istanbul timezone
	startDate := time.Date(2025, 5, 1, 0, 0, 0, 0, tz)
	endDate := time.Date(2025, 5, 24, 23, 59, 59, 0, tz)

	query1 := semql.NewQueryBuilder(schema, "stats.stats_hourly")
	query1.Select("timeDay", "sumWatch", "sumEvent", "eventRate")
	query1.Between(startDate, endDate)
	query1.WithTimezone(tz) // Explicitly set timezone to Istanbul
	sql1, args1, err1 := query1.BuildSQL()
	if err1 != nil {
		log.Fatalf("Timezone Query 1 failed: %v", err1)
	}

	fmt.Println("Example 1 - Istanbul timezone daily stats:")
	fmt.Println(sql1)
	fmt.Println("Args:", args1)
	fmt.Println()

	// Example 2: Same date range but with London timezone
	query2 := semql.NewQueryBuilder(schema, "stats.stats_hourly")
	query2.Select("timeDay", "sumWatch", "sumEvent", "eventRate")
	query2.Between(startDate, endDate) // Same time range but will be interpreted in London time
	query2.WithTimezone(londonTz)      // Explicitly set timezone to London
	sql2, args2, err2 := query2.BuildSQL()
	if err2 != nil {
		log.Fatalf("Timezone Query 2 failed: %v", err2)
	}

	fmt.Println("Example 2 - London timezone daily stats (same date range):")
	fmt.Println(sql2)
	fmt.Println("Args:", args2)
	fmt.Println()

	// Example 3: Cross-timezone analysis with Tokyo timezone
	startOfDay := time.Date(2025, 5, 24, 0, 0, 0, 0, tokyoTz)
	endOfDay := time.Date(2025, 5, 24, 23, 59, 59, 0, tokyoTz)

	query3 := semql.NewQueryBuilder(schema, "stats.stats_hourly")
	query3.Select("timeHour", "sumWatch", "sumEvent", "revenue", "eventRate")
	query3.Between(startOfDay, endOfDay)
	query3.WithTimezone(tokyoTz)
	query3.OrderBy("timeHour", "ASC")
	sql3, args3, err3 := query3.BuildSQL()
	if err3 != nil {
		log.Fatalf("Timezone Query 3 failed: %v", err3)
	}

	fmt.Println("Example 3 - Tokyo timezone hourly stats for May 24, 2025:")
	fmt.Println(sql3)
	fmt.Println("Args:", args3)
	fmt.Println()

	// Example 4: Compare data across different timezones
	// This example shows how to run the same query with different timezones
	// for comparison purposes

	fmt.Println("Example 4 - Comparing the same daily data across different timezones:")

	// Reference time: May 24, 2025 at 12:00 UTC
	refTime := time.Date(2025, 5, 24, 12, 0, 0, 0, time.UTC)

	// Calculate local times in different timezones
	fmt.Printf("Reference time (UTC): %s\n", refTime.Format("2006-01-02 15:04:05 MST"))
	fmt.Printf("Istanbul time:        %s\n", refTime.In(tz).Format("2006-01-02 15:04:05 MST"))
	fmt.Printf("London time:          %s\n", refTime.In(londonTz).Format("2006-01-02 15:04:05 MST"))
	fmt.Printf("Tokyo time:           %s\n", refTime.In(tokyoTz).Format("2006-01-02 15:04:05 MST"))
	fmt.Println()

	// Generate queries for the full day in each timezone
	timezones := []*time.Location{time.UTC, tz, londonTz, tokyoTz}
	for _, tz := range timezones {
		// Create a query for full day in the current timezone
		dayStart := time.Date(2025, 5, 24, 0, 0, 0, 0, tz)
		dayEnd := time.Date(2025, 5, 24, 23, 59, 59, 0, tz)

		query := semql.NewQueryBuilder(schema, "stats.stats_hourly")
		query.Select("timeDay", "sumWatch", "sumEvent")
		query.Between(dayStart, dayEnd)
		query.WithTimezone(tz)
		sql, args, err := query.BuildSQL()
		if err != nil {
			log.Fatalf("Timezone Query (loop) failed for %s: %v", tz.String(), err)
		}

		fmt.Printf("Query for %s timezone:\n", tz.String())
		fmt.Println(sql)
		fmt.Println("Args:", args)
		fmt.Println()
	}
}

// ExampleCustomClickHouseConnection demonstrates how to use a custom ClickHouse connection
func ExampleCustomClickHouseConnection(schema *semql.Schema) {
	// Create a custom ClickHouse connection with specific options
	conn := clickhouse.OpenDB(&clickhouse.Options{
		Addr: []string{"127.0.0.1:9999"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
		TLS: &tls.Config{
			InsecureSkipVerify: true,
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
				{Name: "cinema-analytics", Version: "0.1"},
			},
		},
	})
	conn.SetMaxIdleConns(5)
	conn.SetMaxOpenConns(10)
	conn.SetConnMaxLifetime(time.Hour)

	// Ping to verify
	if err := conn.Ping(); err != nil {
		log.Fatalf("Failed to ping ClickHouse with custom connection: %v", err)
	}

	// Create semql.ClickHouseDB with the existing connection
	db := semql.NewClickHouseDBWithConn(conn)

	// Now use it for queries as usual
	startDate := time.Now().AddDate(0, -1, 0) // Last month
	endDate := time.Now()

	query := semql.NewQueryBuilder(schema, "stats.stats_hourly")
	query.Select("movieId", "sumWatch", "sumEvent", "eventRate")
	query.Between(startDate, endDate)
	query.Limit(10)

	// Execute the query
	var results []TimeSeriesStats
	ctx := context.Background()
	err := db.Query(ctx, query, &results)

	if err != nil {
		fmt.Printf("Query execution error: %v\n", err)
	} else {
		fmt.Printf("Successfully executed query with custom connection, got %d results\n", len(results))

		// Display a few results
		for i, stat := range results {
			if i >= 3 {
				break // Just show the first 3
			}
			fmt.Printf("%s | Watches: %d | Events: %d | Event Rate: %.2f%%\n",
				stat.TimePeriod.Format("2006-01-02"),
				stat.Watches,
				stat.Events,
				stat.EventRate)
		}
	}
}

// ExampleLastTwoDaysData demonstrates how to query data for the last 2 days with cinema name, movie name, and watches
func ExampleLastTwoDaysData(schema *semql.Schema) {
	// Calculate time range for the last 2 days in Istanbul timezone
	// Using the current date from context for consistency with example SQL
	baseDate := time.Date(2025, 5, 24, 0, 0, 0, 0, time.UTC)
	startDate := baseDate.AddDate(0, 0, -2) // 2 days ago
	endDate := baseDate                     // Today (exclusive end for < operator)

	fmt.Printf("\\n--- Example: Last 2 Days Data (From %s to %s) ---\\n",
		startDate.Format("2006-01-02"),
		endDate.Format("2006-01-02"))

	// Create a new query starting from stats_hourly table
	query := semql.NewQueryBuilder(schema, "stats.stats_hourly")

	// Select cinema name, movie name, and sum of watches
	query.Select("timeDay", "cinemaId", "movieId", "sumWatch")

	// Filter for the last 2 days
	query.Between(startDate, endDate)

	query.Where("movieId", "=", 5938) // Example movie ID

	// Sort by date descending, then by watches descending
	query.OrderBy("sumWatch", "DESC")

	// Limit to top 20 results
	query.Limit(20)

	// Build the SQL query
	sql, args, err := query.BuildSQL()
	if err != nil {
		log.Fatalf("LastTwoDaysData query failed: %v", err)
	}

	fmt.Println("Generated SQL Query:")
	fmt.Println(sql)
	fmt.Println("Args:", args)
	fmt.Println()

}

func main() {
	// Set up the schema
	schema := ExampleSetup()

	// Run example queries
	// ExampleQueries(schema)

	// Display example time-based queries
	// ExampleTimeBasedQueries(schema)

	// Display example timezone-aware queries
	// ExampleTimezoneQueries(schema)

	// Display example for last 2 days data
	ExampleLastTwoDaysData(schema)

	// Example with custom connection
	fmt.Println("\n--- Example with Custom ClickHouse Connection ---")
	fmt.Println("Note: This example will be skipped if no ClickHouse server is available")
	fmt.Println("Uncomment the ExampleCustomClickHouseConnection call in main() to try it")
	// ExampleCustomClickHouseConnection(schema) // Uncomment to try with real ClickHouse server

	fmt.Println("\nSemantic layer with timezone support and time granularity example completed.")
}
