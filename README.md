# SemQL: Semantic Query Layer for ClickHouse

SemQL is a lightweight semantic layer for ClickHouse that helps you define table relationships, dimensions, and metrics, and then automatically generates optimized SQL queries with proper joins based on the fields you select.

## Features

- **Schema Definition**: Define tables, dimensions, metrics, and relationships in a structured way
- **Automatic Joins**: The query builder automatically adds necessary joins based on selected fields
- **Calculated Metrics**: Support for calculated metrics with custom SQL expressions
- **Filtering & Pagination**: Easy-to-use filtering, sorting, and pagination
- **Type-Safe Results**: Results are mapped to Go structs with proper field types
- **Simplified API**: Fluent API for building complex queries with minimal code

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
	"github.com/nidyaonur/semql/pkg/semql"
)

func main() {
	// Set up a schema with tables, dimensions, metrics, and relationships
	schema := setupSchema()
	
	// Create a query builder
	query := semql.NewQueryBuilder(schema, "campaigns")
	
	// Define what we want in our query
	query.Select("campaignName", "advertiserName", "impressions", "clicks", "ctr")
	      .Where("startDate", ">=", "2023-01-01")
	      .OrderBy("ctr", "DESC")
	      .Limit(10)
	
	// Get the SQL query (for debugging or direct use)
	sql, args := query.BuildSQL()
	fmt.Println(sql)
	
	// Or execute it directly with the ClickHouse driver
	db, _ := semql.NewClickHouseDB("localhost", 9000, "analytics", "default", "")
	defer db.Close()
	
	var results []Campaign
	if err := db.Query(context.Background(), query, &results); err != nil {
		panic(err)
	}
	
	// Process results
	for _, campaign := range results {
		fmt.Printf("%s (by %s): %.2f%% CTR\n", 
			campaign.Name, campaign.AdvertiserName, campaign.CTR)
	}
}
```

## Schema Definition

Define your tables, dimensions, metrics, and relationships:

```go
func setupSchema() *semql.Schema {
	schema := semql.NewSchema()
	
	// Define the campaigns table
	campaignsTable := &semql.TableConfig{
		Name:  "campaigns",
		Alias: "c",
		Dimensions: []*semql.DimensionField{
			{Name: "campaignId", Column: "campaign_id", Type: "Int32", Primary: true},
			{Name: "campaignName", Column: "campaign_name", Type: "String"},
			{Name: "advertiserId", Column: "advertiser_id", Type: "Int32"},
			// Add more dimensions
		},
		Metrics: []*semql.MetricField{
			{Name: "impressions", Column: "impressions", Type: "Int64"},
			{Name: "clicks", Column: "clicks", Type: "Int64"},
			{
				Name: "ctr", 
				Type: "Float64", 
				Expression: "(c.clicks / c.impressions) * 100",
				Description: "Click-through rate (percentage)",
			},
			// Add more metrics
		},
	}
	
	// Define the advertisers table
	advertisersTable := &semql.TableConfig{
		Name:  "advertisers",
		Alias: "a",
		Dimensions: []*semql.DimensionField{
			{Name: "advertiserId", Column: "advertiser_id", Type: "Int32", Primary: true},
			{Name: "advertiserName", Column: "advertiser_name", Type: "String"},
			// Add more dimensions
		},
		Metrics: []*semql.MetricField{
			// Define metrics
		},
	}
	
	// Define the relationship between campaigns and advertisers
	campaignsTable.Joins = []*semql.JoinConfig{
		{
			Table: "advertisers",
			Type:  semql.LeftJoin,
			Conditions: []semql.JoinCondition{
				{
					LeftField:  "advertiser_id", 
					Operator:   "=", 
					RightField: "advertiser_id",
				},
			},
			// When these dimensions are requested, join advertisers automatically
			ExternalDimensions: []string{"advertiserName"},
		},
	}
	
	// Add tables to the schema
	schema.AddTable(campaignsTable)
	schema.AddTable(advertisersTable)
	
	return schema
}
```

## Query Building

SemQL makes it easy to build complex queries:

```go
// Basic query
query := semql.NewQueryBuilder(schema, "campaigns")
query.Select("campaignName", "impressions", "clicks", "ctr")

// Query with automatic join
query := semql.NewQueryBuilder(schema, "campaigns")
query.Select("campaignName", "advertiserName", "clicks", "ctr")
// The query builder automatically joins the advertisers table because advertiserName is requested

// Query with filtering
query := semql.NewQueryBuilder(schema, "campaigns")
query.Select("campaignName", "clicks", "ctr")
      .Where("impressions", ">", 1000)
      .Where("startDate", ">=", "2023-01-01")

// Query with sorting and pagination
query := semql.NewQueryBuilder(schema, "campaigns")
query.Select("campaignName", "clicks", "ctr")
      .OrderBy("ctr", "DESC")
      .Limit(20)
      .Offset(40)
```

## Working with Results

Map query results to Go structs:

```go
// Define your struct with appropriate db tags
type Campaign struct {
	ID           int       `db:"campaign_id"`
	Name         string    `db:"campaign_name"`
	Impressions  int64     `db:"impressions"`
	Clicks       int64     `db:"clicks"`
	CTR          float64   `db:"ctr"`
}

// Execute the query and scan results
var campaigns []Campaign
if err := db.Query(ctx, query, &campaigns); err != nil {
	// handle error
}

// Process the results
for _, campaign := range campaigns {
	fmt.Printf("%s: %d impressions, %d clicks, %.2f%% CTR\n", 
		campaign.Name, campaign.Impressions, campaign.Clicks, campaign.CTR)
}
```

## License

This project is licensed under the Apache License 2.0 - see the LICENSE file for details.