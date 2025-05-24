package semql

import (
	"fmt"
	"strings"
	"time"
)

// QueryBuilder represents a SQL query builder for the semantic layer
type QueryBuilder struct {
	schema          *Schema
	mainTable       string
	selectedDims    []string
	selectedMets    []string
	filters         []Filter
	sortFields      []SortField
	timeFilter      *TimeFilter
	timeGran        TimeGranularity
	granularityDims []string       // Additional dimensions for time grouping
	timezone        *time.Location // Timezone for time operations
	limit           int
	offset          int
	args            []interface{}
	fieldAliases    map[string]string // Stores mapping from selected field name to its SQL alias
}

// Filter represents a filter condition
type Filter struct {
	Field    string
	Operator string
	Value    interface{}
}

// SortField represents a field to sort by
type SortField struct {
	Field     string
	Direction string // ASC or DESC
}

// TimeFilter represents a time range filter
type TimeFilter struct {
	StartTime time.Time
	EndTime   time.Time
	Field     string         // The time field to filter on
	Timezone  *time.Location // Timezone for time operations
}

// NewQueryBuilder creates a new query builder for a specific table
func NewQueryBuilder(schema *Schema, mainTable string) *QueryBuilder {
	// Check if the table has a default timezone
	var timezone *time.Location
	// Attempt to get default timezone from main table config
	if tableCfg, ok := schema.Tables[mainTable]; ok {
		if tableCfg.TimeZone != nil {
			timezone = tableCfg.TimeZone
		}
	}

	return &QueryBuilder{
		schema:       schema,
		mainTable:    mainTable,
		timezone:     timezone,
		limit:        100, // Default limit
		fieldAliases: make(map[string]string),
	}
}

// Select adds dimensions and metrics to the query
func (qb *QueryBuilder) Select(fields ...string) *QueryBuilder {
	mainTableConfig := qb.schema.Tables[qb.mainTable]

	for _, fieldName := range fields {
		isDimension := false
		isMetric := false

		// Check dimensions in main table
		for _, dim := range mainTableConfig.Dimensions {
			if dim.Name == fieldName {
				if !containsString(qb.selectedDims, fieldName) {
					qb.selectedDims = append(qb.selectedDims, fieldName)
				}
				isDimension = true
				break
			}
		}
		if isDimension {
			continue
		}

		// Check metrics in main table
		for _, met := range mainTableConfig.Metrics {
			if met.Name == fieldName {
				if !containsString(qb.selectedMets, fieldName) {
					qb.selectedMets = append(qb.selectedMets, fieldName)
				}
				isMetric = true
				break
			}
		}
		if isMetric {
			continue
		}

		// If not in main table, check external fields defined in joins of the main table
		foundExternal := false
		for _, join := range mainTableConfig.Joins {
			for _, extDimName := range join.ExternalDimensions {
				if extDimName == fieldName {
					if !containsString(qb.selectedDims, fieldName) {
						qb.selectedDims = append(qb.selectedDims, fieldName)
					}
					isDimension = true
					foundExternal = true
					break
				}
			}
			if foundExternal {
				break
			}
			for _, extMetName := range join.ExternalMetrics {
				if extMetName == fieldName {
					if !containsString(qb.selectedMets, fieldName) {
						qb.selectedMets = append(qb.selectedMets, fieldName)
					}
					isMetric = true
					foundExternal = true
					break
				}
			}
			if foundExternal {
				break
			}
		}
		// If not found anywhere, it's an unknown field.
		// For robust error handling, one might add an error return or logging here.
	}
	return qb
}

// Where adds a filter condition
func (qb *QueryBuilder) Where(field, operator string, value interface{}) *QueryBuilder {
	qb.filters = append(qb.filters, Filter{
		Field:    field,
		Operator: operator,
		Value:    value,
	})
	qb.args = append(qb.args, value)
	return qb
}

// OrderBy adds a sort field
func (qb *QueryBuilder) OrderBy(field, direction string) *QueryBuilder {
	qb.sortFields = append(qb.sortFields, SortField{
		Field:     field,
		Direction: direction,
	})
	return qb
}

// Limit sets the maximum number of rows to return
func (qb *QueryBuilder) Limit(limit int) *QueryBuilder {
	qb.limit = limit
	return qb
}

// Offset sets the number of rows to skip
func (qb *QueryBuilder) Offset(offset int) *QueryBuilder {
	qb.offset = offset
	return qb
}

// Between adds a time range filter to the query
func (qb *QueryBuilder) Between(startTime, endTime time.Time) *QueryBuilder {
	// Find the time field (physical name)
	// This logic relies on getTimeFieldAndTable to find the relevant physical time field.
	_, _, physicalTimeField := qb.getTimeFieldAndTable()

	if physicalTimeField == "" {
		fmt.Printf("Warning: No time field resolved for Between filter on table %s\\n", qb.mainTable)
		return qb
	}

	// Determine the effective timezone for this filter.
	filterTimezone := qb.timezone // Start with query-wide timezone
	if mainTableCfg, ok := qb.schema.Tables[qb.mainTable]; ok && mainTableCfg.TimeZone != nil {
		if filterTimezone == nil { // If query-wide is not set, use table's default for the filter
			filterTimezone = mainTableCfg.TimeZone
		}
	}

	qb.timeFilter = &TimeFilter{
		StartTime: startTime,
		EndTime:   endTime,
		Field:     physicalTimeField, // Store physical field name
		Timezone:  filterTimezone,    // Store the effective timezone for this filter
	}

	// Add arguments for query parameterization
	// ClickHouse expects 'YYYY-MM-DD HH:MM:SS' format for toDateTime function when a timezone is specified in the function call.
	const clickHouseDateTimeFormat = "2006-01-02 15:04:05"
	if filterTimezone != nil {
		// Format time in its original location, then pass as string. CH will interpret it with toDateTime(?, tz)
		qb.args = append(qb.args, startTime.Format(clickHouseDateTimeFormat))
		qb.args = append(qb.args, endTime.Format(clickHouseDateTimeFormat))
	} else {
		// No specific timezone for the filter, pass time.Time objects directly.
		// The driver will handle conversion (typically to UTC or server's default timezone).
		qb.args = append(qb.args, startTime)
		qb.args = append(qb.args, endTime)
	}
	return qb
}

// WithTimezone sets the timezone for time-based operations
func (qb *QueryBuilder) WithTimezone(location *time.Location) *QueryBuilder {
	qb.timezone = location

	// Update the time filter timezone if it exists
	if qb.timeFilter != nil {
		qb.timeFilter.Timezone = location
	}

	return qb
}

// WithGranularity sets the time granularity for the query
func (qb *QueryBuilder) WithGranularity(granularity TimeGranularity, additionalDimensions ...string) *QueryBuilder {
	qb.timeGran = granularity
	qb.granularityDims = additionalDimensions
	return qb
}

// determineRequiredJoins determines which tables need to be joined.
// Returns a map of target table names to their JoinConfig.
func (qb *QueryBuilder) determineRequiredJoins() (map[string]*JoinConfig, error) {
	requiredJoinConfigs := make(map[string]*JoinConfig)
	mainTableConfig := qb.schema.Tables[qb.mainTable]

	allSelectedFields := make(map[string]string) // fieldName -> type ("dim" or "met")
	for _, dimName := range qb.selectedDims {
		allSelectedFields[dimName] = "dim"
	}
	for _, metName := range qb.selectedMets {
		allSelectedFields[metName] = "met"
	}

	for fieldName, fieldType := range allSelectedFields {
		inMainTable := false
		if fieldType == "dim" {
			for _, dim := range mainTableConfig.Dimensions {
				if dim.Name == fieldName {
					inMainTable = true
					break
				}
			}
		} else { // fieldType == "met"
			for _, met := range mainTableConfig.Metrics {
				if met.Name == fieldName {
					inMainTable = true
					break
				}
			}
		}

		if inMainTable {
			continue
		}

		foundViaJoin := false
		for _, joinConfig := range mainTableConfig.Joins {
			targetTableName := joinConfig.Table
			if fieldType == "dim" {
				for _, extDimName := range joinConfig.ExternalDimensions {
					if extDimName == fieldName {
						requiredJoinConfigs[targetTableName] = joinConfig
						foundViaJoin = true
						break
					}
				}
			} else { // fieldType == "met"
				for _, extMetName := range joinConfig.ExternalMetrics {
					if extMetName == fieldName {
						requiredJoinConfigs[targetTableName] = joinConfig
						foundViaJoin = true
						break
					}
				}
			}
			if foundViaJoin {
				break
			}
		}
		if !foundViaJoin {
			// Field selected but not in main table and not an external field in any join.
			// This could be an error or an implicitly selected field from a joined table not marked as "external".
			// For now, we proceed; buildSelectClause will ultimately fail if it can't resolve the field.
			// Consider returning an error: fmt.Errorf("field '%s' is selected but cannot be resolved via main table or joins", fieldName)
		}
	}
	return requiredJoinConfigs, nil
}

// Helper function to check if a string is in a slice
func containsString(slice []string, item string) bool {
	for _, v := range slice {
		if v == item {
			return true
		}
	}
	return false
}

// formatTimeGranularity formats a time field according to the specified granularity and timezone
func (qb *QueryBuilder) formatTimeGranularity(tableAlias, timeFieldPhysical string) string {
	tzName := "UTC" // Default timezone for ClickHouse functions if not specified
	if qb.timezone != nil {
		tzName = qb.timezone.String()
	}

	// Ensure timeFieldPhysical is qualified with tableAlias if not already
	qualifiedTimeField := timeFieldPhysical
	if tableAlias != "" && !strings.Contains(qualifiedTimeField, ".") {
		qualifiedTimeField = fmt.Sprintf("%s.%s", tableAlias, timeFieldPhysical)
	}

	// Apply toTimeZone function first
	timeExprWithTz := fmt.Sprintf("toTimeZone(%s, '%s')", qualifiedTimeField, tzName)

	switch qb.timeGran {
	case GranularityHourly:
		return fmt.Sprintf("toStartOfHour(%s)", timeExprWithTz)
	case GranularityDaily:
		return fmt.Sprintf("toStartOfDay(%s)", timeExprWithTz)
	case GranularityWeekly:
		// ClickHouse: toStartOfWeek(date, mode), mode 1 for Sunday, 2 for Monday
		// Assuming Monday as start of week (mode 2 or 3 depending on ISO/US)
		// Let's use toISOWeek and then toStartOfWeek with that week.
		// Simpler: toMonday() gives start of ISO week.
		return fmt.Sprintf("toMonday(%s)", timeExprWithTz)
	case GranularityMonthly:
		return fmt.Sprintf("toStartOfMonth(%s)", timeExprWithTz)
	case GranularityYearly:
		return fmt.Sprintf("toStartOfYear(%s)", timeExprWithTz)
	default:
		// Default to raw time field, but with timezone conversion
		return timeExprWithTz
	}
}

// getTimeFieldAndTable finds the primary time field (logical and physical) and its table alias for the query.
// It prioritizes the main table's TimeField, then looks for it in schema.
// Returns: tableAlias, logicalName, physicalName
func (qb *QueryBuilder) getTimeFieldAndTable() (string, string, string) {
	mainTableCfg := qb.schema.Tables[qb.mainTable]
	mainAlias := mainTableCfg.Alias
	if mainAlias == "" {
		mainAlias = qb.mainTable // Fallback
	}

	if mainTableCfg.TimeField != "" {
		// Assuming TimeField in TableConfig is the physical name and also serves as its logical name in this context
		return mainAlias, mainTableCfg.TimeField, mainTableCfg.TimeField
	}

	// If main table doesn't have TimeField, search all tables in schema.
	// This is a simple search; a more robust system might have explicit primary time field declaration per query context.
	for tableName, tableCfg := range qb.schema.Tables {
		if tableCfg.TimeField != "" {
			// Check if this table is the main table or is joinable/relevant.
			// For simplicity, if we find *any* table with a TimeField, we consider it.
			// This might be too naive if multiple tables have TimeFields.
			// A better approach: check if this table is among determined requiredJoins or is main.
			alias := tableCfg.Alias
			if alias == "" {
				alias = tableName
			}
			// This logic should be improved to pick the "correct" time field if multiple exist.
			// For now, returns the first one found after main table.
			return alias, tableCfg.TimeField, tableCfg.TimeField
		}
	}
	return "", "", "" // Not found
}

// MetricAggregation represents the type of aggregation to perform on a metric
type MetricAggregation string

// Metric aggregation constants
const (
	AggregationSum   MetricAggregation = "SUM"
	AggregationAvg   MetricAggregation = "AVG"
	AggregationMin   MetricAggregation = "MIN"
	AggregationMax   MetricAggregation = "MAX"
	AggregationCount MetricAggregation = "COUNT"
	AggregationNone  MetricAggregation = ""
)

// formatMetricWithAggregation formats a metric field.
// metricIdentifier is the column name or a full SQL expression.
// metricName is the logical name for aliasing.
func (qb *QueryBuilder) formatMetricWithAggregation(tableAlias, metricIdentifier, metricName string, isExpression bool, needsAggregation bool) string {
	if isExpression {
		// If it's an expression, use it directly.
		// The expression is responsible for its own aggregation if needed.
		return metricIdentifier // buildSelectClause will add "AS metricName"
	}

	// It's a direct column (metricIdentifier is column name)
	columnPart := metricIdentifier
	if tableAlias != "" && !strings.Contains(columnPart, ".") { // Qualify if not already qualified
		columnPart = fmt.Sprintf("%s.%s", tableAlias, metricIdentifier)
	}

	if needsAggregation {
		// Default aggregation for simple column metrics when granularity is applied.
		// TODO: Make aggregation type (SUM, AVG, etc.) configurable per metric.
		return fmt.Sprintf("SUM(%s)", columnPart) // buildSelectClause will add "AS metricName"
	}

	// No aggregation needed, just the column
	return columnPart // buildSelectClause will add "AS metricName"
}

// BuildSQL generates the SQL query string and its arguments.
// Returns the query, arguments, and any error encountered.
func (qb *QueryBuilder) BuildSQL() (string, []interface{}, error) {
	var query strings.Builder

	mainTableConfig := qb.schema.Tables[qb.mainTable]
	mainTableAlias := mainTableConfig.Alias
	if mainTableAlias == "" {
		mainTableAlias = qb.mainTable // Fallback
	}

	requiredJoinConfigs, err := qb.determineRequiredJoins()
	if err != nil {
		return "", nil, fmt.Errorf("error determining required joins: %w", err)
	}

	selectClause, err := qb.buildSelectClause(mainTableConfig, mainTableAlias, requiredJoinConfigs)
	if err != nil {
		return "", nil, fmt.Errorf("error building select clause: %w", err)
	}
	if selectClause == "" {
		return "", nil, fmt.Errorf("no fields selected or an error occurred in select clause generation")
	}
	query.WriteString("SELECT ")
	query.WriteString(selectClause)

	query.WriteString(fmt.Sprintf(" FROM %s AS %s", mainTableConfig.Name, mainTableAlias))

	// JOIN clauses
	for joinedTableName, joinConfig := range requiredJoinConfigs {
		joinedTableSchema, ok := qb.schema.Tables[joinedTableName]
		if !ok {
			return "", nil, fmt.Errorf("schema not found for table %s specified in join config", joinedTableName)
		}
		joinedTableSchemaAlias := joinedTableSchema.Alias
		if joinedTableSchemaAlias == "" {
			joinedTableSchemaAlias = joinedTableName // Fallback
		}

		query.WriteString(fmt.Sprintf(" %s %s AS %s ON ", joinConfig.Type, joinedTableSchema.Name, joinedTableSchemaAlias))
		var joinConditions []string
		for _, cond := range joinConfig.Conditions {
			// Assuming LeftField is from main table, RightField is from joined table.
			// This convention should be documented or made more explicit in JoinCondition.
			joinConditions = append(joinConditions, fmt.Sprintf("toUInt64(%s.%s) %s toUInt64(%s.%s)",
				mainTableAlias, cond.LeftField, // LeftField is physical column name on main table
				cond.Operator,
				joinedTableSchemaAlias, cond.RightField, // RightField is physical column name on joined table
			))
		}
		if len(joinConditions) == 0 {
			return "", nil, fmt.Errorf("no join conditions specified for join with table %s", joinedTableName)
		}
		query.WriteString(strings.Join(joinConditions, " AND "))
	}

	// WHERE clause
	var whereClauses []string
	currentArgsOffset := 0 // Track args consumed by regular filters if timeFilter adds its own later

	// Add regular filters
	if len(qb.filters) > 0 {
		filterArgs := qb.args[:0] // Temporary slice to hold only filter args for this section
		for _, f := range qb.filters {
			resolvedField, err := qb.resolveFieldExpressionForContext(f.Field, mainTableConfig, mainTableAlias, requiredJoinConfigs)
			if err != nil {
				return "", nil, fmt.Errorf("WHERE clause: failed to resolve field '%s': %w", f.Field, err)
			}
			whereClauses = append(whereClauses, fmt.Sprintf("%s %s ?", resolvedField, f.Operator))
			// Value for this filter is at qb.args[currentArgsOffset]
			filterArgs = append(filterArgs, qb.args[currentArgsOffset])
			currentArgsOffset++
		}
		// qb.args for regular filters are handled; timeFilter args are separate.
		// The qb.args needs careful management. Let's re-evaluate.
		// Simpler: Where() adds value to qb.args. BuildSQL uses them in order.
	}

	// Add time filter if present
	if qb.timeFilter != nil && qb.timeFilter.Field != "" {
		tfPhysical := qb.timeFilter.Field
		// Find alias of the table owning this physical time field
		tfTableAlias := qb.getTableAliasForPhysicalField(tfPhysical, mainTableConfig, mainTableAlias, requiredJoinConfigs)

		if tfTableAlias == "" {
			return "", nil, fmt.Errorf("could not determine table alias for time filter field: %s", tfPhysical)
		}

		timeFilterEffectiveTz := qb.timeFilter.Timezone // Timezone specific to this filter

		var timeColumnExpr string
		var placeholderStart, placeholderEnd string

		if timeFilterEffectiveTz != nil {
			tzName := timeFilterEffectiveTz.String()
			// Column is converted to the filter's timezone for comparison against parameters
			timeColumnExpr = fmt.Sprintf("toTimeZone(%s.%s, '%s')", tfTableAlias, tfPhysical, tzName)
			// Arguments are strings (formatted in Between), so use toDateTime(?, tzName) for comparison in ClickHouse
			placeholderStart = fmt.Sprintf("toDateTime(?, '%s')", tzName)
			placeholderEnd = fmt.Sprintf("toDateTime(?, '%s')", tzName)
		} else {
			// No specific timezone for filter, column is used as is (e.g., raw DateTime assumed UTC or server local)
			timeColumnExpr = fmt.Sprintf("%s.%s", tfTableAlias, tfPhysical)
			// Arguments are time.Time objects, use plain placeholders; driver handles conversion
			placeholderStart = "?"
			placeholderEnd = "?"
		}

		whereClauses = append(whereClauses, fmt.Sprintf("%s >= %s", timeColumnExpr, placeholderStart))
		whereClauses = append(whereClauses, fmt.Sprintf("%s < %s", timeColumnExpr, placeholderEnd))
		// Args for StartTime, EndTime were added by Between() method in the correct format (string or time.Time)
	}

	if len(whereClauses) > 0 {
		query.WriteString(" WHERE ")
		query.WriteString(strings.Join(whereClauses, " AND "))
	}

	// GROUP BY clause
	var groupParts []string
	needsGroupBy := false
	if qb.timeGran != "" {
		// Use the alias of the time_period column from SELECT clause for GROUP BY
		// This is standard SQL behavior if the DB supports it.
		// Or, repeat the expression. Repeating expression is safer.
		timeTableAliasForGroupBy, _, timePhysicalFieldForGroupBy := qb.getTimeFieldAndTable()
		if timePhysicalFieldForGroupBy != "" {
			groupParts = append(groupParts, qb.formatTimeGranularity(timeTableAliasForGroupBy, timePhysicalFieldForGroupBy))
			needsGroupBy = true
		} else {
			return "", nil, fmt.Errorf("time granularity specified but no time field found for GROUP BY")
		}
	}

	if len(qb.selectedDims) > 0 {
		for _, dimName := range qb.selectedDims {
			resolvedDimExpr, err := qb.resolveFieldExpressionForContext(dimName, mainTableConfig, mainTableAlias, requiredJoinConfigs)
			if err != nil {
				return "", nil, fmt.Errorf("GROUP BY: failed to resolve dimension '%s': %w", dimName, err)
			}
			groupParts = append(groupParts, resolvedDimExpr)
			needsGroupBy = true
		}
	}
	if len(qb.granularityDims) > 0 {
		for _, dimName := range qb.granularityDims {
			resolvedDimExpr, err := qb.resolveFieldExpressionForContext(dimName, mainTableConfig, mainTableAlias, requiredJoinConfigs)
			if err != nil {
				return "", nil, fmt.Errorf("GROUP BY: failed to resolve granularity dimension '%s': %w", dimName, err)
			}
			if !containsString(groupParts, resolvedDimExpr) { // Avoid duplicates
				groupParts = append(groupParts, resolvedDimExpr)
			}
			needsGroupBy = true
		}
	}

	// Add GROUP BY clause only if there are grouping elements or if aggregation is implied by metrics/timegran
	// If there are metrics selected and time granularity is applied, group by is necessary.
	// If only dimensions are selected, but time granularity is applied, group by is necessary.
	isAggregatedQuery := qb.timeGran != "" || (len(qb.selectedMets) > 0 && needsAggregation(qb, mainTableConfig, requiredJoinConfigs))

	if needsGroupBy || (isAggregatedQuery && len(groupParts) == 0 && len(qb.selectedDims) == 0 && qb.timeGran == "") {
		// If it's an aggregated query but no explicit group by fields yet (e.g. SELECT COUNT(*)),
		// we might not need a GROUP BY. But if there are non-aggregated dims selected alongside aggregated metrics,
		// those dims must be in GROUP BY. This is handled by adding selectedDims to groupParts.
		// If groupParts is still empty but it's an aggregate query (e.g. SELECT SUM(X) FROM T), no GROUP BY needed.
		// This condition is complex. Simplification: if groupParts is populated, add GROUP BY.
	}

	if len(groupParts) > 0 {
		query.WriteString(" GROUP BY ")
		query.WriteString(strings.Join(groupParts, ", "))
	}

	// ORDER BY clause
	if len(qb.sortFields) > 0 {
		query.WriteString(" ORDER BY ")
		var orderParts []string
		for _, sf := range qb.sortFields {
			// SQL standard allows ordering by SELECT list alias.
			if alias, ok := qb.fieldAliases[sf.Field]; ok {
				orderParts = append(orderParts, fmt.Sprintf("%s %s", alias, sf.Direction))
			} else {
				// If not a selected field alias, resolve it to its expression.
				resolvedField, err := qb.resolveFieldExpressionForContext(sf.Field, mainTableConfig, mainTableAlias, requiredJoinConfigs)
				if err != nil {
					return "", nil, fmt.Errorf("ORDER BY: failed to resolve field '%s': %w", sf.Field, err)
				}
				orderParts = append(orderParts, fmt.Sprintf("%s %s", resolvedField, sf.Direction))
			}
		}
		query.WriteString(strings.Join(orderParts, ", "))
	} else if qb.timeGran != "" {
		// Default sort by time_period when using time granularity, if time_period is selected
		if _, ok := qb.fieldAliases["time_period"]; ok {
			query.WriteString(" ORDER BY time_period ASC")
		}
	}

	// LIMIT and OFFSET
	if qb.limit > 0 {
		query.WriteString(fmt.Sprintf(" LIMIT %d", qb.limit))
	}
	if qb.offset > 0 {
		query.WriteString(fmt.Sprintf(" OFFSET %d", qb.offset))
	}

	return query.String(), qb.args, nil
}

// Helper to check if query implies aggregation
func needsAggregation(qb *QueryBuilder, mainTableConfig *TableConfig, requiredJoinConfigs map[string]*JoinConfig) bool {
	if qb.timeGran != "" {
		return true
	}
	for _, metName := range qb.selectedMets {
		// Find metric definition
		_, _, isExpr, _ := qb.findFieldDetails(metName, mainTableConfig, requiredJoinConfigs)
		if !isExpr { // Simple column metric, implies aggregation if other non-aggregated fields or timegran exists
			return true
		}
		// If it's an expression, it's responsible for its own aggregation.
		// However, its presence might still imply an aggregate query.
		// This logic is simplified; a full check is complex.
	}
	return false
}

// findFieldDetails finds details of a field (dim or met) across main and joined tables.
// Returns: actualIdentifier (col or expr), tableAlias, isExpression, found
func (qb *QueryBuilder) findFieldDetails(
	fieldName string,
	mainTableConfig *TableConfig,
	requiredJoinConfigs map[string]*JoinConfig,
) (string, string, bool, bool) {

	mainTableAlias := mainTableConfig.Alias
	if mainTableAlias == "" {
		mainTableAlias = qb.mainTable
	}

	// Check main table dimensions
	for _, dim := range mainTableConfig.Dimensions {
		if dim.Name == fieldName {
			return dim.Column, mainTableAlias, false, true
		}
	}
	// Check main table metrics
	for _, met := range mainTableConfig.Metrics {
		if met.Name == fieldName {
			id := met.Column
			isExpr := false
			if met.Expression != "" {
				id = met.Expression
				isExpr = true
			}
			return id, mainTableAlias, isExpr, true
		}
	}

	// Check joined tables
	for joinedTableName, joinConfig := range requiredJoinConfigs {
		joinedTableSchema := qb.schema.Tables[joinedTableName]
		joinedTableAlias := joinedTableSchema.Alias
		if joinedTableAlias == "" {
			joinedTableAlias = joinedTableName
		}

		// Check dimensions in joined table if fieldName is an external dim for this join
		for _, extDimName := range joinConfig.ExternalDimensions {
			if extDimName == fieldName {
				for _, dim := range joinedTableSchema.Dimensions {
					if dim.Name == fieldName {
						return dim.Column, joinedTableAlias, false, true
					}
				}
			}
		}
		// Check metrics in joined table if fieldName is an external met for this join
		for _, extMetName := range joinConfig.ExternalMetrics {
			if extMetName == fieldName {
				for _, met := range joinedTableSchema.Metrics {
					if met.Name == fieldName {
						id := met.Column
						isExpr := false
						if met.Expression != "" {
							id = met.Expression
							isExpr = true
						}
						return id, joinedTableAlias, isExpr, true
					}
				}
			}
		}
	}
	return "", "", false, false
}

// buildSelectClause builds the SELECT part of the query.
func (qb *QueryBuilder) buildSelectClause(
	mainTableConfig *TableConfig,
	mainTableAlias string,
	requiredJoinConfigs map[string]*JoinConfig,
) (string, error) {
	selectParts := []string{}
	qb.fieldAliases = make(map[string]string) // Reset for current build

	needsAgg := qb.timeGran != "" // Simplified: aggregation needed if time granularity is set.
	// More complex: if any metric needs aggregation.

	// Add time granularity field if specified
	if qb.timeGran != "" {
		timeTableAlias, _, timePhysicalField := qb.getTimeFieldAndTable()
		if timePhysicalField != "" {
			formattedTimeField := qb.formatTimeGranularity(timeTableAlias, timePhysicalField)
			selectParts = append(selectParts, fmt.Sprintf("%s AS time_period", formattedTimeField))
			qb.fieldAliases["time_period"] = "time_period"
		} else {
			return "", fmt.Errorf("time granularity specified but no time field found")
		}
	}

	// Add dimensions
	for _, dimName := range qb.selectedDims {
		identifier, tableAlias, _, found := qb.findFieldDetails(dimName, mainTableConfig, requiredJoinConfigs)
		if !found {
			return "", fmt.Errorf("dimension '%s' not found in schema or joins", dimName)
		}
		selectParts = append(selectParts, fmt.Sprintf("%s.%s AS %s", tableAlias, identifier, dimName))
		qb.fieldAliases[dimName] = dimName
	}

	// Add metrics
	for _, metName := range qb.selectedMets {
		identifier, tableAlias, isExpr, found := qb.findFieldDetails(metName, mainTableConfig, requiredJoinConfigs)
		if !found {
			return "", fmt.Errorf("metric '%s' not found in schema or joins", metName)
		}

		// Determine if this specific metric implies overall query aggregation
		// This is a simplification; a metric might be pre-aggregated in its expression.
		metricNeedsAgg := needsAgg      // 'needsAgg' here is from qb.timeGran != ""
		if !isExpr && !metricNeedsAgg { // If it's a simple column and no global agg, check if other metrics force agg
			if len(qb.selectedMets) > 1 || qb.timeGran != "" { // If multiple mets or time gran, simple cols likely need agg
				// This heuristic might need refinement.
				// If a query is SELECT dim, sum(met1), met2. met2 needs agg.
				// For now, rely on global `needsAgg` from timeGran.
				// And formatMetricWithAggregation's `needsAggregation` param.
			}
		}

		// If isExpr is false (metric is a simple column), then (needsAgg || !isExpr) becomes (needsAgg || true), which is true.
		// This ensures formatMetricWithAggregation receives true for needsAggregation, and applies SUM().
		// If isExpr is true (metric is an expression), then (needsAgg || !isExpr) becomes (needsAgg || false), which is needsAgg.
		// formatMetricWithAggregation receives isExpr=true, so it uses the expression directly, and the needsAgg param doesn't add SUM().
		formattedMetricBase := qb.formatMetricWithAggregation(tableAlias, identifier, metName, isExpr, needsAgg || !isExpr)
		selectParts = append(selectParts, fmt.Sprintf("%s AS %s", formattedMetricBase, metName))
		qb.fieldAliases[metName] = metName
	}

	if len(selectParts) == 0 {
		return "", fmt.Errorf("no fields to select")
	}
	return strings.Join(selectParts, ", "), nil
}

// resolveFieldExpressionForContext resolves a logical field name to its SQL expression for WHERE/GROUP BY/ORDER BY.
func (qb *QueryBuilder) resolveFieldExpressionForContext(
	fieldName string,
	mainTableConfig *TableConfig,
	mainTableAlias string,
	requiredJoinConfigs map[string]*JoinConfig,
) (string, error) {
	identifier, tableAlias, isExpr, found := qb.findFieldDetails(fieldName, mainTableConfig, requiredJoinConfigs)
	if !found {
		return "", fmt.Errorf("field '%s' not found for resolving in context", fieldName)
	}

	if isExpr {
		// If it's an expression, it might already contain table aliases.
		// e.g., "table.col1 + table.col2" or "SUM(table.col)"
		// It's responsibility of schema to define expressions correctly.
		// TODO: Consider replacing alias placeholders in expressions if a convention is adopted.
		return identifier, nil
	}
	// It's a simple column
	return fmt.Sprintf("%s.%s", tableAlias, identifier), nil
}

// getTableAliasForPhysicalField finds the alias of the table that owns a given physical field name.
// Used for timeFilter.Field which is a physical column name.
func (qb *QueryBuilder) getTableAliasForPhysicalField(
	physicalField string,
	mainTableConfig *TableConfig,
	mainTableAlias string,
	requiredJoinConfigs map[string]*JoinConfig,
) string {
	// Check main table's designated TimeField
	if mainTableConfig.TimeField == physicalField {
		return mainTableAlias
	}
	// Check all dimensions in main table for physical column match
	for _, dim := range mainTableConfig.Dimensions {
		if dim.Column == physicalField {
			return mainTableAlias
		}
	}
	// Check all metrics (physical column) in main table
	for _, met := range mainTableConfig.Metrics {
		if met.Column == physicalField && met.Expression == "" { // Ensure it's a direct column
			return mainTableAlias
		}
	}

	// Check joined tables
	for joinedTableName := range requiredJoinConfigs {
		tableCfg := qb.schema.Tables[joinedTableName]
		tableAlias := tableCfg.Alias
		if tableAlias == "" {
			tableAlias = joinedTableName
		}
		if tableCfg.TimeField == physicalField {
			return tableAlias
		}
		for _, dim := range tableCfg.Dimensions {
			if dim.Column == physicalField {
				return tableAlias
			}
		}
		for _, met := range tableCfg.Metrics {
			if met.Column == physicalField && met.Expression == "" {
				return tableAlias
			}
		}
	}
	return "" // Not found
}

// findFieldInTable is a stub, replaced by more specific findFieldDetails or resolveFieldExpressionForContext
func (qb *QueryBuilder) findFieldInTable(tableName, fieldName string) (string, string, bool) {
	// This function's role is covered by findFieldDetails.
	// It can be removed or adapted if a different use case arises.
	return "", "", false
}

// findJoinPath is a stub and not used in the current core BuildSQL logic.
func (qb *QueryBuilder) findJoinPath(fromTable, toTable string) (*JoinConfig, error) {
	// If the tables are the same, no join needed
	if fromTable == toTable {
		return nil, nil // Or an empty JoinConfig if that's more appropriate
	}

	// Check direct joins first
	fromTableConfig, ok := qb.schema.Tables[fromTable]
	if !ok {
		return nil, fmt.Errorf("source table '%s' not found in schema", fromTable)
	}

	for _, joinConfig := range fromTableConfig.Joins {
		if joinConfig.Table == toTable {
			return joinConfig, nil
		}
	}

	// TODO: Implement multi-hop join path finding if necessary.
	// For now, only direct joins are considered by determineRequiredJoins.

	return nil, fmt.Errorf("no direct join path found from '%s' to '%s'", fromTable, toTable)
}

// Definition for findDimensionInTable (if needed separately, though findFieldDetails is more general)
func findDimensionInTable(tableConfig *TableConfig, dimName string) (*DimensionField, bool) {
	for _, dim := range tableConfig.Dimensions {
		if dim.Name == dimName {
			return dim, true
		}
	}
	return nil, false
}

// Definition for findMetricInTable (if needed separately)
func findMetricInTable(tableConfig *TableConfig, metName string) (*MetricField, bool) {
	for _, met := range tableConfig.Metrics {
		if met.Name == metName {
			return met, true
		}
	}
	return nil, false
}
