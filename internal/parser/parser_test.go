package parser

import (
	"testing"
)

// TestSanitizeColumnName verifies that column headers are converted to valid snake_case SQL names.
func TestSanitizeColumnName(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"Total Assets (USD)", "total_assets_usd"},
		{"Company Name", "company_name"},
		{"Revenue ($M)", "revenue_m"},
		{"rank", "rank"},
		{"  spaces  ", "spaces"},
	}

	for _, tt := range tests {
		result := sanitizeColumnName(tt.input)
		if result != tt.expected {
			t.Errorf("sanitizeColumnName(%q) = %q, want %q", tt.input, result, tt.expected)
		}
	}
}

// TestInferColumnType verifies that the correct SQL type is inferred from column values.
func TestInferColumnType(t *testing.T) {
	tests := []struct {
		name     string
		colIdx   int
		rows     [][]string
		expected string
	}{
		{
			name:     "all integers",
			colIdx:   0,
			rows:     [][]string{{"1"}, {"2"}, {"3"}},
			expected: "INT",
		},
		{
			name:     "floats",
			colIdx:   0,
			rows:     [][]string{{"1.5"}, {"2.3"}, {"3.7"}},
			expected: "FLOAT",
		},
		{
			name:     "text values",
			colIdx:   0,
			rows:     [][]string{{"Walmart"}, {"Amazon"}, {"Apple"}},
			expected: "VARCHAR(255)",
		},
		{
			name:     "currency values like $1,234",
			colIdx:   0,
			rows:     [][]string{{"$1,234"}, {"$5,678"}},
			expected: "INT",
		},
		{
			name:     "mixed text and numbers",
			colIdx:   0,
			rows:     [][]string{{"Financials"}, {"123"}},
			expected: "VARCHAR(255)",
		},
		{
			name:     "empty values default to VARCHAR",
			colIdx:   0,
			rows:     [][]string{{""}},
			expected: "VARCHAR(255)",
		},
		{
    		name:     "date values",
    		colIdx:   0,
    		rows:     [][]string{{"2024-01-15"}, {"2023-06-30"}},
    		expected: "DATE",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := inferColumnType(tt.colIdx, tt.rows)
			if result != tt.expected {
				t.Errorf("inferColumnType() = %q, want %q", result, tt.expected)
			}
		})
	}
}

// TestCleanNumeric verifies that currency symbols and commas are stripped correctly.
func TestCleanNumeric(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"$1,234,567", "1234567"},
		{"1.5%", "1.5"},
		{"USD 500", "500"},
		{"", ""},
		{"123", "123"},
	}

	for _, tt := range tests {
		result := cleanNumeric(tt.input)
		if result != tt.expected {
			t.Errorf("cleanNumeric(%q) = %q, want %q", tt.input, result, tt.expected)
		}
	}
}

// TestIsHeaderRepeat verifies that repeated header rows are correctly detected.
func TestIsHeaderRepeat(t *testing.T) {
	headers := []string{"Rank", "Name", "Revenue"}

	tests := []struct {
		name     string
		cells    []string
		expected bool
	}{
		{
			name:     "exact header repeat",
			cells:    []string{"Rank", "Name", "Revenue"},
			expected: true,
		},
		{
			name:     "data row",
			cells:    []string{"1", "Walmart", "713200"},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isHeaderRepeat(tt.cells, headers)
			if result != tt.expected {
				t.Errorf("isHeaderRepeat() = %v, want %v", result, tt.expected)
			}
		})
	}
}
