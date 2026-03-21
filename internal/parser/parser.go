package parser

import (
	"log"
	"regexp"
	"strconv"
	"strings"

	"golang.org/x/net/html"
)

// Column represents a single table column with its name and inferred SQL type.
type Column struct {
	Name     string
	DataType string
}

// Table holds the parsed schema and all rows of data.
type Table struct {
	Columns []Column
	Rows    []map[string]string
}

// Parse finds the largest wikitable in the HTML and returns a typed Table.
// We pick the largest table because Wikipedia pages often have smaller nav/info tables.
func Parse(htmlContent string) (*Table, error) {
	doc, err := html.Parse(strings.NewReader(htmlContent))
	if err != nil {
		return nil, err
	}

	tables := findWikiTables(doc)
	if len(tables) == 0 {
		return nil, nil
	}

	best := tables[0]
	for _, t := range tables[1:] {
		if countRows(t) > countRows(best) {
			best = t
		}
	}

	return extractTable(best), nil
}

// findWikiTables traverses the HTML tree and collects all wikitable nodes.
func findWikiTables(n *html.Node) []*html.Node {
	var tables []*html.Node
	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if n.Type == html.ElementNode && n.Data == "table" {
			for _, a := range n.Attr {
				if a.Key == "class" && strings.Contains(a.Val, "wikitable") {
					tables = append(tables, n)
				}
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	walk(n)
	return tables
}

// countRows returns the number of tr elements — used to identify the main data table.
func countRows(table *html.Node) int {
	count := 0
	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if n.Type == html.ElementNode && n.Data == "tr" {
			count++
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	walk(table)
	return count
}

// extractTable pulls headers and rows out of a table node and infers SQL types.
func extractTable(table *html.Node) *Table {
	var headers []string
	var rawRows [][]string

	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if n.Type == html.ElementNode && n.Data == "tr" {
			var cells []string
			for c := n.FirstChild; c != nil; c = c.NextSibling {
				if c.Type == html.ElementNode && (c.Data == "th" || c.Data == "td") {
					cells = append(cells, cleanText(textContent(c)))
				}
			}
			if len(cells) == 0 {
				return
			}
			if len(headers) == 0 && hasThCells(n) {
				headers = cells
			} else if len(headers) > 0 {
				// skip rows that repeat the header — Wikipedia sometimes adds these mid-table
				if !isHeaderRepeat(cells, headers) {
					rawRows = append(rawRows, cells)
				}
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	walk(table)

	log.Printf("Parsed %d columns and %d rows", len(headers), len(rawRows))

	columns := inferSchema(headers, rawRows)

	var rows []map[string]string
	for _, raw := range rawRows {
		row := make(map[string]string)
		for i, col := range columns {
			if i < len(raw) {
				row[col.Name] = raw[i]
			}
		}
		rows = append(rows, row)
	}

	return &Table{Columns: columns, Rows: rows}
}

// inferSchema picks the tightest SQL type per column — INT > FLOAT > VARCHAR.
func inferSchema(headers []string, rows [][]string) []Column {
	columns := make([]Column, len(headers))
	for i, h := range headers {
		columns[i] = Column{
			Name:     sanitizeColumnName(h),
			DataType: inferColumnType(i, rows),
		}
		log.Printf("Column %q inferred as %s", columns[i].Name, columns[i].DataType)
	}
	return columns
}

// textContent extracts visible text from an HTML node, skipping footnote markers.
func textContent(n *html.Node) string {
	if n.Type == html.ElementNode && n.Data == "sup" {
		return "" // skip Wikipedia footnote markers like [1], [2]
	}
	if n.Type == html.TextNode {
		return n.Data
	}
	var sb strings.Builder
	for c := n.FirstChild; c != nil; c = c.NextSibling {
		sb.WriteString(textContent(c))
	}
	return sb.String()
}

// cleanText normalises whitespace and trims surrounding spaces.
func cleanText(s string) string {
	s = strings.TrimSpace(s)
	s = strings.Join(strings.Fields(s), " ")
	return s
}

// cleanNumeric strips currency symbols and commas so numeric strings can be parsed.
func cleanNumeric(s string) string {
	re := regexp.MustCompile(`[^\d.\-]`)
	return re.ReplaceAllString(s, "")
}

// sanitizeColumnName converts a header like "Total Assets (USD)" to "total_assets_usd".
func sanitizeColumnName(s string) string {
	s = strings.ToLower(s)
	re := regexp.MustCompile(`[^a-z0-9]+`)
	s = re.ReplaceAllString(s, "_")
	return strings.Trim(s, "_")
}

// hasThCells returns true if a tr node contains at least one th cell.
func hasThCells(n *html.Node) bool {
	for c := n.FirstChild; c != nil; c = c.NextSibling {
		if c.Type == html.ElementNode && c.Data == "th" {
			return true
		}
	}
	return false
}

// isHeaderRepeat returns true if a row contains the same values as the header.
// Wikipedia sometimes repeats header rows mid-table for readability.
func isHeaderRepeat(cells, headers []string) bool {
    matches := 0
    for i, cell := range cells {
        if i < len(headers) && strings.EqualFold(cell, headers[i]) {
            matches++
        }
    }
    return matches > len(headers)/2
}

func inferColumnType(colIdx int, rows [][]string) string {
	isInt := true
	isFloat := true
	hasValue := false

	for _, row := range rows {
		if colIdx >= len(row) {
			continue
		}
		raw := strings.TrimSpace(row[colIdx])
		if raw == "" {
			continue
		}
		cleaned := cleanNumeric(raw)
		if cleaned == "" {
			return "VARCHAR(255)"
		}
		hasValue = true
		if _, err := strconv.ParseInt(cleaned, 10, 64); err != nil {
			isInt = false
		}
		if _, err := strconv.ParseFloat(cleaned, 64); err != nil {
			isFloat = false
		}
	}

	if !hasValue {
		return "VARCHAR(255)"
	}
	if isInt {
		return "INT"
	}
	if isFloat {
		return "FLOAT"
	}
	return "VARCHAR(255)"
}