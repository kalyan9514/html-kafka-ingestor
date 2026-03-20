package db

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	_ "github.com/go-sql-driver/mysql" // registers MySQL driver with database/sql
	"github.com/kalyan9514/html-kafka-ingestor/internal/parser"
)

// DB wraps a sql.DB connection with our app's data access logic.
type DB struct {
	conn *sql.DB
}

// New opens a MySQL connection and verifies it with a ping.
// DSN format: user:password@tcp(host:port)/dbname
func New(host, port, user, password, dbname string) (*DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true", user, password, host, port, dbname)
	conn, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open DB connection: %w", err)
	}

	if err := conn.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping DB: %w", err)
	}

	log.Println("Connected to MySQL successfully")
	return &DB{conn: conn}, nil
}

// CreateTable dynamically builds and executes a CREATE TABLE statement
// from the inferred schema. IF NOT EXISTS prevents errors on re-runs.
func (d *DB) CreateTable(tableName string, columns []parser.Column) error {
	var colDefs []string
	for _, col := range columns {
		colDefs = append(colDefs, fmt.Sprintf("`%s` %s", col.Name, col.DataType))
	}

	query := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS `%s` (id INT AUTO_INCREMENT PRIMARY KEY, %s)",
		tableName,
		strings.Join(colDefs, ", "),
	)

	log.Printf("Creating table: %s", query)
	_, err := d.conn.Exec(query)
	return err
}

// BatchInsert inserts multiple rows in a single SQL statement for efficiency.
// Falls back to inserting remaining rows if batch size doesn't divide evenly.
func (d *DB) BatchInsert(tableName string, columns []parser.Column, rows []map[string]string) error {
	if len(rows) == 0 {
		return nil
	}

	// build column name list for the INSERT statement
	colNames := make([]string, len(columns))
	for i, col := range columns {
		colNames[i] = fmt.Sprintf("`%s`", col.Name)
	}

	// build one placeholder group per row e.g. (?, ?, ?)
	placeholders := fmt.Sprintf("(%s)", strings.Join(make([]string, len(columns)), "?, "))
	placeholders = strings.Replace(placeholders, " ", "", -1)
	placeholders = strings.TrimPrefix(placeholders, "(")
	placeholders = "(" + strings.Repeat("?,", len(columns))
	placeholders = strings.TrimSuffix(placeholders, ",") + ")"

	allPlaceholders := make([]string, len(rows))
	for i := range rows {
		allPlaceholders[i] = placeholders
	}

	query := fmt.Sprintf(
		"INSERT INTO `%s` (%s) VALUES %s",
		tableName,
		strings.Join(colNames, ", "),
		strings.Join(allPlaceholders, ", "),
	)

	// flatten all row values into a single slice for the query args
	args := make([]interface{}, 0, len(rows)*len(columns))
	for _, row := range rows {
		for _, col := range columns {
			args = append(args, row[col.Name])
		}
	}

	_, err := d.conn.Exec(query, args...)
	if err != nil {
		return fmt.Errorf("batch insert failed: %w", err)
	}

	log.Printf("Inserted %d rows into %s", len(rows), tableName)
	return nil
}

// Close releases the database connection pool.
func (d *DB) Close() error {
	return d.conn.Close()
}