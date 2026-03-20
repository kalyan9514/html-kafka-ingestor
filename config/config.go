package config

import (
	"log"
	"os"

	"github.com/joho/godotenv"
)

type Config struct {
	TargetURL    string 
	KafkaTopic   string 
	KafkaBrokers string 
	KafkaGroupID string 
	DBHost       string 
	DBPort       string 
	DBUser       string 
	DBPassword   string 
	DBName       string 
	APIPort      string 
	MetricsPort  string 
}


func Load() *Config {
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, reading from system environment")
	}

	return &Config{
		TargetURL:    mustGetEnv("TARGET_URL"),
		KafkaTopic:   getEnv("KAFKA_TOPIC", "html-records"),
		KafkaBrokers: getEnv("KAFKA_BROKERS", "localhost:9092"),
		KafkaGroupID: getEnv("KAFKA_GROUP_ID", "html-ingestor-group"),
		DBHost:       getEnv("DB_HOST", "localhost"),
		DBPort:       getEnv("DB_PORT", "3306"),
		DBUser:       getEnv("DB_USER", "ingestor"),
		DBPassword:   mustGetEnv("DB_PASSWORD"),
		DBName:       getEnv("DB_NAME", "ingestor_db"),
		APIPort:      getEnv("API_PORT", "8080"),
		MetricsPort:  getEnv("METRICS_PORT", "2112"),
	}
}


func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	log.Printf("WARNING: %s not set, using default: %s", key, fallback)
	return fallback
}

func mustGetEnv(key string) string {
	val := os.Getenv(key)
	if val == "" {
		log.Fatalf("FATAL: required env variable %s is not set", key)
	}
	return val
}