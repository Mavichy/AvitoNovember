package config

import (
	"log"
	"os"
)

type Config struct {
	HTTPPort string
	DBDSN    string
}

func FromEnv() Config {
	port := os.Getenv("HTTP_PORT")
	if port == "" {
		port = "8080"
	}

	dsn := os.Getenv("DB_DSN")
	if dsn == "" {
		log.Fatal("env DB_DSN is required")
	}

	return Config{
		HTTPPort: port,
		DBDSN:    dsn,
	}
}
