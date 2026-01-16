package database

type Config struct {
	Host     string
	Port     int
	User     string
	Password string
	DBName   string
	Schema   string
	SSLMode  string
}
