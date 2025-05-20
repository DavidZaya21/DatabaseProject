package cassandra_client

import (
	"github.com/joho/godotenv"
	"os"
	"strconv"
	"time"

	"github.com/fatih/color"
	"github.com/gocql/gocql"
)

var session *gocql.Session

func ConnectionToCassandra() error {
	if err := godotenv.Load(".env"); err != nil {
		color.Red("Failed to load .env file: %v", err)
		return err
	}

	host := os.Getenv("HOST")
	if host == "" {
		host = "127.0.0.1"
	}
	port := os.Getenv("PORT")
	if port == "" {
		port = "9042"
	}
	keyspace := os.Getenv("KEYSPACE")
	if keyspace == "" {
		keyspace = "fina_schema"
	}

	cluster := gocql.NewCluster(host)
	cluster.Port = parsePort(port)
	cluster.Keyspace = keyspace
	cluster.Consistency = gocql.Quorum
	cluster.NumConns = 2
	cluster.Timeout = 10 * time.Second
	cluster.ConnectTimeout = 10 * time.Second
	cluster.Hosts = []string{host + ":" + port}
	cluster.DisableInitialHostLookup = true
	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())

	var err error
	session, err = cluster.CreateSession()
	if err != nil {
		color.Red("Failed to create Cassandra session: %v", err)
		return err
	}

	color.Green("Successfully connected to Cassandra at %s:%s", host, port)
	return nil
}

func GetSession() *gocql.Session {
	if session == nil {
		color.Red("Cassandra session is nil. Call ConnectionToCassandra first.")
	}
	return session
}

func Close() {
	if session != nil {
		session.Close()
		session = nil
		color.Green("Cassandra session closed")
	}
}

func parsePort(port string) int {
	p, err := strconv.Atoi(port)
	if err != nil {
		color.Red("Invalid port %s, defaulting to 9042: %v", port, err)
		return 9042
	}
	return p
}
