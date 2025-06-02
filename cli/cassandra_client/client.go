package cassandra_client

import (
	"os"

	"github.com/fatih/color"
	"github.com/gocql/gocql"
	"github.com/joho/godotenv"
)

var session *gocql.Session

func ConnectionToCassandra() {
	var err error

	if err = godotenv.Load(".env"); err != nil {
		color.Red("Failed to load .env file: %s", err.Error())
		return
	}
	color.Green("✅ .env file loaded")

	// Get HOST and KEYSPACE
	host := os.Getenv("HOST")
	keyspace := os.Getenv("KEYSPACE")
	if host == "" || keyspace == "" {
		color.Red("HOST or KEYSPACE not defined in .env file")
		return
	}
	color.Cyan("Connecting to Cassandra at %s, keyspace: %s", host, keyspace)

	// Create Cassandra cluster
	cluster := gocql.NewCluster(host)
	cluster.Keyspace = keyspace
	cluster.Consistency = gocql.One
	session, err = cluster.CreateSession()
	if err != nil {
		color.Red("❌ Failed to create Cassandra session: %s", err.Error())
		return
	}
	color.Green("✅ Connected to Cassandra")
}

func GetSession() *gocql.Session {
	return session
}
func Close() {
	session.Close()
}
