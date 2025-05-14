package cassandra_client

import (
	"github.com/fatih/color"
	"github.com/gocql/gocql"
	"github.com/joho/godotenv"
	"os"
)

var session *gocql.Session

func ConnectionToCassandra() {
	var err error
	if err = godotenv.Load(".env"); err != nil {
		color.Red("Loading env file is failed", err.Error())
		return
	}
	host := os.Getenv("HOST")
	cluster := gocql.NewCluster(host)
	cluster.Keyspace = os.Getenv("KEYSPACE")
	cluster.Consistency = gocql.Quorum
	session, err = cluster.CreateSession()
	if err != nil {
		color.Red("Failed to create the cassandra session" + err.Error())
		return
	}
}

func GetSession() *gocql.Session {
	return session
}
func Close() {
	session.Close()
}
