package main

import (
	"github.com/DavidZayar/cli/cassandra_client"
	"github.com/DavidZayar/cli/cmd"
)

func main() {
	cassandra_client.ConnectionToCassandra()
	cmd.Exec()
}
