package cmd

import (
	"fmt"
	"github.com/DavidZayar/cli/cassandra_client"
	"log"

	"github.com/fatih/color"
	"github.com/spf13/cobra"
)

var from_node string
var queryOneTemplate = fmt.Sprintf("SELECT to_node FROM edges WHERE from_node = '%s' ALLOW FILTERING ;\n", from_node)

var QueryOneCmd = &cobra.Command{
	Use:     "one",
	Aliases: []string{"one"},
	Short:   color.GreenString("Query successors of a node"),
	Run: func(cmd *cobra.Command, args []string) {
		QueryOneAction()
	},
}

func QueryOneAction() {
	fmt.Println("Finding the successor of the given node")
	if err := cassandra_client.ConnectionToCassandra(); err != nil {
		log.Fatalf("Failed to connect to Cassandra: %v", err)
	}
	defer cassandra_client.Close()

	color.Yellow("Creating the Session")
	session := cassandra_client.GetSession()
	if session == nil {
		log.Fatal("No Cassandra session available")
	}

	// Execute the query
	iter := session.Query(queryOneTemplate).Iter()

	var toNode string
	count := 0
	for iter.Scan(&toNode) {
		color.Cyan("Successor: %s", toNode)
		count++
	}

	if err := iter.Close(); err != nil {
		log.Fatal("Error reading results: ", err)
	}

	color.Green("Query complete. Found %d successors.", count)
}
