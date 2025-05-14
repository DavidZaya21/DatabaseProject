package cmd

import (
	"github.com/DavidZayar/cli/cassandra_client"
	"github.com/fatih/color"
	"github.com/spf13/cobra"
	"log"
)

var queryOneTemplate = `
select to_node from edges where from_node = '/c/en/value' allow filtering;
`
var QueryOneCmd = &cobra.Command{
	Use:     "one",
	Aliases: []string{"one"},
	Short:   color.GreenString("Creating the new table"),
	Run: func(cmd *cobra.Command, args []string) {
		QueryOneAction()
	},
}

func QueryOneAction() {
	color.Yellow("Creating the Session")
	session := cassandra_client.GetSession()
	defer session.Close()

	// Execute the query
	iter := session.Query(queryOneTemplate).Iter()

	var toNode string
	count := 0
	for iter.Scan(&toNode) {
		color.Cyan("Successor: %s", toNode)
		count++
	}

	if err := iter.Close(); err != nil {
		log.Fatal("Error reading results: ", err.Error())
	}

	color.Green("Query complete. Found %d successors.", count)
}
