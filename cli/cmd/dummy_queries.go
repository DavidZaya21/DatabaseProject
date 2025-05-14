package cmd

import (
	context "context"
	"fmt"
	"github.com/DavidZayar/cli/cassandra_client"
	"github.com/fatih/color"
	"github.com/spf13/cobra"
	"log"
	"time"
)

var selectIdtemplate = `select id  from nodes where name = '/c/en/smokeball/n' allow filtering ;`

var QueryDummyCmd = &cobra.Command{
	Use:     "dummy",
	Aliases: []string{"dummy"},
	Short:   color.GreenString("dummy one"),
	Run: func(cmd *cobra.Command, args []string) {
		DummyAction()
	},
}

func DummyAction() {
	color.Yellow("Dummy is running")
	session := cassandra_client.GetSession()
	defer session.Close()

	// Step 1: Get the node ID from the 'nodes' table
	var id string
	query := `SELECT id FROM nodes WHERE name = '/c/en/smokeball/n' ALLOW FILTERING;`
	err := session.Query(query).Scan(&id)
	if err != nil {
		log.Fatalf("Failed to fetch node ID: %v", err)
		return
	}
	fmt.Println("Found node ID:", id)

	// Step 2: Use the node ID to find all 'from_node' values from 'edges' table
	queryTwo := `SELECT from_node FROM edges WHERE to_node = ? ALLOW FILTERING `
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	iter := session.Query(queryTwo, id).WithContext(ctx).Iter()

	var fromNode string
	fmt.Println("Successor nodes (from_node):")
	for iter.Scan(&fromNode) {
		fmt.Println(" -", fromNode)
	}

	if err := iter.Close(); err != nil {
		log.Fatalf("Error closing iterator: %v", err.Error())
	}
}
