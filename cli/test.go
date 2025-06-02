package main

// import (
// 	"fmt"
// 	"log"

// 	"github.com/DavidZayar/cli/cassandra_client"
// 	"github.com/gocql/gocql"
// 	// Add this import
// )

// // // var EdgeBidirectionCmd = &cobra.Command{
// // // 	Use:   "edgeBidirection",
// // // 	Short: "Insert the edges_bidirectional table with undirected edges",
// // // 	Run: func(cmd *cobra.Command, args []string) {
// // // 		batchInsertBidirection()
// // // 	},
// // // }

// func batchInsertBidirection() {
// 	session := cassandra_client.GetSession()
// 	defer session.Close()

// 	iter := session.Query("SELECT from_node, to_node FROM edges").Iter()
// 	var fromNode, toNode string
// 	batchSize := 100
// 	batch := session.NewBatch(gocql.UnloggedBatch)
// 	count := 0

// 	for iter.Scan(&fromNode, &toNode) {
// 		// Avoid self-loops
// 		if fromNode == toNode {
// 			continue
// 		}

// 		// Insert both directions with unique edge_id using Cassandra's uuid() function
// 		batch.Query(
// 			"INSERT INTO edges_bidirectional (from_node, to_node, edge_id) VALUES (?, ?, uuid())",
// 			fromNode, toNode,
// 		)
// 		batch.Query(
// 			"INSERT INTO edges_bidirectional (from_node, to_node, edge_id) VALUES (?, ?, uuid())",
// 			toNode, fromNode,
// 		)

// 		count += 2
// 		if count >= batchSize {
// 			if err := session.ExecuteBatch(batch); err != nil {
// 				log.Fatalf("Batch insert failed: %v", err.Error())
// 			}
// 			batch = session.NewBatch(gocql.UnloggedBatch)
// 			count = 0
// 		}
// 	}

// 	if count > 0 {
// 		if err := session.ExecuteBatch(batch); err != nil {
// 			log.Fatalf("Final batch insert failed: %v", err)
// 		}
// 	}

// 	if err := iter.Close(); err != nil {
// 		log.Printf("Error closing iterator: %v"