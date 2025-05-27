package cmd

import (
	"fmt"
	"strings"

	"github.com/DavidZayar/cli/cassandra_client"
	"github.com/gocql/gocql"
	"github.com/spf13/cobra"
)

var (
	QuerySixteenCmd = &cobra.Command{
		Use:     "sixteen <from_node> <to_node>",
		Aliases: []string{"sixteen"},
		Short:   "Query 16: Find shortest path between two nodes",
		Args:    cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			QuerySixteenAction(args[0], args[1])
		},
	}
)

// PathNode represents a node in a path with its distance and path history
type PathNode struct {
	Node     string
	Distance int
	Path     []string
}

// Query 16: Find shortest path between two nodes
func QuerySixteenAction(fromNode, toNode string) {
	session := cassandra_client.GetSession()
	defer session.Close()

	path, distance := findShortestPath(session, fromNode, toNode)

	if len(path) == 0 {
		fmt.Printf("No path found between %s and %s\n", fromNode, toNode)
		return
	}

	fmt.Printf("Shortest path length: %d\n", distance)
	fmt.Printf("Path: %s\n", strings.Join(path, " -> "))
}

// BFS implementation for shortest path (undirected graph)
func findShortestPath(session *gocql.Session, fromNode, toNode string) ([]string, int) {
	if fromNode == toNode {
		return []string{fromNode}, 0
	}

	visited := make(map[string]bool)
	queue := []PathNode{{Node: fromNode, Distance: 0, Path: []string{fromNode}}}
	visited[fromNode] = true

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		// Get all neighbors (both directions since graph is undirected)
		neighbors := getNeighbors(session, current.Node)

		for _, neighbor := range neighbors {
			if neighbor == toNode {
				// Found target
				path := append(current.Path, neighbor)
				return path, current.Distance + 1
			}

			if !visited[neighbor] {
				visited[neighbor] = true
				newPath := make([]string, len(current.Path))
				copy(newPath, current.Path)
				newPath = append(newPath, neighbor)

				queue = append(queue, PathNode{
					Node:     neighbor,
					Distance: current.Distance + 1,
					Path:     newPath,
				})
			}
		}
	}

	return []string{}, -1 // No path found
}

// Get all neighbors of a node (undirected graph)
func getNeighbors(session *gocql.Session, node string) []string {
	var neighbors []string
	iter := session.Query("SELECT to_node FROM edges_bidirectional WHERE from_node = ?", node).Iter()
	var toNode string
	for iter.Scan(&toNode) {
		neighbors = append(neighbors, toNode)
	}
	iter.Close()
	return neighbors
}
