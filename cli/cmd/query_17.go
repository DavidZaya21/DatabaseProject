package cmd

import (
	"fmt"
	"strings"

	"github.com/DavidZayar/cli/cassandra_client"
	"github.com/gocql/gocql"
	"github.com/spf13/cobra"
)

var (
	QuerySeventeenCmd = &cobra.Command{
		Use:     "seventeen <node> <distance>",
		Aliases: []string{"seventeen"},
		Short:   "Query 17: Find distant synonyms at specified distance",
		Args:    cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			QuerySeventeenAction(args[0], args[1])
		},
	}
)

// type RelationType 

const (
	Synonym = "synonym"
	Antonym = "antonym"
)

type PathResult struct {
	Node string
	Path []string
}

// Query 17: Find distant synonyms at specified distance
func QuerySeventeenAction(node, distanceStr string) {
	session := cassandra_client.GetSession()
	defer session.Close()

	distance := parseDistanceQ17(distanceStr)
	if distance <= 0 {
		fmt.Println("Distance must be a positive integer")
		return
	}

	synonyms := findDistantSynonyms(session, node, distance)

	if len(synonyms) == 0 {
		fmt.Printf("No distant synonyms found for %s at distance %d\n", node, distance)
		return
	}

	fmt.Printf("Distant synonyms of %s at distance %d:\n", node, distance)
	for _, result := range synonyms {
		fmt.Printf("- %s (path: %s)\n", result.Node, strings.Join(result.Path, " -> "))
	}
}

// Helper function to parse distance string to int
func parseDistanceQ17(distanceStr string) int {
	var distance int
	if _, err := fmt.Sscanf(distanceStr, "%d", &distance); err != nil {
		return 0
	}
	return distance
}

// Find distant synonyms using BFS with relation type tracking
func findDistantSynonyms(session *gocql.Session, startNode string, targetDistance int) []PathResult {
	var results []PathResult

	// Queue item: node, distance, path, current_relation_type
	type QueueItem struct {
		Node         string
		Distance     int
		Path         []string
		RelationType string
	}

	visited := make(map[string]map[int]bool) // node -> distance -> visited
	queue := []QueueItem{{Node: startNode, Distance: 0, Path: []string{startNode}, RelationType: Synonym}}

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		if current.Distance == targetDistance {
			if current.RelationType == Synonym {
				results = append(results, PathResult{
					Node: current.Node,
					Path: current.Path,
				})
			}
			continue
		}

		if current.Distance >= targetDistance {
			continue
		}

		// Get synonym and antonym neighbors
		synonymNeighbors := getSynonymAntonymNeighborsQ17(session, current.Node, "synonym")
		antonymNeighbors := getSynonymAntonymNeighborsQ17(session, current.Node, "antonym")

		// Process synonym neighbors
		for _, neighbor := range synonymNeighbors {
			if !isVisitedQ17(visited, neighbor, current.Distance+1) && !containsQ17(current.Path, neighbor) {
				setVisitedQ17(visited, neighbor, current.Distance+1)

				newPath := make([]string, len(current.Path))
				copy(newPath, current.Path)
				newPath = append(newPath, neighbor)

				// Synonym of synonym = synonym, synonym of antonym = antonym
				newRelationType := current.RelationType

				queue = append(queue, QueueItem{
					Node:         neighbor,
					Distance:     current.Distance + 1,
					Path:         newPath,
					RelationType: newRelationType,
				})
			}
		}

		// Process antonym neighbors
		for _, neighbor := range antonymNeighbors {
			if !isVisitedQ17(visited, neighbor, current.Distance+1) && !containsQ17(current.Path, neighbor) {
				setVisitedQ17(visited, neighbor, current.Distance+1)

				newPath := make([]string, len(current.Path))
				copy(newPath, current.Path)
				newPath = append(newPath, neighbor)

				// Antonym flips the relation type
				var newRelationType string
				if current.RelationType == Synonym {
					newRelationType = Antonym
				} else {
					newRelationType = Synonym
				}

				queue = append(queue, QueueItem{
					Node:         neighbor,
					Distance:     current.Distance + 1,
					Path:         newPath,
					RelationType: newRelationType,
				})
			}
		}
	}

	return results
}

// Get neighbors connected by synonym or antonym relations
func getSynonymAntonymNeighborsQ17(session *gocql.Session, node, relation string) []string {
	var neighbors []string

	// Query edges where node is from_node
	iter := session.Query("SELECT from_node FROM edges WHERE to_node = ? AND relation = ? allow filtering", node, relation).Iter()
	var fromNode string
	for iter.Scan(&fromNode) {
		neighbors = append(neighbors, fromNode)
	}
	iter.Close()

	// Query edges where node is to_node (for undirected graph)
	iter = session.Query("SELECT to_node FROM edges WHERE from_node = ? AND relation = ?", node, relation).Iter()
	var toNode string
	for iter.Scan(&toNode) {
		neighbors = append(neighbors, toNode)
	}
	iter.Close()

	return neighbors
}

func isVisitedQ17(visited map[string]map[int]bool, node string, distance int) bool {
	if nodeMap, exists := visited[node]; exists {
		return nodeMap[distance]
	}
	return false
}

func setVisitedQ17(visited map[string]map[int]bool, node string, distance int) {
	if _, exists := visited[node]; !exists {
		visited[node] = make(map[int]bool)
	}
	visited[node][distance] = true
}

func containsQ17(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}
