package cmd

import (
	"fmt"
	"strings"

	"github.com/DavidZayar/cli/cassandra_client"
	"github.com/gocql/gocql"
	"github.com/spf13/cobra"
)

var (
	QueryEighteenCmd = &cobra.Command{
		Use:     "eighteen <node> <distance>",
		Aliases: []string{"eighteen"},
		Short:   "Query 18: Find distant antonyms at specified distance",
		Args:    cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			QueryEighteenAction(args[0], args[1])
		},
	}
)

// RelationType represents the semantic relationship type for Q18
type RelationTypeQ18 int

const (
	SynonymQ18 RelationTypeQ18 = iota
	AntonymQ18
)

// PathResultQ18 represents a result with node and path information
type PathResultQ18 struct {
	Node string
	Path []string
}

// Query 18: Find distant antonyms at specified distance
func QueryEighteenAction(node, distanceStr string) {
	session := cassandra_client.GetSession()
	defer session.Close()

	distance := parseDistanceQ18(distanceStr)
	if distance <= 0 {
		fmt.Println("Distance must be a positive integer")
		return
	}

	antonyms := findDistantAntonyms(session, node, distance)

	if len(antonyms) == 0 {
		fmt.Printf("No distant antonyms found for %s at distance %d\n", node, distance)
		return
	}

	fmt.Printf("Distant antonyms of %s at distance %d:\n", node, distance)
	for _, result := range antonyms {
		fmt.Printf("- %s (path: %s)\n", result.Node, strings.Join(result.Path, " -> "))
	}
}

// Helper function to parse distance string to int
func parseDistanceQ18(distanceStr string) int {
	var distance int
	if _, err := fmt.Sscanf(distanceStr, "%d", &distance); err != nil {
		return 0
	}
	return distance
}

// Find distant antonyms using BFS with relation type tracking
func findDistantAntonyms(session *gocql.Session, startNode string, targetDistance int) []PathResultQ18 {
	var results []PathResultQ18

	// Queue item: node, distance, path, current_relation_type
	type QueueItem struct {
		Node         string
		Distance     int
		Path         []string
		RelationType RelationTypeQ18
	}

	visited := make(map[string]map[int]bool) // node -> distance -> visited
	queue := []QueueItem{{Node: startNode, Distance: 0, Path: []string{startNode}, RelationType: SynonymQ18}}

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		if current.Distance == targetDistance {
			if current.RelationType == AntonymQ18 {
				results = append(results, PathResultQ18{
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
		synonymNeighbors := getSynonymAntonymNeighborsQ18(session, current.Node, "/r/Synonym")
		antonymNeighbors := getSynonymAntonymNeighborsQ18(session, current.Node, "/r/Antonym")

		// Process synonym neighbors
		for _, neighbor := range synonymNeighbors {
			if !isVisitedQ18(visited, neighbor, current.Distance+1) && !containsQ18(current.Path, neighbor) {
				setVisitedQ18(visited, neighbor, current.Distance+1)

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
			if !isVisitedQ18(visited, neighbor, current.Distance+1) && !containsQ18(current.Path, neighbor) {
				setVisitedQ18(visited, neighbor, current.Distance+1)

				newPath := make([]string, len(current.Path))
				copy(newPath, current.Path)
				newPath = append(newPath, neighbor)

				// Antonym flips the relation type
				var newRelationType RelationTypeQ18
				if current.RelationType == SynonymQ18 {
					newRelationType = AntonymQ18
				} else {
					newRelationType = SynonymQ18
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
func getSynonymAntonymNeighborsQ18(session *gocql.Session, node, relation string) []string {
	var neighbors []string

	// Query edges where node is from_node
	iter := session.Query("SELECT to_node FROM edges WHERE from_node = ? AND relation = ?", node, relation).Iter()
	var toNode string
	for iter.Scan(&toNode) {
		neighbors = append(neighbors, toNode)
	}
	iter.Close()

	// Query edges where node is to_node (for undirected graph)
	iter = session.Query("SELECT from_node FROM edges WHERE to_node = ? AND relation = ?", node, relation).Iter()
	var fromNode string
	for iter.Scan(&fromNode) {
		neighbors = append(neighbors, fromNode)
	}
	iter.Close()

	return neighbors
}

func isVisitedQ18(visited map[string]map[int]bool, node string, distance int) bool {
	if nodeMap, exists := visited[node]; exists {
		return nodeMap[distance]
	}
	return false
}

func setVisitedQ18(visited map[string]map[int]bool, node string, distance int) {
	if _, exists := visited[node]; !exists {
		visited[node] = make(map[int]bool)
	}
	visited[node][distance] = true
}

func containsQ18(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}
