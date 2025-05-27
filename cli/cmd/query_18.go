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

const (
	SynonymQ18 = "synonym"
	AntonymQ18 = "antonym"
)

type PathResultQ18 struct {
	Node string
	Path []string
}

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

func parseDistanceQ18(distanceStr string) int {
	var distance int
	if _, err := fmt.Sscanf(distanceStr, "%d", &distance); err != nil {
		return 0
	}
	return distance
}

func findDistantAntonyms(session *gocql.Session, startNode string, targetDistance int) []PathResultQ18 {
	type QueueItem struct {
		Node         string
		Distance     int
		Path         []string
		RelationType string
	}

	var results []PathResultQ18
	visited := make(map[string]map[int]bool)
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

		synonymNeighbors := getNeighborsQ18(session, current.Node, SynonymQ18)
		antonymNeighbors := getNeighborsQ18(session, current.Node, AntonymQ18)

		// Process synonym neighbors
		for _, neighbor := range synonymNeighbors {
			if !isVisitedQ18(visited, neighbor, current.Distance+1) && !containsQ18(current.Path, neighbor) {
				setVisitedQ18(visited, neighbor, current.Distance+1)

				newPath := append(append([]string{}, current.Path...), neighbor)
				newRelationType := current.RelationType // stays the same

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

				newPath := append(append([]string{}, current.Path...), neighbor)
				newRelationType := flipRelation(current.RelationType)

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

func flipRelation(current string) string {
	if current == SynonymQ18 {
		return AntonymQ18
	}
	return SynonymQ18
}

func getNeighborsQ18(session *gocql.Session, node, relation string) []string {
	var neighbors []string

	iter := session.Query("SELECT from_node FROM edges WHERE to_node = ? AND relation = ? allow filtering", node, relation).Iter()
	var fromNode string
	for iter.Scan(&fromNode) {
		neighbors = append(neighbors, fromNode)
	}
	iter.Close()

	iter = session.Query("SELECT to_node FROM edges WHERE from_node = ? AND relation = ?", node, relation).Iter()
	var toNode string
	for iter.Scan(&toNode) {
		neighbors = append(neighbors, toNode)
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
