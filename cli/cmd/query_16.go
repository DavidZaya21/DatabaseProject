package cmd

import (
	"runtime"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	"github.com/DavidZayar/cli/cassandra_client"
	"github.com/fatih/color"
	"github.com/gocql/gocql"
	"github.com/spf13/cobra"
)

var (
	QuerySixteenCmd = &cobra.Command{
		Use:     "sixteen <from_node> <to_node>",
		Aliases: []string{"sixteen"},
		Short:   "Find shortest path between two nodes with performance metrics",
		Args:    cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			QuerySixteenAction(args[0], args[1])
		},
	}
)

type PathNode struct {
	Node     string
	Distance int
	Path     []string
}

func QuerySixteenAction(fromNode, toNode string) {
	session := cassandra_client.GetSession()
	defer session.Close()

	// Start performance tracking
	startTime := time.Now()
	var rusageStart syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &rusageStart)

	var memStart runtime.MemStats
	runtime.ReadMemStats(&memStart)

	// Perform BFS
	path, distance, nodesVisited := findShortestPath(session, fromNode, toNode)

	// End performance tracking
	endTime := time.Now()
	var rusageEnd syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &rusageEnd)

	var memEnd runtime.MemStats
	runtime.ReadMemStats(&memEnd)

	// Metrics calculations
	duration := endTime.Sub(startTime)
	cpuUserTime := time.Duration(rusageEnd.Utime.Nano() - rusageStart.Utime.Nano())
	cpuSysTime := time.Duration(rusageEnd.Stime.Nano() - rusageStart.Stime.Nano())
	memUsed := memEnd.Alloc - memStart.Alloc
	gcPauseNs := memEnd.PauseTotalNs - memStart.PauseTotalNs
	throughput := float64(nodesVisited) / duration.Seconds()

	// Output results
	if len(path) == 0 {
		color.Red("❌ No path found between %s and %s", fromNode, toNode)
	} else {
		color.Green("✅ Shortest path found")
		color.Cyan("🔗 Length: %d", distance)
		color.Yellow("🛣️  Path: %s", strings.Join(path, " -> "))
	}

	// Output metrics
	color.Yellow("⏱️  Wall Time: %s", duration)
	color.Yellow("⚙️  CPU Time (User): %s | (Sys): %s", cpuUserTime, cpuSysTime)
	color.Magenta("🧠 Memory Used: %.2f KB", float64(memUsed)/1024)
	color.Blue("🧹 GC Pause: %.2f ms", float64(gcPauseNs)/1e6)
	color.Cyan("📈 Nodes Visited: %d | Throughput: %.2f nodes/sec", nodesVisited, throughput)

	debug.FreeOSMemory()
}

// Modified to return number of visited nodes too
func findShortestPath(session *gocql.Session, fromNode, toNode string) ([]string, int, int) {
	if fromNode == toNode {
		return []string{fromNode}, 0, 1
	}

	visited := make(map[string]bool)
	queue := []PathNode{{Node: fromNode, Distance: 0, Path: []string{fromNode}}}
	visited[fromNode] = true
	nodesVisited := 1

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		neighbors := getNeighbors(session, current.Node)

		for _, neighbor := range neighbors {
			if neighbor == toNode {
				path := append(current.Path, neighbor)
				return path, current.Distance + 1, nodesVisited + 1
			}
			if !visited[neighbor] {
				visited[neighbor] = true
				nodesVisited++

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

	return []string{}, -1, nodesVisited
}

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
