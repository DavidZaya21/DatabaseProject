package cmd

import (
	"github.com/DavidZayar/cli/cassandra_client"
	"github.com/fatih/color"
	"github.com/spf13/cobra"
	"log"
	"runtime"
	"runtime/debug"
	"syscall"
	"time"
)

var QueryTwelveCmd = &cobra.Command{
	Use:     "twelve",
	Aliases: []string{"twelve"},
	Short:   color.GreenString("Finding the node with the most neighbors"),
	Run: func(cmd *cobra.Command, args []string) {
		QueryTwelveAction()
	},
}

func QueryTwelveAction() {
	// TODO: implement query 12
	color.Yellow("🔌 Creating the Session")
	session := cassandra_client.GetSession()
	defer session.Close()

	startTime := time.Now()
	var rusageStart syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &rusageStart)

	var memStart runtime.MemStats
	runtime.ReadMemStats(&memStart)

	// Step 1: Collect neighbors
	neighbors := make(map[string]map[string]bool)

	iter := session.Query("SELECT from_node, to_node FROM edges allow filtering ").Iter()
	var fromNode, toNode string
	for iter.Scan(&fromNode, &toNode) {
		if fromNode != toNode {
			if neighbors[fromNode] == nil {
				neighbors[fromNode] = make(map[string]bool)
			}
			if neighbors[toNode] == nil {
				neighbors[toNode] = make(map[string]bool)
			}
			neighbors[fromNode][toNode] = true
			neighbors[toNode][fromNode] = true
		}
	}
	if err := iter.Close(); err != nil {
		log.Fatalf("❌ Error reading edges: %v", err)
	}

	// Step 2: Find max neighbor count
	maxCount := 0
	for _, set := range neighbors {
		if len(set) > maxCount {
			maxCount = len(set)
		}
	}

	// Step 3: Find nodes with that count
	var mostConnected []string
	for node, set := range neighbors {
		if len(set) == maxCount {
			mostConnected = append(mostConnected, node)
		}
	}

	endTime := time.Now()
	var rusageEnd syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &rusageEnd)

	var memEnd runtime.MemStats
	runtime.ReadMemStats(&memEnd)
	debug.FreeOSMemory()

	duration := endTime.Sub(startTime)
	cpuUserTime := time.Duration(rusageEnd.Utime.Nano() - rusageStart.Utime.Nano())
	cpuSysTime := time.Duration(rusageEnd.Stime.Nano() - rusageStart.Stime.Nano())
	memUsed := memEnd.Alloc - memStart.Alloc
	gcPauseNs := memEnd.PauseTotalNs - memStart.PauseTotalNs
	throughput := float64(len(neighbors)) / duration.Seconds()
	logQueryTime(duration, "query_twelve")
	// Output
	color.Green("✅ Query completed successfully.")
	color.Cyan("📌 Max unique neighbor count: %d", maxCount)
	color.Cyan("🌐 Nodes with the most neighbors:")
	for _, node := range mostConnected {
		color.Green("%s", node)
	}
	color.Yellow("⏱️  Wall Time: %s", duration)
	color.Yellow("⚙️  CPU Time (User): %s | (Sys): %s", cpuUserTime, cpuSysTime)
	color.Magenta("🧠 Memory Used: %.2f KB", float64(memUsed)/1024)
	color.Blue("🧹 GC Pause: %.2f ms", float64(gcPauseNs)/1e6)
	color.Cyan("📈 Throughput: %.2f nodes/sec", throughput)
}
