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

var QueryTenCmd = &cobra.Command{
	Use:     "ten",
	Aliases: []string{"ten"},
	Short:   color.GreenString("Counting all nodes without successors"),
	Run: func(cmd *cobra.Command, args []string) {
		QueryTenAction()
	},
}

func QueryTenAction() {
	// TODO: implement query 10
	color.Yellow("🔌 Creating the Session")
	session := cassandra_client.GetSession()
	defer session.Close()

	startTime := time.Now()
	var rusageStart syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &rusageStart)

	var memStart runtime.MemStats
	runtime.ReadMemStats(&memStart)

	// Step 1: Fetch all nodes
	iter := session.Query("SELECT name FROM node allow filtering").Iter()
	var label string
	allNodes := make(map[string]bool)
	for iter.Scan(&label) {
		allNodes[label] = true
	}
	if err := iter.Close(); err != nil {
		log.Fatalf("❌ Error fetching nodes: %v", err)
	}

	// Step 2: Fetch all from_node (nodes with successors)
	iter = session.Query("SELECT from_node FROM edges").Iter()
	var fromNode string
	for iter.Scan(&fromNode) {
		delete(allNodes, fromNode)
	}
	if err := iter.Close(); err != nil {
		log.Fatalf("❌ Error fetching successors: %v", err)
	}

	// Step 3: Count remaining nodes (nodes without successors)
	totalCount := len(allNodes)

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
	throughput := float64(totalCount) / duration.Seconds()

	color.Green("✅ Query completed successfully.")
	color.Cyan("Nodes without successors: %d", totalCount)
	color.Yellow("⏱️  Wall Time: %s", duration)
	color.Yellow("⚙️  CPU Time (User): %s | (Sys): %s", cpuUserTime, cpuSysTime)
	color.Magenta("🧠 Memory Used: %.2f KB", float64(memUsed)/1024)
	color.Blue("🧹 GC Pause: %.2f ms", float64(gcPauseNs)/1e6)
	color.Cyan("📈 Throughput: %.2f rows/sec", throughput)
}
