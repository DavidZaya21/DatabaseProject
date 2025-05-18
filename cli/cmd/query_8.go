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

var (
	QueryEightNode string
	QueryEightCmd  = &cobra.Command{
		Use:     "eight",
		Aliases: []string{"eight"},
		Short:   color.GreenString("Find all grandparents of given node"),
		Run: func(cmd *cobra.Command, args []string) {
			QueryEightAction()
		},
	}
)

func QueryEightAction() {
	// TODO: implement query 8
	if QueryEightNode == "" {
		log.Fatal("❌ You must provide a --node value")
	}

	color.Yellow("🔌 Creating the Session")
	session := cassandra_client.GetSession()
	defer session.Close()

	startTime := time.Now()
	var rusageStart syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &rusageStart)

	var memStart runtime.MemStats
	runtime.ReadMemStats(&memStart)

	node := QueryEightNode
	directPredecessors := make(map[string]bool)
	grandparents := make(map[string]bool)

	// Step 1: Get direct predecessors of the node
	iter := session.Query("SELECT from_node FROM edges WHERE to_node = ? ALLOW FILTERING", node).Iter()
	var fromNode string
	for iter.Scan(&fromNode) {
		color.Green("Direct predecessor: %s", fromNode)
		directPredecessors[fromNode] = true
	}
	if err := iter.Close(); err != nil {
		log.Fatalf("❌ Error fetching direct predecessors: %v", err)
	}

	// Step 2: Get grandparents of the node
	for predecessor := range directPredecessors {
		subIter := session.Query("SELECT from_node FROM edges WHERE to_node = ? ALLOW FILTERING", predecessor).Iter()
		for subIter.Scan(&fromNode) {
			grandparents[fromNode] = true
		}
		if err := subIter.Close(); err != nil {
			log.Printf("⚠️ Error reading grandparents from %s: %v", predecessor, err)
		}
	}

	finalCount := len(grandparents)

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
	throughput := float64(finalCount) / duration.Seconds()

	// Summary
	color.Green("✅ Grandparents query completed successfully.")
	color.Green("Grandparents: ")
	for gp := range grandparents {
		color.Green("%s", gp)
	}
	color.Cyan("📌 Unique grandparents of %s: %d", node, finalCount)
	color.Yellow("⏱️  Wall Time: %s", duration)
	color.Yellow("⚙️  CPU Time (User): %s | (Sys): %s", cpuUserTime, cpuSysTime)
	color.Magenta("🧠 Memory Used: %.2f KB", float64(memUsed)/1024)
	color.Blue("🧹 GC Pause: %.2f ms", float64(gcPauseNs)/1e6)
	color.Cyan("📈 Throughput: %.2f rows/sec", throughput)
}
