package cmd

import (
	"fmt"
	"log"
	"runtime"
	"runtime/debug"
	"syscall"
	"time"

	"github.com/DavidZayar/cli/cassandra_client"
	"github.com/fatih/color"
	"github.com/spf13/cobra"
)

var (
	queryThreeNode string
	QueryThreeCmd  = &cobra.Command{
		Use:     "three",
		Aliases: []string{"three"},
		Short:   color.GreenString("Find all predecessors of a node"),
		Run: func(cmd *cobra.Command, args []string) {
			QueryThreeAction()
		},
	}
)

func QueryThreeAction() {
	if queryThreeNode == "" {
		log.Fatal("❌ You must provide a --to_node value")
	}

	query := fmt.Sprintf("SELECT from_node FROM edges WHERE to_node = '%s' allow filtering ;", queryThreeNode)

	color.Yellow("Creating the Session")
	session := cassandra_client.GetSession()
	defer session.Close()

	var startTime = time.Now()
	var rusageStart syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &rusageStart)

	var memStart runtime.MemStats
	runtime.ReadMemStats(&memStart)

	iter := session.Query(query).Iter()
	var fromNode string
	uniqueMap := make(map[string]bool)
	for iter.Scan(&fromNode) {
		fmt.Printf("Predecessors: %s \n", fromNode)
		uniqueMap[fromNode] = true
	}

	//for iter.Scan(&fromNode) {
	//	if !strings.EqualFold(fromNode, queryThreeNode) {
	//		color.Green(fmt.Sprintf("Successors of %s : %s ", queryThreeNode, fromNode))
	//		count++
	//	} else {
	//		skipped++
	//	}
	//}

	if err := iter.Close(); err != nil {
		log.Fatalf("❌ Error reading results: %v", err)
	}

	finalCount := len(uniqueMap)

	// Post-query profiling
	var endTime = time.Now()
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
	throughput := float64(finalCount) / duration.Seconds()

	// Results output
	color.Green("✅ Query completed successfully.")
	color.Cyan("📌 Unique predecessors (from_node): %d", finalCount)
	color.Yellow("⏱️  Wall Time: %s", duration)
	color.Yellow("⚙️  CPU Time (User): %s | (Sys): %s", cpuUserTime, cpuSysTime)
	color.Magenta("🧠 Memory Used: %.2f KB", float64(memUsed)/1024)
	color.Blue("🧹 GC Pause: %.2f ms", float64(gcPauseNs)/1e6)
	color.Cyan("📈 Throughput: %.2f rows/sec", throughput)

	debug.FreeOSMemory()
}
