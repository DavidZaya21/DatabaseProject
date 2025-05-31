package cmd

import (
	"fmt"
	"log"
	"runtime"
	"runtime/debug"

	// "strings"
	"syscall"
	"time"

	"github.com/DavidZayar/cli/cassandra_client"
	"github.com/fatih/color"
	"github.com/spf13/cobra"
)

var (
	QueryOneNode string
	QueryOneCmd  = &cobra.Command{
		Use:     "one",
		Aliases: []string{"one"},
		Short:   color.GreenString("High-performance query to Cassandra with metrics"),
		Run: func(cmd *cobra.Command, args []string) {
			QueryOneAction()
		},
	}
)

func QueryOneAction() {
	if QueryOneNode == "" {
		log.Fatal("❌ You must provide a --from_node value")
	}

	query := fmt.Sprintf("SELECT to_node FROM edges WHERE from_node = '%s';", QueryOneNode)

	color.Yellow("Creating the Session")
	session := cassandra_client.GetSession()
	defer session.Close()

	startTime := time.Now()
	var rusageStart syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &rusageStart)

	var memStart runtime.MemStats
	runtime.ReadMemStats(&memStart)

	iter := session.Query(query).Iter()
	var toNode string
	uniqueMap := make(map[string]bool)

	count := 0
	for iter.Scan(&toNode) {
		if !uniqueMap[toNode] {
			uniqueMap[toNode] = true
			count++
		}
	}

	if err := iter.Close(); err != nil {
		log.Fatalf("❌ Error reading results: %v", err)
	}

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
	throughput := float64(count) / duration.Seconds()

	// Output successors
	color.White("Successors of node '%s':", QueryOneNode)
	for node := range uniqueMap {
		color.Green("%s", node)
	}

	// Results output
	color.Green("✅ Query completed successfully.")
	color.Cyan("📌 Successors found: %d", count)
	color.Yellow("⏱️  Wall Time: %s", duration)
	color.Yellow("⚙️  CPU Time (User): %s | (Sys): %s", cpuUserTime, cpuSysTime)
	color.Magenta("🧠 Memory Used: %.2f KB", float64(memUsed)/1024)
	color.Blue("🧹 GC Pause: %.2f ms", float64(gcPauseNs)/1e6)
	color.Cyan("📈 Throughput: %.2f rows/sec", throughput)

	debug.FreeOSMemory()
}
