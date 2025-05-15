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

var queryTwoTemplate = `
SELECT to_node FROM edges WHERE from_node = '/c/en/value' ALLOW FILTERING;
`

var QueryTwoCmd = &cobra.Command{
	Use:     "two",
	Aliases: []string{"two"},
	Short:   color.GreenString("Count all distinct successors of a given node"),
	Run: func(cmd *cobra.Command, args []string) {
		QueryTwoAction()
	},
}

func QueryTwoAction() {
	color.Yellow("Creating the Session")
	session := cassandra_client.GetSession()
	defer session.Close()

	startTime := time.Now()
	var rusageStart syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &rusageStart)

	var memStart runtime.MemStats
	runtime.ReadMemStats(&memStart)

	iter := session.Query(queryTwoTemplate).Iter()
	var toNode string
	uniqueMap := make(map[string]bool)
	skipped := 0

	for iter.Scan(&toNode) {
		if toNode != "/c/en/value" {
			fmt.Printf("successors: %s \n", toNode)
			uniqueMap[toNode] = true
		} else {
			skipped++
		}
	}

	if err := iter.Close(); err != nil {
		log.Fatalf("❌ Error reading results: %v", err)
	}

	finalCount := len(uniqueMap)

	endTime := time.Now()
	var rusageEnd syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &rusageEnd)

	var memEnd runtime.MemStats
	runtime.ReadMemStats(&memEnd)

	debug.FreeOSMemory()

	// Metrics
	duration := endTime.Sub(startTime)
	cpuUserTime := time.Duration(rusageEnd.Utime.Nano() - rusageStart.Utime.Nano())
	cpuSysTime := time.Duration(rusageEnd.Stime.Nano() - rusageStart.Stime.Nano())
	memUsedKB := float64(memEnd.Alloc-memStart.Alloc) / 1024
	gcPauseMs := float64(memEnd.PauseTotalNs-memStart.PauseTotalNs) / 1e6

	// Output
	//color.Green("\n✅ Counted unique successors for node: %s", node)
	color.Cyan("🔢 Unique successors: %d | Skipped: %d", finalCount, skipped)
	fmt.Printf("⏱️  Wall Time: %s\n", duration)
	fmt.Printf("⚙️  CPU Time - User: %s | Sys: %s\n", cpuUserTime, cpuSysTime)
	fmt.Printf("🧠 Memory Used: %.2f KB\n", memUsedKB)
	fmt.Printf("🧹 GC Pause Total: %.2f ms\n", gcPauseMs)
}
