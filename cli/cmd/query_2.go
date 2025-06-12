package cmd

import (
	"fmt"
	"log"

	"runtime"
	"strings"
	"syscall"
	"time"
	"github.com/DavidZayar/cli/cassandra_client"
	"github.com/fatih/color"
	"github.com/spf13/cobra"
)

var (
	QueryTwoFromNode string
	QueryTwoCmd = &cobra.Command{
		Use:     "two",
		Aliases: []string{"two"},
		Short:   color.GreenString("Count all distinct successors of a given node"),
		Run: func(cmd *cobra.Command, args []string) {
			QueryTwoAction()
		},
	}
)

func QueryTwoAction() {
	if QueryTwoFromNode == "" {
		log.Fatal("You must provide a --from_node value")
	}

	session := cassandra_client.GetSession()
	defer session.Close()

	// Start measurements
	startTime := time.Now()
	var rusageStart syscall.Rusage
	syscall.Getrusage(syscall.RUSAGE_SELF, &rusageStart)
	var memStart runtime.MemStats
	runtime.ReadMemStats(&memStart)

	// Execute query
	query := fmt.Sprintf("SELECT to_node FROM edges WHERE from_node = '%s';", QueryTwoFromNode)
	iter := session.Query(query).Iter()
	var toNode string
	uniqueMap := make(map[string]bool)
	skipped := 0

	for iter.Scan(&toNode) {
		if !strings.EqualFold(toNode, QueryTwoFromNode) {
			uniqueMap[toNode] = true
		} else {
			skipped++
		}
	}

	if err := iter.Close(); err != nil {
		log.Fatalf("Error reading results: %v", err)
	}

	finalCount := len(uniqueMap)

	// End measurements
	endTime := time.Now()
	var rusageEnd syscall.Rusage
	syscall.Getrusage(syscall.RUSAGE_SELF, &rusageEnd)
	var memEnd runtime.MemStats
	runtime.ReadMemStats(&memEnd)

	// Calculate metrics
	duration := endTime.Sub(startTime)
	cpuUserTime := time.Duration(rusageEnd.Utime.Nano() - rusageStart.Utime.Nano())
	cpuSysTime := time.Duration(rusageEnd.Stime.Nano() - rusageStart.Stime.Nano())
	memUsedKB := float64(memEnd.Alloc-memStart.Alloc) / 1024
	gcPauseMs := float64(memEnd.PauseTotalNs-memStart.PauseTotalNs) / 1e6
	throughput := float64(finalCount) / duration.Seconds()

	// Log query time to file
	logQueryTime(duration, "query_two")

	// Display results
	color.Green("Query completed successfully")
	color.Cyan("Unique successors: %d | Skipped: %d", finalCount, skipped)
	color.Yellow("Wall Time: %s", duration)
	color.Yellow("CPU Time - User: %s | Sys: %s", cpuUserTime, cpuSysTime)
	color.Magenta("Memory Used: %.2f KB", memUsedKB)
	color.Blue("GC Pause Total: %.2f ms", gcPauseMs)
	color.Blue("Throughput: %.2f unique nodes/sec", throughput)
}

