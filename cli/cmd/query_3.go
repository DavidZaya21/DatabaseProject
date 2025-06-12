package cmd

import (
	"fmt"
	"log"
	"runtime"
	"syscall"
	"time"
	"github.com/DavidZayar/cli/cassandra_client"
	"github.com/fatih/color"
	"github.com/spf13/cobra"
)

var (
	QueryThreeNode string
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
	if QueryThreeNode == "" {
		log.Fatal("You must provide a --to_node value")
	}

	session := cassandra_client.GetSession()
	defer session.Close()

	startTime := time.Now()
	var rusageStart syscall.Rusage
	syscall.Getrusage(syscall.RUSAGE_SELF, &rusageStart)
	var memStart runtime.MemStats
	runtime.ReadMemStats(&memStart)

	query := fmt.Sprintf("SELECT from_node FROM edges WHERE to_node = '%s' ALLOW FILTERING;", QueryThreeNode)
	iter := session.Query(query).Iter()
	var fromNode string
	uniqueMap := make(map[string]bool)

	for iter.Scan(&fromNode) {
		uniqueMap[fromNode] = true
	}

	if err := iter.Close(); err != nil {
		log.Fatalf("Error reading results: %v", err)
	}

	color.White("Predecessors of node '%s':", QueryThreeNode)
	for predecessor := range uniqueMap {
		var label string
		labelQuery := fmt.Sprintf("SELECT label FROM node WHERE name = '%s';", predecessor)
		labelIter := session.Query(labelQuery).Iter()

		hasLabel := false
		for labelIter.Scan(&label) {
			color.Green("Node: %s, Label: %s", predecessor, label)
			hasLabel = true
		}

		if !hasLabel {
			color.Green("Node: %s, Label: (no label found)", predecessor)
		}

		if err := labelIter.Close(); err != nil {
			log.Printf("Warning: Query error for predecessor %s: %v", predecessor, err)
		}
	}

	finalCount := len(uniqueMap)

	endTime := time.Now()
	var rusageEnd syscall.Rusage
	syscall.Getrusage(syscall.RUSAGE_SELF, &rusageEnd)
	var memEnd runtime.MemStats
	runtime.ReadMemStats(&memEnd)

	duration := endTime.Sub(startTime)
	cpuUserTime := time.Duration(rusageEnd.Utime.Nano() - rusageStart.Utime.Nano())
	cpuSysTime := time.Duration(rusageEnd.Stime.Nano() - rusageStart.Stime.Nano())
	memUsed := memEnd.Alloc - memStart.Alloc
	gcPauseNs := memEnd.PauseTotalNs - memStart.PauseTotalNs
	throughput := float64(finalCount) / duration.Seconds()

	logQueryTime(duration, "query_three")

	color.Green("\nQuery completed successfully")
	color.Cyan("Unique predecessors (from_node): %d", finalCount)
	color.Yellow("Wall Time: %s", duration)
	color.Yellow("CPU Time (User): %s | (Sys): %s", cpuUserTime, cpuSysTime)
	color.Magenta("Memory Used: %.2f KB", float64(memUsed)/1024)
	color.Blue("GC Pause: %.2f ms", float64(gcPauseNs)/1e6)
	color.Blue("Throughput: %.2f rows/sec", throughput)
}
