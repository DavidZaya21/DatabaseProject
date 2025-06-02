package cmd

import (
	"fmt"
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
	QuerySevenNode string
	QuerySevenCmd  = &cobra.Command{
		Use:     "seven",
		Aliases: []string{"seven"},
		Short:   color.GreenString("Find all grandchildren of given node"),
		Run: func(cmd *cobra.Command, args []string) {
			QuerySevenAction()
		},
	}
)

func QuerySevenAction() {
	// TODO: implement query 7
	if QuerySevenNode == "" {
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

	node := QuerySevenNode
	directSuccessors := make(map[string]bool)
	grandchildren := make(map[string]bool)
	skipped := 0

	// Step 1: Get direct successors
	iter := session.Query(fmt.Sprintf("SELECT to_node FROM edges WHERE from_node = '%s';", node)).Iter()
	var toNode string
	for iter.Scan(&toNode) {
		if toNode != node {
			directSuccessors[toNode] = true
		}
	}
	if err := iter.Close(); err != nil {
		log.Fatalf("❌ Error fetching direct successors: %v", err)
	}

	// Step 2: For each direct successor, get its successors
	for successor := range directSuccessors {
		subIter := session.Query(fmt.Sprintf("SELECT to_node FROM edges WHERE from_node = '%s';", successor)).Iter()
		for subIter.Scan(&toNode) {
			if toNode != node && !grandchildren[toNode] {
				grandchildren[toNode] = true
			} else {
				skipped++
			}
		}
		if err := subIter.Close(); err != nil {
			log.Printf("⚠️ Error reading grandchildren from %s: %v", successor, err)
		}
	}

	for child := range grandchildren {
		var label string
		query := fmt.Sprintf("SELECT label FROM node WHERE name = '%s';", child)
		iter := session.Query(query).Iter()
	
		for iter.Scan(&label) {
			fmt.Printf("Node: %s, Label: %s\n", child, label)
		}
	
		if err := iter.Close(); err != nil {
			log.Printf("Query error for child %s: %v", child, err)
		}
	}
	

	finalCount := len(grandchildren)

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
	color.Green("✅ Grandchildren query completed successfully.")
	// color.Green("Grandchildren: ")
	// for gc := range grandchildren {
	// 	color.Green("%s \n", gc)
	// }
	color.Cyan("📌 Unique grandchildren of %s: %d | Skipped: %d", node, finalCount, skipped)
	color.Yellow("⏱️  Wall Time: %s", duration)
	color.Yellow("⚙️  CPU Time (User): %s | (Sys): %s", cpuUserTime, cpuSysTime)
	color.Magenta("🧠 Memory Used: %.2f KB", float64(memUsed)/1024)
	color.Blue("🧹 GC Pause: %.2f ms", float64(gcPauseNs)/1e6)
	color.Cyan("📈 Throughput: %.2f rows/sec", throughput)
}
