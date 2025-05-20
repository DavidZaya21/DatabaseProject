package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
	"log"
	"runtime"
	"runtime/debug"
	"syscall"
	"time"

	"github.com/DavidZayar/cli/cassandra_client"
	"github.com/fatih/color"
)

var (
	QuerySixNode string
	QuerySixCmd  = &cobra.Command{
		Use:     "six",
		Aliases: []string{"six"},
		Short:   color.GreenString("Count all neighbors of given node"),
		Run: func(cmd *cobra.Command, args []string) {
			QuerySixAction()
		},
	}
)

func QuerySixAction() {
	// TODO: implement query 6
	if QuerySixNode == "" {
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

	neighbors := make(map[string]bool)
	var count, skipped int

	color.Cyan("🔍 Querying successors...")
	succIter := session.Query(fmt.Sprintf("SELECT to_node FROM edges WHERE from_node = '%s';", QuerySixNode)).Iter()
	var toNode string
	for succIter.Scan(&toNode) {
		if toNode != QuerySixNode {
			if !neighbors[toNode] {
				neighbors[toNode] = true
				count++
			}
		} else {
			skipped++
		}
	}
	if err := succIter.Close(); err != nil {
		log.Fatalf("❌ Error reading successors: %v", err)
	}

	color.Cyan("🔍 Querying predecessors...")
	predIter := session.Query(fmt.Sprintf("SELECT from_node FROM edges WHERE to_node = '%s' ALLOW FILTERING;", QuerySixNode)).Iter()
	var fromNode string
	for predIter.Scan(&fromNode) {
		if fromNode != QuerySixNode {
			if !neighbors[fromNode] {
				neighbors[fromNode] = true
				count++
			}
		} else {
			skipped++
		}
	}
	if err := predIter.Close(); err != nil {
		log.Fatalf("❌ Error reading predecessors: %v", err)
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
	throughput := float64(count) / duration.Seconds()

	color.Green("✅ Neighbor query completed successfully.")
	color.Cyan("📌 Total unique neighbors: %d | Skipped: %d", count, skipped)
	color.Yellow("⏱️  Wall Time: %s", duration)
	color.Yellow("⚙️  CPU Time (User): %s | (Sys): %s", cpuUserTime, cpuSysTime)
	color.Magenta("🧠 Memory Used: %.2f KB", float64(memUsed)/1024)
	color.Blue("🧹 GC Pause: %.2f ms", float64(gcPauseNs)/1e6)
	color.Cyan("📈 Throughput: %.2f rows/sec", throughput)
}
