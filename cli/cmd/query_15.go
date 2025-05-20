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
	QueryFifteenNode string
	QueryFifteenCmd  = &cobra.Command{
		Use:     "fifteen",
		Aliases: []string{"fifteen"},
		Short:   color.GreenString("Finding all similar nodes for given node"),
		Run: func(cmd *cobra.Command, args []string) {
			QueryFifteenAction()
		},
	}
)

func QueryFifteenAction() {
	// TODO: implement query 15
	if QueryFifteenNode == "" {
		log.Fatal("❌ You must provide --node flag")
	}

	session := cassandra_client.GetSession()
	defer session.Close()

	startTime := time.Now()
	var rusageStart syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &rusageStart)

	var memStart runtime.MemStats
	runtime.ReadMemStats(&memStart)

	node := QueryFifteenNode
	similarNodes := make(map[string]bool)

	// 1. Find parents of node with edge_type
	iterParents := session.Query(`SELECT from_node, relation FROM edges WHERE to_node = ? ALLOW FILTERING`, node).Iter()
	var parent, edgeType string
	for iterParents.Scan(&parent, &edgeType) {
		// Optionally filter by label if needed

		// Find all children of parent with the same edge_type except original node
		iterChildren := session.Query(`SELECT to_node FROM edges WHERE from_node = ? AND relation = ? ALLOW FILTERING`, parent, edgeType).Iter()
		var child string
		for iterChildren.Scan(&child) {
			if child != node {
				similarNodes[child] = true
			}
		}
		iterChildren.Close()
	}
	iterParents.Close()

	iterChild := session.Query(`SELECT to_node, relation FROM edges WHERE from_node = ? ALLOW FILTERING`, node).Iter()
	var childNode, edgeType2 string
	for iterChild.Scan(&childNode, &edgeType2) {
		// Find all other parents of this child with the same relation except original node
		iterSiblings := session.Query(`SELECT from_node FROM edges WHERE to_node = ? AND relation = ? ALLOW FILTERING`, childNode, edgeType2).Iter()
		var sibling string
		for iterSiblings.Scan(&sibling) {
			if sibling != node {
				similarNodes[sibling] = true
			}
		}
		iterSiblings.Close()
	}
	iterChild.Close()

	count := len(similarNodes)

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
	throughput := float64(len(similarNodes)) / duration.Seconds()

	//// Print results
	color.Green("✅ Similar nodes = %d", count)
	for n := range similarNodes {
		color.Green("%s\n", n)
	}

	color.Yellow("⏱️  Wall Time: %s", duration)
	color.Yellow("⚙️  CPU Time (User): %s | (Sys): %s", cpuUserTime, cpuSysTime)
	color.Magenta("🧠 Memory Used: %.2f KB", float64(memUsed)/1024)
	color.Blue("🧹 GC Pause: %.2f ms", float64(gcPauseNs)/1e6)
	color.Cyan("📈 Throughput: %.2f ops/sec", throughput)
}
