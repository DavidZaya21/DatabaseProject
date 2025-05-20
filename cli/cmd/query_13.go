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

var QueryThirteenCmd = &cobra.Command{
	Use:     "thirteen",
	Aliases: []string{"thirteen"},
	Short:   color.GreenString("Counting nodes with a single neighbor"),
	Run: func(cmd *cobra.Command, args []string) {
		QueryThirteenAction()
	},
}

func QueryThirteenAction() {
	// TODO: implement query 13
	color.Yellow("🔌 Creating the Session")
	session := cassandra_client.GetSession()
	defer session.Close()

	startTime := time.Now()
	var rusageStart syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &rusageStart)

	var memStart runtime.MemStats
	runtime.ReadMemStats(&memStart)

	neighbors := make(map[string]map[string]bool)

	iter := session.Query("SELECT from_node, to_node FROM edges").Iter()
	var from, to string
	for iter.Scan(&from, &to) {
		if neighbors[from] == nil {
			neighbors[from] = make(map[string]bool)
		}
		if neighbors[to] == nil {
			neighbors[to] = make(map[string]bool)
		}
		neighbors[from][to] = true
		neighbors[to][from] = true
	}
	if err := iter.Close(); err != nil {
		log.Fatalf("❌ Failed reading edges: %v", err)
	}

	singleNeighborCount := 0
	for _, nset := range neighbors {
		if len(nset) == 1 {
			singleNeighborCount++
		}
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
	throughput := float64(singleNeighborCount) / duration.Seconds()

	color.Green("✅ Query completed successfully.")
	color.Cyan("📌 Nodes with exactly 1 unique neighbor: %d", singleNeighborCount)
	color.Yellow("⏱️  Wall Time: %s", duration)
	color.Yellow("⚙️  CPU Time (User): %s | (Sys): %s", cpuUserTime, cpuSysTime)
	color.Magenta("🧠 Memory Used: %.2f KB", float64(memUsed)/1024)
	color.Blue("🧹 GC Pause: %.2f ms", float64(gcPauseNs)/1e6)
	color.Cyan("📈 Throughput: %.2f nodes/sec", throughput)
}
