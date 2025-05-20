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

var QueryNineCmd = &cobra.Command{
	Use:     "nine",
	Aliases: []string{"nine"},
	Short:   color.GreenString("Count total number of nodes"),
	Run: func(cmd *cobra.Command, args []string) {
		QueryNineAction()
	},
}

func QueryNineAction() {
	// TODO: implement query 9
	color.Yellow("🔌 Creating the Session")
	session := cassandra_client.GetSession()
	defer session.Close()

	startTime := time.Now()
	var rusageStart syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &rusageStart)

	var memStart runtime.MemStats
	runtime.ReadMemStats(&memStart)

	// Cassandra doesn't support COUNT(*) efficiently for large datasets
	// So we iterate manually and count
	iter := session.Query("SELECT label FROM node").Iter()
	var label string
	totalCount := 0

	for iter.Scan(&label) {
		totalCount++
	}
	if err := iter.Close(); err != nil {
		log.Fatalf("❌ Error fetching nodes: %v", err)
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
	throughput := float64(totalCount) / duration.Seconds()

	// Summary
	color.Green("✅ Node count query completed successfully.")
	color.Cyan("📌 Total nodes: %d", totalCount)
	color.Yellow("⏱️  Wall Time: %s", duration)
	color.Yellow("⚙️  CPU Time (User): %s | (Sys): %s", cpuUserTime, cpuSysTime)
	color.Magenta("🧠 Memory Used: %.2f KB", float64(memUsed)/1024)
	color.Blue("🧹 GC Pause: %.2f ms", float64(gcPauseNs)/1e6)
	color.Cyan("📈 Throughput: %.2f rows/sec", throughput)
}
