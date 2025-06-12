package cmd

import (
	"fmt"
	"log"
	"os"
	"runtime"
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
		log.Fatal("You must provide a --from_node value")
	}

	query := fmt.Sprintf("SELECT to_node FROM edges WHERE from_node = '%s';", QueryOneNode)

	session := cassandra_client.GetSession()
	defer session.Close()

	// Start measurements
	startTime := time.Now()
	var rusageStart syscall.Rusage
	syscall.Getrusage(syscall.RUSAGE_SELF, &rusageStart)
	var memStart runtime.MemStats
	runtime.ReadMemStats(&memStart)

	// Execute query
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
		log.Fatalf("Error reading results: %v", err)
	}

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
	memUsed := memEnd.Alloc - memStart.Alloc
	throughput := float64(count) / duration.Seconds()
	logQueryTime(duration, "query_one")
	color.White("Successors of node '%s':", QueryOneNode)
	for node := range uniqueMap {
		color.Green("%s", node)
	}
	color.Green("\nQuery completed successfully")
	color.Cyan("Successors found: %d", count)
	color.Yellow("Wall Time: %s", duration)
	color.Yellow("CPU Time (User): %s | (Sys): %s", cpuUserTime, cpuSysTime)
	color.Magenta("Memory Used: %.2f KB", float64(memUsed)/1024)
	color.Blue("Throughput: %.2f rows/sec", throughput)
}

func logQueryTime(duration time.Duration, queryNumber string) {
	file, err := os.OpenFile("./times.csv", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("Warning: Could not open times.txt: %v", err)
		return
	}
	defer file.Close()

	timestamp := time.Now().Format("2006-01-02 15:04:05")
	logEntry := fmt.Sprintf("%s, %s, %s\n", queryNumber, timestamp, duration)

	if _, err := file.WriteString(logEntry); err != nil {
		log.Printf("Warning: Could not write to times.txt: %v", err)
	}
}