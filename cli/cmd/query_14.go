package cmd

import (
	"fmt"
	"log"
	"runtime"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	"github.com/DavidZayar/cli/cassandra_client"
	"github.com/fatih/color"
	"github.com/spf13/cobra"
)

var (
	QueryFourteenOldName string
	QueryFourteenNewName string
	QueryFourteenCmd     = &cobra.Command{
		Use:     "fourteen",
		Aliases: []string{"fourteen"},
		Short:   "Renaming the node",
		Run: func(cmd *cobra.Command, args []string) {
			QueryFourteenAction()
		},
	}
)

func QueryFourteenAction() {
	if QueryFourteenOldName == "" || QueryFourteenNewName == "" {
		log.Fatal("❌ You must provide --oldname and --newname flags")
	}

	session := cassandra_client.GetSession()
	defer session.Close()

	startTime := time.Now()
	var rusageStart syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &rusageStart)

	var memStart runtime.MemStats
	runtime.ReadMemStats(&memStart)

	// Step 1: Read old node data
	var label string
	err := session.Query(`SELECT label FROM node WHERE name = ? ALLOW FILTERING`, QueryFourteenOldName).Scan(&label)
	if err != nil {
		log.Fatalf("❌ Failed to fetch old node data: %v", err)
	}

	// Step 2: Insert new node
	err = session.Query(`INSERT INTO node (name, label) VALUES (?, ?)`, QueryFourteenNewName, label).Exec()
	if err != nil {
		log.Fatalf("❌ Failed to insert new node: %v", err)
	}

	// Step 3: Migrate outgoing edges
	iter := session.Query(`SELECT to_node, relation FROM edges WHERE from_node = ?`, QueryFourteenOldName).Iter()
	var toNode, relation string
	for iter.Scan(&toNode, &relation) {
		err := session.Query(`INSERT INTO edges (from_node, to_node, relation, edge_id) VALUES (?, ?, ?, uuid())`, QueryFourteenNewName, toNode, relation).Exec()
		if err != nil {
			log.Printf("⚠️ Failed to insert new outgoing edge: %v", err)
		}
	}
	iter.Close()

	// Step 4: Migrate incoming edges
	iter = session.Query(`SELECT from_node, relation FROM edges WHERE to_node = ? ALLOW FILTERING`, QueryFourteenOldName).Iter()
	var fromNode string
	for iter.Scan(&fromNode, &relation) {
		err := session.Query(`INSERT INTO edges (from_node, to_node, relation, edge_id) VALUES (?, ?, ?, uuid())`, fromNode, QueryFourteenNewName, relation).Exec()
		if err != nil {
			log.Printf("⚠️ Failed to insert new incoming edge: %v", err)
		}
	}
	iter.Close()

	// Step 5: Delete old edges
	_ = session.Query(`DELETE FROM edges WHERE from_node = ?`, QueryFourteenOldName).Exec()
	_ = session.Query(`DELETE FROM edges WHERE to_node = ?`, QueryFourteenOldName).Exec()

	// Step 6: Delete old node
	err = session.Query(`DELETE FROM node WHERE name = ?`, QueryFourteenOldName).Exec()
	if err != nil {
		log.Fatalf("❌ Failed to delete old node: %v", err)
	}

	// Step 7: Verify successors and predecessors
	query := fmt.Sprintf("SELECT to_node FROM edges WHERE from_node = '%s'", QueryFourteenNewName)
	iter = session.Query(query).Iter()
	var count, skipped int
	for iter.Scan(&toNode) {
		if !strings.EqualFold(toNode, QueryOneNode) {
			color.Green("Successors of %s : %s", QueryFourteenNewName, toNode)
			count++
		} else {
			skipped++
		}
	}
	iter.Close()

	query = fmt.Sprintf("SELECT from_node FROM edges WHERE to_node = '%s' ALLOW FILTERING", QueryFourteenNewName)
	iter = session.Query(query).Iter()
	uniqueMap := make(map[string]bool)
	for iter.Scan(&fromNode) {
		fmt.Printf("Predecessors: %s \n", fromNode)
		uniqueMap[fromNode] = true
	}
	iter.Close()

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
	throughput := 1.0 / duration.Seconds()
	logQueryTime(duration, "query_fourteen")
	color.Green("✅ Node renamed from %s to %s", QueryFourteenOldName, QueryFourteenNewName)
	color.Yellow("⏱️  Wall Time: %s", duration)
	color.Yellow("⚙️  CPU Time (User): %s | (Sys): %s", cpuUserTime, cpuSysTime)
	color.Magenta("🧠 Memory Used: %.2f KB", float64(memUsed)/1024)
	color.Blue("🧹 GC Pause: %.2f ms", float64(gcPauseNs)/1e6)
	color.Cyan("📈 Throughput: %.2f ops/sec", throughput)
}
