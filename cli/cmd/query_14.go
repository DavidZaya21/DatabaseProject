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

	// Step 1: Read the old node data (select all columns)
	var label string
	log.Println(QueryFourteenOldName)
	log.Println(QueryFourteenNewName)
	// Add other columns you have in the node table here
	err := session.Query(`SELECT label FROM node WHERE name = ? allow filtering `, QueryFourteenOldName).Scan(&label)
	if err != nil {
		log.Fatalf("❌ Failed to fetch old node data: %v", err)
	}

	// Step 2: Insert a new node with new name and copied data
	err = session.Query(`INSERT INTO node (name, label) VALUES (?, ?)`, QueryFourteenNewName, label).Exec()
	if err != nil {
		log.Fatalf("❌ Failed to insert new node: %v", err)
	}

	// Step 3: Update edges - Update from_node references
	oldName := QueryFourteenOldName
	newName := QueryFourteenNewName
	log.Printf(oldName)
	log.Printf(newName)
	err = session.Query(`UPDATE edges SET from_node = ? WHERE from_node = ?`, newName, oldName).Exec()
	if err != nil {
		log.Printf("⚠️ Warning: Failed to update edges from_node: %v", err)
	}

	// Step 4: Update edges - Update to_node references
	err = session.Query(`UPDATE edges SET to_node = ? WHERE to_node = ?`, newName, oldName).Exec()
	if err != nil {
		log.Printf("⚠️ Warning: Failed to update edges to_node: %v", err)
	}

	// Step 5: Delete old node
	err = session.Query(`DELETE FROM node WHERE name = ?`, QueryFourteenOldName).Exec()
	if err != nil {
		log.Fatalf("❌ Failed to delete old node: %v", err)
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

	throughput := 1.0 / duration.Seconds()

	color.Green("✅ Node renamed from %s to %s", oldName, newName)
	color.Yellow("⏱️  Wall Time: %s", duration)
	color.Yellow("⚙️  CPU Time (User): %s | (Sys): %s", cpuUserTime, cpuSysTime)
	color.Magenta("🧠 Memory Used: %.2f KB", float64(memUsed)/1024)
	color.Blue("🧹 GC Pause: %.2f ms", float64(gcPauseNs)/1e6)
	color.Cyan("📈 Throughput: %.2f ops/sec", throughput)

}
