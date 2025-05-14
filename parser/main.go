package main

import (
	"github.com/DavidZaya21/parser/fileReader"
	"github.com/DavidZaya21/parser/model"
	"github.com/fatih/color"
	"github.com/gocql/gocql"
	"log"
	"runtime"
	"time"
)

var (
	filePath = "/Users/davidzayar/Downloads/cskg.tsv"
	session  *gocql.Session
)

func init() {
	cluster := gocql.NewCluster("127.0.0.1")
	cluster.Keyspace = "fina_schema"
	cluster.Timeout = 60 * time.Minute
	cluster.ConnectTimeout = 60 * time.Minute
	cluster.Consistency = gocql.Quorum
	var err error
	session, err = cluster.CreateSession()
	if err != nil {
		color.Red("❌ Connection Error: %v", err)
		return
	}
	color.Green("✅ Cassandra connection successful")
}

func main() {
	var memStatsBefore, memStatsAfter runtime.MemStats
	runtime.ReadMemStats(&memStatsBefore)

	start := time.Now()
	color.Blue("📖 Reading File...")

	reader := fileReader.FileReader(filePath)

	// === Benchmark Insert Nodes ===
	// nodes := fileReader.RawFileProcessing(reader)
	// cleanNodes := fileReader.RemoveNodeDuplication(nodes)
	// benchmark("🧩 Node Insertion", func() {
	// 	batchInsertNodes(cleanNodes)
	// })

	// === Benchmark Insert Edges ===
	edges := fileReader.RawDataToEdgeProcessing(reader)
	benchmark("🔗 Edge Insertion", func() {
		batchEdgesInsertion(edges)
	})

	color.Cyan("✅ Total Processing Time: %s", time.Since(start))

	runtime.ReadMemStats(&memStatsAfter)
	printMemoryReport(memStatsBefore, memStatsAfter)
}

func benchmark(title string, f func()) {
	color.Magenta("⏱️  Starting: %s", title)
	start := time.Now()
	f()
	duration := time.Since(start)
	color.Green("✅ Completed: %s in %s", title, duration)
}

func batchInsertNodes(nodes []*model.Node) {
	batch := session.NewBatch(gocql.LoggedBatch)
	for i, node := range nodes {
		batch.Query("INSERT INTO node (node_id, name, label) VALUES (uuid(), ?, ?)", node.Name, node.Label)

		if (i+1)%100 == 0 {
			err := session.ExecuteBatch(batch)
			if err != nil {
				log.Fatalf("❌ Failed batch insert at record %d: %v", i, err)
			}
			batch = session.NewBatch(gocql.LoggedBatch)
		}
	}
	if len(batch.Entries) > 0 {
		err := session.ExecuteBatch(batch)
		if err != nil {
			log.Fatalf("❌ Final batch failed: %v", err)
		}
	}
	color.Green("🧩 Node insertion succeeded.")
}

func batchEdgesInsertion(edges []*model.Edge) {
	batch := session.NewBatch(gocql.LoggedBatch)
	for i, edge := range edges {
		batch.Query("INSERT INTO edges (edge_id, from_node, to_node, relation) VALUES (uuid(), ?, ?, ?)",
			edge.FromNode, edge.ToNode, edge.RelationType)

		if (i+1)%100 == 0 {
			err := session.ExecuteBatch(batch)
			if err != nil {
				log.Fatalf("❌ Failed edge batch at %d: %v", i, err)
			}
			batch = session.NewBatch(gocql.LoggedBatch)
		}
	}
	if len(batch.Entries) > 0 {
		err := session.ExecuteBatch(batch)
		if err != nil {
			log.Fatalf("❌ Final edge batch failed: %v", err)
		}
	}
	color.Green("🔗 Edge insertion succeeded.")
}

func printMemoryReport(before, after runtime.MemStats) {
	color.Blue("\n📊 Memory Usage Report")
	color.Yellow("--------------------------------------")
	color.Cyan("🔹 Alloc:        %d KB", after.Alloc/1024)
	color.Cyan("🔹 TotalAlloc:   %d KB", after.TotalAlloc/1024)
	color.Cyan("🔹 Sys:          %d KB", after.Sys/1024)
	color.Cyan("🔹 NumGC:        %d", after.NumGC)
	color.Cyan("🔹 HeapObjects:  %d", after.HeapObjects)
	color.Yellow("--------------------------------------")

	allocDiff := int64(after.Alloc) - int64(before.Alloc)
	if allocDiff > 0 {
		color.Red("🔺 Increase in memory: %d KB", allocDiff/1024)
	} else {
		color.Green("🔻 Decrease in memory: %d KB", -allocDiff/1024)
	}
}
