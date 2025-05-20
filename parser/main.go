package main

import (
	"bufio"
	"bytes"
	"log"
	"os"
	"runtime"
	"runtime/debug"
	"sync"
	"time"

	"github.com/fatih/color"
	"github.com/gocql/gocql"
)

const (
	filePath     = "/Users/swanhtet1aungphyo/Downloads/cskg.tsv"
	batchSize    = 250
	maxWorkers   = 8
	connectHost  = "127.0.0.1"
	keyspaceName = "fina_schema"
)

var (
	session        *gocql.Session
	insertEdgeStmt = "INSERT INTO edges (from_node, relation, to_node, edge_id) VALUES (?, ?, ?, ?)"

	// Pool for Edge objects to avoid GC churn
	edgePool = sync.Pool{
		New: func() interface{} { return new(Edge) },
	}
)

type Edge struct {
	FromNode     string
	ToNode       string
	RelationType string
}

func main() {
	// Raise GC target from 100% (default) to 500%: fewer, larger collections
	debug.SetGCPercent(500)

	color.Green("🚀 Starting optimized Cassandra edge inserter...")

	if err := connectCassandra(); err != nil {
		color.Red("❌ Cassandra connection failed: %v", err)
		return
	}
	defer session.Close()

	var memBefore, memAfter runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	edges, err := readEdges(filePath)
	if err != nil {
		color.Red("❌ Failed to read edges: %v", err)
		return
	}
	color.Yellow("📊 Total edges read: %d", len(edges))

	batches := createBatches(edges, batchSize)
	edges = nil // free the slice early

	benchmark("🔗 Edge insertion", func() {
		insertBatchesParallel(batches)
	})

	runtime.ReadMemStats(&memAfter)
	printMemoryReport(memBefore, memAfter)
	color.Green("🎉 Finished!")
}

func connectCassandra() error {
	cluster := gocql.NewCluster(connectHost)
	cluster.Keyspace = keyspaceName
	cluster.Timeout = 30 * time.Second
	cluster.ConnectTimeout = 30 * time.Second
	cluster.Consistency = gocql.One
	cluster.DisableInitialHostLookup = true

	// Session tuning
	cluster.NumConns = 20
	cluster.MaxPreparedStmts = 2000
	cluster.PoolConfig.HostSelectionPolicy =
		gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())

	var err error
	session, err = cluster.CreateSession()
	if err == nil {
		color.Green("✅ Connected to Cassandra at %s/%s", connectHost, keyspaceName)
	}
	return err
}

func readEdges(path string) ([]*Edge, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	edges := make([]*Edge, 0, 1_000_000)

	scanner := bufio.NewScanner(f)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 256*1024)

	if !scanner.Scan() {
		return edges, scanner.Err()
	}

	for scanner.Scan() {
		line := scanner.Bytes()
		parts := bytes.Split(line, []byte{'\t'})
		if len(parts) < 7 {
			continue
		}

		e := edgePool.Get().(*Edge)
		e.FromNode = string(bytes.TrimSpace(parts[1]))
		e.ToNode = string(bytes.TrimSpace(parts[3]))
		e.RelationType = string(bytes.TrimSpace(parts[6]))

		if e.FromNode != "" && e.ToNode != "" && e.RelationType != "" {
			edges = append(edges, e)
		} else {
			// Return to pool immediately if invalid
			edgePool.Put(e)
		}
	}
	return edges, scanner.Err()
}

func createBatches(edges []*Edge, size int) [][]*Edge {
	var batches [][]*Edge
	for i := 0; i < len(edges); i += size {
		end := i + size
		if end > len(edges) {
			end = len(edges)
		}
		batches = append(batches, edges[i:end])
	}
	return batches
}

func insertBatchesParallel(batches [][]*Edge) {
	jobs := make(chan []*Edge, len(batches))
	var wg sync.WaitGroup

	// Worker pool
	for i := 0; i < maxWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for batch := range jobs {
				b := session.NewBatch(gocql.UnloggedBatch)
				for _, e := range batch {
					b.Query(insertEdgeStmt,
						e.FromNode, e.RelationType, e.ToNode, gocql.TimeUUID())
					// Return object to pool after queuing
					edgePool.Put(e)
				}
				if err := session.ExecuteBatch(b); err != nil {
					log.Printf("❌ Batch insert error: %v", err)
				}
			}
		}()
	}

	for _, batch := range batches {
		jobs <- batch
	}
	close(jobs)
	wg.Wait()
	color.Green("✅ All batches inserted.")
}

func benchmark(name string, fn func()) {
	color.Magenta("⏱️  %s...", name)
	start := time.Now()
	fn()
	color.Green("✅ %s completed in %s", name, time.Since(start))
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

	diff := int64(after.Alloc) - int64(before.Alloc)
	if diff > 0 {
		color.Red("🔺 Increase in memory: %d KB", diff/1024)
	} else {
		color.Green("🔻 Decrease in memory: %d KB", -diff/1024)
	}
}
