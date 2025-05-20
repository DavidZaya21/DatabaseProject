package main

import (
	"bufio"
	"bytes"
	"github.com/DavidZaya21/parser/model"
	"github.com/fatih/color"
	"github.com/gocql/gocql"
	"log"
	"os"
	"runtime"
	"runtime/debug"
	"sync"
	"time"
)

const (
	filePath      = "/Users/swanhtet1aungphyo/Downloads/cskg.tsv"
	batchSize     = 100 // Reduced batch size
	maxWorkers    = 4   // Reduced worker count
	connectHost   = "127.0.0.1"
	keyspaceName  = "fina_schema"
	retryAttempts = 5               // Number of retry attempts for failed batches
	retryDelay    = time.Second * 3 // Delay between retries
)

var (
	session        *gocql.Session
	insertEdgeStmt = "INSERT INTO edges (from_node, relation, to_node, edge_id) VALUES (?, ?, ?, ?)"
	insertNodeStmt = "INSERT INTO node (name, label, node_id) VALUES (?, ?, uuid())"
	nodePool       = sync.Pool{New: func() interface{} { return new(model.Node) }}
	edgePool       = sync.Pool{New: func() interface{} { return new(Edge) }}
	nodeInsertWg   sync.WaitGroup
	edgeInsertWg   sync.WaitGroup
	nodeInsertJobs chan nodeBatchJob
	edgeInsertJobs chan edgeBatchJob
)

type Edge struct {
	FromNode     string
	ToNode       string
	RelationType string
}

type nodeBatchJob struct {
	batch    []*model.Node
	attempts int
}

type edgeBatchJob struct {
	batch    []*Edge
	attempts int
}

func main() {
	debug.SetGCPercent(500)
	color.Green("🚀 Starting Cassandra loader...")

	if err := connectCassandra(); err != nil {
		color.Red("❌ Cassandra connection failed: %v", err)
		return
	}
	defer session.Close()

	var memBefore, memAfter runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	color.Yellow("📂 Reading nodes from file...")
	nodes, err := readNodes(filePath)
	if err != nil {
		color.Red("❌ Failed to read nodes: %v", err)
		return
	}
	nodeBatches := createNodeBatches(nodes, batchSize)
	color.Yellow("📦 Total nodes: %d (in %d batches)", len(nodes), len(nodeBatches))

	color.Yellow("📂 Reading edges from file...")
	edges, err := readEdges(filePath)
	if err != nil {
		color.Red("❌ Failed to read edges: %v", err)
		return
	}
	edgeBatches := createEdgeBatches(edges, batchSize)
	color.Yellow("🔗 Total edges: %d (in %d batches)", len(edges), len(edgeBatches))

	// Initialize job channels with buffer capacity
	nodeInsertJobs = make(chan nodeBatchJob, maxWorkers*2)
	edgeInsertJobs = make(chan edgeBatchJob, maxWorkers*2)

	color.Magenta("⚙️  Inserting nodes and edges in parallel with retry mechanism...")
	start := time.Now()

	// Start workers for processing batches
	startParallelInsertWorkers()

	go enqueueNodeBatches(nodeBatches)
	go enqueueEdgeBatches(edgeBatches)

	nodeInsertWg.Wait()
	edgeInsertWg.Wait()

	color.Green("✅ All inserts completed in %s", time.Since(start))

	runtime.ReadMemStats(&memAfter)
	printMemoryReport(memBefore, memAfter)
	color.Green("🎉 Done!")
}

func connectCassandra() error {
	cluster := gocql.NewCluster(connectHost)
	cluster.Keyspace = keyspaceName

	cluster.Timeout = 2 * time.Minute
	cluster.ConnectTimeout = 5 * time.Minute
	cluster.SocketKeepalive = 5 * time.Minute
	cluster.ReconnectInterval = time.Minute

	cluster.NumConns = 8 // Reduced number of connections per host
	cluster.MaxPreparedStmts = 1000
	cluster.MaxRoutingKeyInfo = 5000
	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(
		gocql.RoundRobinHostPolicy(),
	)

	cluster.Consistency = gocql.One

	cluster.DisableInitialHostLookup = false
	cluster.RetryPolicy = &gocql.ExponentialBackoffRetryPolicy{
		Min:        time.Second,
		Max:        10 * time.Second,
		NumRetries: 5,
	}

	cluster.PageSize = 5000

	cluster.Compressor = &gocql.SnappyCompressor{}

	var err error
	session, err = cluster.CreateSession()
	if err == nil {
		color.Green("✅ Connected to Cassandra at %s/%s", connectHost, keyspaceName)

		if err := session.Query("SELECT now() FROM system.local").Exec(); err != nil {
			color.Red("❌ Connection test failed: %v", err)
			return err
		}
		color.Green("✅ Connection test successful")
	}
	return err
}

func readNodes(path string) ([]*model.Node, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	nodeMap := make(map[string]*model.Node)
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 256*1024)

	if !scanner.Scan() {
		return nil, scanner.Err()
	}

	for scanner.Scan() {
		parts := bytes.Split(scanner.Bytes(), []byte{'\t'})
		if len(parts) < 6 {
			continue
		}
		name1 := string(bytes.TrimSpace(parts[1]))
		label1 := string(bytes.TrimSpace(parts[4]))
		name2 := string(bytes.TrimSpace(parts[3]))
		label2 := string(bytes.TrimSpace(parts[5]))

		if name1 != "" && label1 != "" {
			nodeMap[name1] = &model.Node{Name: name1, Label: label1}
		}
		if name2 != "" && label2 != "" {
			nodeMap[name2] = &model.Node{Name: name2, Label: label2}
		}
	}

	nodes := make([]*model.Node, 0, len(nodeMap))
	for _, n := range nodeMap {
		nodes = append(nodes, n)
	}
	return nodes, scanner.Err()
}

func readEdges(path string) ([]*Edge, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func(f *os.File) {
		err := f.Close()
		if err != nil {
			log.Printf("Failed to close file: %v", err.Error())
		}
	}(f)

	var edges []*Edge
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 256*1024)

	if !scanner.Scan() {
		return edges, scanner.Err()
	}

	for scanner.Scan() {
		parts := bytes.Split(scanner.Bytes(), []byte{'\t'})
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
			edgePool.Put(e)
		}
	}
	return edges, scanner.Err()
}

func createNodeBatches(nodes []*model.Node, size int) []nodeBatchJob {
	var batches []nodeBatchJob
	for i := 0; i < len(nodes); i += size {
		end := i + size
		if end > len(nodes) {
			end = len(nodes)
		}
		batches = append(batches, nodeBatchJob{
			batch:    nodes[i:end],
			attempts: 0,
		})
	}
	return batches
}

func createEdgeBatches(edges []*Edge, size int) []edgeBatchJob {
	var batches []edgeBatchJob
	for i := 0; i < len(edges); i += size {
		end := i + size
		if end > len(edges) {
			end = len(edges)
		}
		batches = append(batches, edgeBatchJob{
			batch:    edges[i:end],
			attempts: 0,
		})
	}
	return batches
}

func startParallelInsertWorkers() {
	for i := 0; i < maxWorkers; i++ {
		nodeInsertWg.Add(1)
		edgeInsertWg.Add(1)

		go func(workerID int) {
			defer nodeInsertWg.Done()
			for job := range nodeInsertJobs {
				if err := processNodeBatch(job, workerID); err != nil {
					if job.attempts < retryAttempts {
						time.Sleep(retryDelay * time.Duration(job.attempts+1))
						job.attempts++
						nodeInsertJobs <- job
					} else {
						log.Printf("❌ Node batch failed after %d attempts", retryAttempts)
					}
				}
			}
		}(i)

		go func(workerID int) {
			defer edgeInsertWg.Done()
			for job := range edgeInsertJobs {
				if err := processEdgeBatch(job, workerID); err != nil {
					if job.attempts < retryAttempts {
						time.Sleep(retryDelay * time.Duration(job.attempts+1))
						job.attempts++
						edgeInsertJobs <- job
					} else {
						log.Printf("❌ Edge batch failed after %d attempts", retryAttempts)
					}
				}
			}
		}(i)
	}
}

func processNodeBatch(job nodeBatchJob, workerID int) error {
	b := session.NewBatch(gocql.UnloggedBatch)
	b.SetConsistency(gocql.One)

	for _, n := range job.batch {
		b.Query(insertNodeStmt, n.Name, n.Label)
	}

	err := session.ExecuteBatch(b)
	if err != nil {
		log.Printf("❌ Node batch insert error (attempt %d): %v", job.attempts+1, err)
		return err
	}

	return nil
}

func processEdgeBatch(job edgeBatchJob, workerID int) error {
	b := session.NewBatch(gocql.UnloggedBatch)
	b.SetConsistency(gocql.One)

	for _, e := range job.batch {
		b.Query(insertEdgeStmt, e.FromNode, e.RelationType, e.ToNode, gocql.TimeUUID())
	}

	err := session.ExecuteBatch(b)
	if err != nil {
		log.Printf("❌ Edge batch insert error (attempt %d): %v", job.attempts+1, err)
		return err
	}

	// Return edges to pool after successful insertion
	for _, e := range job.batch {
		edgePool.Put(e)
	}

	return nil
}

func enqueueNodeBatches(batches []nodeBatchJob) {
	for _, batch := range batches {
		nodeInsertJobs <- batch
	}
	close(nodeInsertJobs)
}

func enqueueEdgeBatches(batches []edgeBatchJob) {
	for _, batch := range batches {
		edgeInsertJobs <- batch
	}
	close(edgeInsertJobs)
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
		color.Red("🔺 Memory increased: %d KB", diff/1024)
	} else {
		color.Green("🔻 Memory decreased: %d KB", -diff/1024)
	}
}
