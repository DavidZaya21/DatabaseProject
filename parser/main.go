package main

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/debug"
	"time"

	"github.com/DavidZaya21/parser/model"
	"github.com/fatih/color"
	"github.com/gocql/gocql"
)

const (
	batchSize     = 100
	connectHost   = "127.0.0.1"
	keyspaceName  = "final_schema"
	retryAttempts = 5
	retryDelay    = 3 * time.Second
)

var (
	session        *gocql.Session
	insertEdgeStmt = "INSERT INTO edges (from_node, relation, to_node, edge_id) VALUES (?, ?, ?, ?)"
	insertNodeStmt = "INSERT INTO node (name, label, node_id) VALUES (?, ?, uuid())"
)

type Edge struct {
	FromNode     string
	ToNode       string
	RelationType string
}

func main() {
	// Check command line arguments
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <tsv-file-path>\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Example: %s /path/to/cskg.tsv\n", os.Args[0])
		os.Exit(1)
	}

	filePath := os.Args[1]

	// Validate file exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		color.Red("❌ File does not exist: %s", filePath)
		os.Exit(1)
	}

	debug.SetGCPercent(500)
	color.Green("🚀 Starting Cassandra loader...")
	color.Yellow("📁 Using file: %s", filePath)

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

	start := time.Now()

	color.Magenta("⚙️  Inserting nodes sequentially...")
	for i, batch := range nodeBatches {
		if err := retryInsertNodeBatch(batch); err != nil {
			log.Printf("❌ Node batch %d failed after retries", i)
		}
	}

	color.Magenta("⚙️  Inserting edges sequentially...")
	for i, batch := range edgeBatches {
		if err := retryInsertEdgeBatch(batch); err != nil {
			log.Printf("❌ Edge batch %d failed after retries", i)
		}
	}

	color.Green("✅ All inserts completed in %s", time.Since(start))

	runtime.ReadMemStats(&memAfter)
	printMemoryReport(memBefore, memAfter)
	color.Green("🎉 Done!")
}

func connectCassandra() error {
	cluster := gocql.NewCluster(connectHost)
	cluster.Keyspace = keyspaceName
	cluster.Consistency = gocql.One
	cluster.Timeout = 2 * time.Minute
	cluster.ConnectTimeout = 5 * time.Minute
	cluster.RetryPolicy = &gocql.ExponentialBackoffRetryPolicy{
		Min:        time.Second,
		Max:        10 * time.Second,
		NumRetries: 5,
	}
	cluster.Compressor = &gocql.SnappyCompressor{}

	var err error
	session, err = cluster.CreateSession()
	if err == nil {
		color.Green("✅ Connected to Cassandra at %s/%s", connectHost, keyspaceName)
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
	defer f.Close()

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
		from := string(bytes.TrimSpace(parts[1]))
		to := string(bytes.TrimSpace(parts[3]))
		rel := string(bytes.TrimSpace(parts[6]))

		if from != "" || to != "" || rel != "" {
			edges = append(edges, &Edge{
				FromNode:     from,
				ToNode:       to,
				RelationType: rel,
			})
		}
	}
	return edges, scanner.Err()
}

func createNodeBatches(nodes []*model.Node, size int) [][]*model.Node {
	var batches [][]*model.Node
	for i := 0; i < len(nodes); i += size {
		end := i + size
		if end > len(nodes) {
			end = len(nodes)
		}
		batches = append(batches, nodes[i:end])
	}
	return batches
}

func createEdgeBatches(edges []*Edge, size int) [][]*Edge {
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

func retryInsertNodeBatch(batch []*model.Node) error {
	var err error
	for attempt := 1; attempt <= retryAttempts; attempt++ {
		err = insertNodeBatch(batch)
		if err == nil {
			return nil
		}
		log.Printf("⚠️  Node batch insert failed (attempt %d): %v", attempt, err)
		time.Sleep(retryDelay)
	}
	return err
}

func retryInsertEdgeBatch(batch []*Edge) error {
	var err error
	for attempt := 1; attempt <= retryAttempts; attempt++ {
		err = insertEdgeBatch(batch)
		if err == nil {
			return nil
		}
		log.Printf("⚠️  Edge batch insert failed (attempt %d): %v", attempt, err)
		time.Sleep(retryDelay)
	}
	return err
}

func insertNodeBatch(batch []*model.Node) error {
	b := session.NewBatch(gocql.UnloggedBatch)
	for _, n := range batch {
		b.Query(insertNodeStmt, n.Name, n.Label)
	}
	return session.ExecuteBatch(b)
}

func insertEdgeBatch(batch []*Edge) error {
	b := session.NewBatch(gocql.UnloggedBatch)
	for _, e := range batch {
		b.Query(insertEdgeStmt, e.FromNode, e.RelationType, e.ToNode, gocql.TimeUUID())
	}
	return session.ExecuteBatch(b)
}

func printMemoryReport(before, after runtime.MemStats) {
	color.Cyan("🧠 Memory Usage Report:")
	color.Cyan("  HeapAlloc:    %d -> %d", before.HeapAlloc, after.HeapAlloc)
	color.Cyan("  TotalAlloc:   %d -> %d", before.TotalAlloc, after.TotalAlloc)
	color.Cyan("  NumGC:        %d -> %d", before.NumGC, after.NumGC)
}