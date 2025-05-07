package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gocql/gocql"
	"github.com/google/uuid"
)

var session *gocql.Session

type (
	Node struct {
		ID       uuid.UUID
		NodeName string
		Label    string
	}

	Edge struct {
		FromNode     uuid.UUID
		ToNode       uuid.UUID
		RelationType uuid.UUID
	}

	Relation struct {
		ID           uuid.UUID
		RelationType string
	}
)

var (
	firstNodeMap  = make(map[string]uuid.UUID)
	secondNodeMap = make(map[string]uuid.UUID)
	relationMap   = make(map[string]uuid.UUID)

	firstNodes  []Node
	secondNodes []Node

	edges     []Edge
	relations []Relation

	muFirstNode  sync.Mutex
	muSecondNode sync.Mutex
	muRelation   sync.Mutex
	muEdge       sync.Mutex
)

func RemoveDuplicate(objs []Node) []Node {
	seen := make(map[string]bool)
	var uniqueNode []Node

	for _, node := range objs {

		//nodeName := strings.ReplaceAll(node.NodeName, "/", "")
		if !seen[node.NodeName] { // Check if node name is already seen
			seen[node.NodeName] = true
			uniqueNode = append(uniqueNode, node) // Append only unique nodes
		}
	}

	return uniqueNode
}

func MergeNodes(firstNodes, secondNodes []Node) []Node {
	finalNodes := make([]Node, 0, len(firstNodes)+len(secondNodes))
	finalNodes = append(finalNodes, firstNodes...)
	finalNodes = append(finalNodes, secondNodes...)
	return finalNodes
}

const workerCount = 8

func main() {
	cluster := gocql.NewCluster("127.0.0.1")
	cluster.Keyspace = "keyspace_name"
	cluster.Consistency = gocql.One
	var err error
	session, err = cluster.CreateSession()
	failOnError(err, "Failed to connect to Cassandra")
	defer session.Close()

	start := time.Now()
	lines := make(chan string, 10000)
	var wg sync.WaitGroup

	// Start workers
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go worker(lines, &wg)
	}

	// Read file and send lines to workers
	file, err := os.Open("./cskg.tsv")
	failOnError(err, "Failed to open file")
	defer file.Close()

	scanner := bufio.NewScanner(file)
	if scanner.Scan() { /* Skip header */
	}

	for scanner.Scan() {
		lines <- scanner.Text()
	}
	close(lines) // Close the channel to signal workers
	wg.Wait()    // Wait for all workers to finish

	// Write outputs
	writeToFile("./firstNode.csv", firstNodes)
	writeToFile("./secondNode.csv", secondNodes)
	writeEdgeToFile("./edge.csv", edges)
	writeRelationToFile("./relation.csv", relations)

	fmt.Printf("Unique first nodes: %d\n", len(firstNodes))
	fmt.Printf("Unique second nodes: %d\n", len(secondNodes))
	fmt.Printf("Unique relations: %d\n", len(relations))
	fmt.Printf("Edges created: %d\n", len(edges))
	fmt.Printf("Processing completed in %s\n", time.Since(start))

	mergedNodes := MergeNodes(firstNodes, secondNodes)
	fmt.Println(len(mergedNodes))
	unique := RemoveDuplicate(mergedNodes)
	fmt.Println(len(unique))

	startInsert := time.Now() // Start timing insertion process
	// Insert into Cassandra with batch
	fmt.Println("Inserting data into Cassandra...")
	//batchInsertNodes(unique)
	// batchInsertNodes(secondNodes)
	//batchInsertRelations(relations)
	checkForDuplicateEdges(edges)
	batchInsertEdges(edges)
	fmt.Println("Insertion complete.")

	fmt.Printf("Insertion completed in %s\n", time.Since(startInsert))

}

func batchInsertNodes(nodes []Node) {
	batch := session.NewBatch(gocql.LoggedBatch)
	for i, node := range nodes {
		batch.Query("INSERT INTO nodes (id, name, label) VALUES (?, ?, ?)",
			node.ID.String(), node.NodeName, node.Label)

		if (i+1)%100 == 0 {
			err := session.ExecuteBatch(batch)
			failOnError(err, "Failed to batch insert nodes")
			batch = session.NewBatch(gocql.LoggedBatch)
		}
	}
	if len(batch.Entries) > 0 {
		err := session.ExecuteBatch(batch)
		failOnError(err, "Failed to batch insert remaining nodes")
	}
}

func batchInsertRelations(rels []Relation) {
	batch := session.NewBatch(gocql.LoggedBatch)
	for i, rel := range rels {
		batch.Query("INSERT INTO relations (id, relation_type) VALUES (?, ?)",
			rel.ID.String(), rel.RelationType)

		if (i+1)%100 == 0 {
			err := session.ExecuteBatch(batch)
			failOnError(err, "Failed to batch insert relations")
			batch = session.NewBatch(gocql.LoggedBatch)
		}
	}
	if len(batch.Entries) > 0 {
		err := session.ExecuteBatch(batch)
		failOnError(err, "Failed to batch insert remaining relations")
	}
}

func checkForDuplicateEdges(edges []Edge) {
	seen := make(map[string]bool)
	duplicates := 0
	for _, edge := range edges {
		key := edge.FromNode.String() + "_" + edge.ToNode.String() + "_" + edge.RelationType.String()
		if seen[key] {
			duplicates++
		} else {
			seen[key] = true
		}
	}
	fmt.Printf("Total edges: %d\n", len(edges))
	fmt.Printf("Duplicate edges: %d\n", duplicates)
}

func batchInsertEdges(edges []Edge) {
	batch := session.NewBatch(gocql.LoggedBatch)
	for i, edge := range edges {
		batch.Query("INSERT INTO edges (from_node, to_node, relation_id) VALUES (?, ?, ?)",
			edge.FromNode.String(), edge.ToNode.String(), edge.RelationType.String())

		if (i+1)%100 == 0 {
			err := session.ExecuteBatch(batch)
			failOnError(err, "Failed to batch insert edges")
			batch = session.NewBatch(gocql.LoggedBatch)
		}
	}
	if len(batch.Entries) > 0 {
		err := session.ExecuteBatch(batch)
		failOnError(err, "Failed to batch insert remaining edges")
	}
}

func insertNode(id uuid.UUID, name, label string) {
	query := "INSERT INTO nodes (id, name, label) VALUES (?, ?, ?)"
	err := session.Query(query, id.String(), name, label).Exec()
	failOnError(err, "Failed to insert node")
}

func insertRelation(id uuid.UUID, relationType string) {
	query := "INSERT INTO relations (id, relation_type) VALUES (?, ?)"
	err := session.Query(query, id.String(), relationType).Exec()
	failOnError(err, "Failed to insert relation")
}

func insertEdge(from uuid.UUID, to uuid.UUID, relation uuid.UUID) {
	query := "INSERT INTO edges (from_node, to_node, relation_id) VALUES (?, ?, ?)"
	err := session.Query(query, from.String(), to.String(), relation.String()).Exec()
	failOnError(err, "Failed to insert edge")
}

func worker(lines <-chan string, wg *sync.WaitGroup) {
	defer wg.Done()
	for line := range lines {
		processLine(line)
	}
}

func processLine(line string) {
	line = strings.TrimSpace(line)
	if line == "" {
		return
	}
	record := strings.Split(line, "\t")
	if len(record) < 5 || isAllEmpty(record) {
		return
	}
	processRecord(record)
}

func processRecord(record []string) {
	firstNodeID := getOrCreateNode(record[1], record[4], &muFirstNode, firstNodeMap, &firstNodes)
	secondNodeID := getOrCreateNode(record[3], record[5], &muSecondNode, secondNodeMap, &secondNodes)
	relationID := getOrCreateRelation(record[6])

	muEdge.Lock()
	edges = append(edges, Edge{FromNode: firstNodeID, ToNode: secondNodeID, RelationType: relationID})
	muEdge.Unlock()
}

func getOrCreateNode(name, label string, mu *sync.Mutex, nodeMap map[string]uuid.UUID, nodeList *[]Node) uuid.UUID {
	mu.Lock()
	defer mu.Unlock()
	if id, exists := nodeMap[name]; exists {
		return id
	}
	id := uuid.New()
	nodeMap[name] = id
	*nodeList = append(*nodeList, Node{ID: id, NodeName: name, Label: label})
	return id
}

func getOrCreateRelation(relationType string) uuid.UUID {
	muRelation.Lock()
	defer muRelation.Unlock()
	if id, exists := relationMap[relationType]; exists {
		return id
	}
	id := uuid.New()
	relationMap[relationType] = id
	relations = append(relations, Relation{ID: id, RelationType: relationType})
	return id
}

func writeToFile(filePath string, nodes []Node) {
	file, err := os.Create(filePath)
	failOnError(err, "Failed to create file")
	defer file.Close()

	writer := bufio.NewWriter(file)
	for _, node := range nodes {
		writer.WriteString(node.NodeName + " | " + node.Label + " | " + node.ID.String() + "\n")
	}
	writer.Flush()
}

func writeRelationToFile(filePath string, relations []Relation) {
	file, err := os.Create(filePath)
	failOnError(err, "Failed to create file")
	defer file.Close()

	writer := bufio.NewWriter(file)
	for _, relation := range relations {
		writer.WriteString(relation.RelationType + "|" + relation.ID.String() + "\n")
	}
	writer.Flush()
}

func writeEdgeToFile(filePath string, edges []Edge) {
	file, err := os.Create(filePath)
	failOnError(err, "Failed to create file")
	defer file.Close()

	writer := bufio.NewWriter(file)
	for _, edge := range edges {
		writer.WriteString(edge.FromNode.String() + " | " + edge.ToNode.String() + " | " + edge.RelationType.String() + "\n")
	}
	writer.Flush()
}

func isAllEmpty(record []string) bool {
	for _, field := range record {
		if strings.TrimSpace(field) != "" {
			return false
		}
	}
	return true
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %v", msg, err)
	}
}
