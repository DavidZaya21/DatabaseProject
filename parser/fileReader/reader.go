package fileReader

import (
	"bufio"
	"fmt"
	"github.com/DavidZaya21/parser/model"
	"github.com/fatih/color"
	"log"
	"os"
	"strings"
	"sync"
)

var filePath = "/Users/swanhtet1aungphyo/Downloads/cskg.tsv"

func FileReader(filename string) *os.File {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal(err.Error())
	}
	return file
}

func RawFileProcessing(file *os.File) []*model.Node {
	var nodes []*model.Node
	scanner := bufio.NewScanner(file)
	if scanner.Scan() {

	}
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, "\t")
		if len(parts) < 5 || isRecordEmpty(parts) {
			continue
		} else {
			firstNodeApperance := &model.Node{
				Name:  parts[1],
				Label: parts[4],
			}
			secondNodApperance := &model.Node{
				Name:  parts[3],
				Label: parts[5],
			}
			nodes = append(nodes, firstNodeApperance)
			nodes = append(nodes, secondNodApperance)
		}
	}
	return nodes
}

// TODO: Edge processing
func RawDataToEdgeProcessing(file *os.File) []*model.Edge {
	var edges []*model.Edge
	scanner := bufio.NewScanner(file)
	if scanner.Scan() {
	}
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, "\t")
		if len(parts) < 5 || isRecordEmpty(parts) {
			continue
		} else {
			foundEdges := &model.Edge{
				FromNode:     parts[1],
				ToNode:       parts[3],
				RelationType: parts[6],
			}
			edges = append(edges, foundEdges)
		}
	}
	color.Yellow(fmt.Sprintf("Length of the Raw Edges : %d", len(edges)))
	return edges
}

func RemoveNodeDuplication(nodes []*model.Node) []*model.Node {
	var nodeMap sync.Map
	var nonDuplicatedNode []*model.Node
	for _, n := range nodes {
		if strings.TrimSpace(n.Label) == "" || strings.TrimSpace(n.Name) == "" {
			continue
		}
		key := n.Name
		if _, ok := nodeMap.Load(key); !ok {
			nonDuplicatedNode = append(nonDuplicatedNode, n)
			nodeMap.Store(key, true)
		}
	}
	return nonDuplicatedNode
}
func isRecordEmpty(parts []string) bool {
	for _, part := range parts {
		if strings.TrimSpace(part) != "" {
			return false
		}
	}
	return true
}
