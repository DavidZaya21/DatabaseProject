package model

type Node struct {
	Label string
	Name  string
}

type Edge struct {
	FromNode     string
	ToNode       string
	RelationType string
}
