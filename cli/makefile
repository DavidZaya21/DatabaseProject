OUTPUT=./bin/dbcli

build:
	@echo "building the cli..."
	@go build -o $(OUTPUT)

run-parse:
	@echo "running the parser..."
	@go run ../parser/main.go
query-one:
	@echo "finding the successor of the given node"
	@$(OUTPUT) one -f="/c/en/steam_locomotive"
query-two:
	@echo "Counting the successor of the given node"
	@$(OUTPUT) two -f="/c/en/value"

query-three:
	@echo "finding the predecessors of the given node"
	@$(OUTPUT) three -f="Q40157"

query-four:
	@echo "Counting all the Predecessors of the given node"
	@$(OUTPUT) four -f="/c/en/country"

query-five:
	@echo "finding all neighbors of given node"
	@$(OUTPUT) five -f="/c/en/spectrogram"

query-six:
	@echo "finding the successor of the given node"
	@$(OUTPUT) six -f="/c/en/jar"

query-seven:
	@echo "finding the successor of the given node"
	@$(OUTPUT) seven -f="Q676"

query-eight:
	@echo "finding the successor of the given node"
	@$(OUTPUT) eight -f="/c/en/ms_dos"

query-nine:
	@echo "Counting the total number of nodes"
	@$(OUTPUT) nine

query-ten:
	@echo "Counting all the nodes without successors"
	@$(OUTPUT) ten

query-eleven:
	@echo "Counting all the nodes without predecessors"
	@$(OUTPUT) eleven

query-twelve:
	@echo "Finding the node with the most neighbors"
	@$(OUTPUT) twelve

query-thirteen:
	@echo "Counting the nodes with a single neighbor"
	@$(OUTPUT) thirteen

query-fourteen:
	@echo "Rename the given node"
	@$(OUTPUT) fourteen -o="/c/en/transportation_topic/n" -n="/c/en/movement_topic/n"

query-fifteen:
	@echo "Finding similar nodes for given node"
	@$(OUTPUT) fifteen -f="/c/en/emission_nebula"

query-sixteen:
	@echo "finding the successor of the given node"
	@$(OUTPUT) sixteen

query-seventeen:
	@echo "finding the successor of the given node"
	@$(OUTPUT) seventeen

query-eighteen:
	@echo "finding the successor of the given node"
	@$(OUTPUT) eighteen
