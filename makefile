OUTPAR=./bin/parser
OUT=./bin
CLI=./bin/dbcli
GOOS=darwin
GOARCH=arm64
RESULTS_DIR = ./results
build:
	@echo "Building the parser ..."
	@mkdir -p $(OUT)
	@cd parser && pwd && GOOS=$(GOOS) GOARCH=$(GOARCH) go build -o ../$(OUTPAR) main.go
	@cd cli  && pwd && GOOS=$(GOOS) GOARCH=$(GOARCH) go build -o ../$(CLI) main.go
	@mkdir -p $(RESULTS_DIR)


parse:
	@$(OUTPAR)

query-one:
	@echo "Finding the successor of the given node"
	@$(CLI) one -f="/c/en/steam_locomotive" >> $(RESULTS_DIR)/query_one.txt

query-two:
	@echo "Counting the successor of the given node"
	@$(CLI) two -f="/c/en/value" >> $(RESULTS_DIR)/query_two.txt

query-three:
	@echo "Finding the predecessors of the given node"
	@$(CLI) three -f="Q40157" >> $(RESULTS_DIR)/query_three.txt

query-four:
	@echo "Counting all the predecessors of the given node"
	@$(CLI) four -f="/c/en/country" >> $(RESULTS_DIR)/query_four.txt

query-five:
	@echo "Finding all neighbors of given node"
	@$(CLI) five -f="/c/en/spectrogram" >> $(RESULTS_DIR)/query_five.txt

query-six:
	@echo "Finding the successor of the given node"
	@$(CLI) six -f="/c/en/jar" >> $(RESULTS_DIR)/query_six.txt

query-seven:
	@echo "Finding the successor of the given node"
	@$(CLI) seven -f="Q676" >> $(RESULTS_DIR)/query_seven.txt

query-eight:
	@echo "Finding the successor of the given node"
	@$(CLI) eight -f="/c/en/ms_dos" >> $(RESULTS_DIR)/query_eight.txt

query-nine:
	@echo "Counting the total number of nodes"
	@$(CLI) nine >> $(RESULTS_DIR)/query_nine.txt

query-ten:
	@echo "Counting all the nodes without successors"
	@$(CLI) ten >> $(RESULTS_DIR)/query_ten.txt

query-eleven:
	@echo "Counting all the nodes without predecessors"
	@$(CLI) eleven >> $(RESULTS_DIR)/query_eleven.txt

query-twelve:
	@echo "Finding the node with the most neighbors"
	@$(CLI) twelve >> $(RESULTS_DIR)/query_twelve.txt

query-thirteen:
	@echo "Counting the nodes with a single neighbor"
	@$(CLI) thirteen >> $(RESULTS_DIR)/query_thirteen.txt

query-fourteen:
	@echo "Renaming the given node"
	@$(CLI) fourteen -o="/c/en/transportation_topic/n" -n="/c/en/movement_topic/n" >> $(RESULTS_DIR)/query_fourteen.txt

query-fifteen:
	@echo "Finding similar nodes for given node"
	@$(CLI) fifteen -f="/c/en/emission_nebula" >> $(RESULTS_DIR)/query_fifteen.txt

query-sixteen:
	@echo "Finding shortest path between two nodes"
	@$(CLI) sixteen "/c/en/uchuva" "/c/en/square_sails/n" >> $(RESULTS_DIR)/query_sixteen.txt

query-seventeen:
	@echo "Finding distant synonyms"
	@$(CLI) seventeen "/c/en/defeatable" "2" >> $(RESULTS_DIR)/query_seventeen.txt

query-eighteen:
	@echo "Finding distant antonyms"
	@$(CLI) eighteen "/c/en/automate" "3" >> $(RESULTS_DIR)/query_eighteen.txt
run-all: query-one query-two query-three query-four query-five query-six query-seven query-eight query-nine query-ten query-eleven query-twelve query-thirteen query-fourteen query-fifteen query-sixteen query-seventeen query-eighteen

.PHONY: build