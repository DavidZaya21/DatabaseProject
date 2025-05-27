# Variables
OUTPUT = ./bin/dbcli
RESULTS_DIR = ./results


build:
	@echo "Building the CLI..."
	@go build -o $(OUTPUT)


run-parse:
	@echo "Running the parser..."
	@go run ../parser/main.go


query-one:
	@echo "Finding the successor of the given node"
	@$(OUTPUT) one -f="/c/en/steam_locomotive" >> $(RESULTS_DIR)/one.txt

query-two:
	@echo "Counting the successor of the given node"
	@$(OUTPUT) two -f="/c/en/value" >> $(RESULTS_DIR)/two.txt

query-three:
	@echo "Finding the predecessors of the given node"
	@$(OUTPUT) three -f="Q40157" >> $(RESULTS_DIR)/three.txt

query-four:
	@echo "Counting all the predecessors of the given node"
	@$(OUTPUT) four -f="/c/en/country" >> $(RESULTS_DIR)/four.txt

query-five:
	@echo "Finding all neighbors of given node"
	@$(OUTPUT) five -f="/c/en/spectrogram" >> $(RESULTS_DIR)/five.txt

query-six:
	@echo "Finding the successor of the given node"
	@$(OUTPUT) six -f="/c/en/jar" >> $(RESULTS_DIR)/six.txt

query-seven:
	@echo "Finding the successor of the given node"
	@$(OUTPUT) seven -f="Q676" >> $(RESULTS_DIR)/seven.txt

query-eight:
	@echo "Finding the successor of the given node"
	@$(OUTPUT) eight -f="/c/en/ms_dos" >> $(RESULTS_DIR)/eight.txt

query-nine:
	@echo "Counting the total number of nodes"
	@$(OUTPUT) nine >> $(RESULTS_DIR)/nine.txt

query-ten:
	@echo "Counting all the nodes without successors"
	@$(OUTPUT) ten >> $(RESULTS_DIR)/ten.txt

query-eleven:
	@echo "Counting all the nodes without predecessors"
	@$(OUTPUT) eleven >> $(RESULTS_DIR)/eleven.txt

query-twelve:
	@echo "Finding the node with the most neighbors"
	@$(OUTPUT) twelve >> $(RESULTS_DIR)/twelve.txt

query-thirteen:
	@echo "Counting the nodes with a single neighbor"
	@$(OUTPUT) thirteen >> $(RESULTS_DIR)/thirteen.txt

query-fourteen:
	@echo "Renaming the given node"
	@$(OUTPUT) fourteen -o="/c/en/transportation_topic/n" -n="/c/en/movement_topic/n" >> $(RESULTS_DIR)/fourteen.txt

query-fifteen:
	@echo "Finding similar nodes for given node"
	@$(OUTPUT) fifteen -f="/c/en/emission_nebula" >> $(RESULTS_DIR)/fifteen.txt

query-sixteen:
	@echo "Finding shortest path between two nodes"
	@$(OUTPUT) sixteen "/c/en/uchuva" "/c/en/square_sails/n" >> $(RESULTS_DIR)/sixteen.txt

query-seventeen:
	@echo "Finding distant synonyms"
	@$(OUTPUT) seventeen "/c/en/defeatable" "2" >> $(RESULTS_DIR)/seventeen.txt

query-eighteen:
	@echo "Finding distant antonyms"
	@$(OUTPUT) eighteen "/c/en/automate" "3" >> $(RESULTS_DIR)/eighteen.txt

# Run all queries
run-all: query-one query-two query-three query-four query-five query-six query-seven query-eight query-nine query-ten query-eleven query-twelve query-thirteen query-fourteen query-fifteen query-sixteen query-seventeen query-eighteen

# Clean results directory
clean-results:
	@echo "Cleaning results directory..."
	@rm -rf $(RESULTS_DIR)

# Clean everything
clean: clean-results
	@echo "Cleaning binary..."
	@rm -f $(OUTPUT)

# Show results summary
show-results:
	@echo "Results Summary:"
	@echo "================"
	@for file in $(RESULTS_DIR)/*.txt; do \
		if [ -f "$$file" ]; then \
			echo "$$file:"; \
			head -3 "$$file" | sed 's/^/  /'; \
			echo ""; \
		fi; \
	done

# Declare phony targets
.PHONY: build run-parse query-one query-two query-three query-four query-five query-six query-seven query-eight query-nine query-ten query-eleven query-twelve query-thirteen query-fourteen query-fifteen query-sixteen query-seventeen query-eighteen run-all clean-results clean show-results