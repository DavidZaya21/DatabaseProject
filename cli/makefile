OUTPUT=./bin/dbcli
RESULTS_DIR=./results

build:
	@echo "building the cli..."
	@go build -o $(OUTPUT)

run-parse:
	@echo "running the parser..."
	@go run ../parser/main.go

# Individual query commands
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
	@echo "Finding shortest path between two nodes"
	@$(OUTPUT) sixteen "/c/en/steam_locomotive" "/c/en/value"

query-seventeen:
	@echo "Finding distant synonyms"
	@$(OUTPUT) seventeen "/c/en/good" "50"

query-eighteen:
	@echo "Finding distant antonyms"
	@$(OUTPUT) eighteen "/c/en/good" "50"

# Run all queries and save results to files
run-all: build
	@echo "Running all queries and saving results to files..."
	@mkdir -p $(RESULTS_DIR)
	@echo "========================================" > $(RESULTS_DIR)/all_results.log
	@echo "Database CLI Query Results" >> $(RESULTS_DIR)/all_results.log
	@echo "Execution Date: $$(date)" >> $(RESULTS_DIR)/all_results.log
	@echo "========================================" >> $(RESULTS_DIR)/all_results.log
	@echo ""
	
	@echo "Query 1: Finding successors of /c/en/steam_locomotive"
	@echo "Query 1: Finding successors of /c/en/steam_locomotive" >> $(RESULTS_DIR)/all_results.log
	@$(OUTPUT) one -f="/c/en/steam_locomotive" > $(RESULTS_DIR)/query1_results.txt 2>&1
	@cat $(RESULTS_DIR)/query1_results.txt >> $(RESULTS_DIR)/all_results.log
	@echo "" >> $(RESULTS_DIR)/all_results.log
	
	@echo "Query 2: Counting successors of /c/en/value"
	@echo "Query 2: Counting successors of /c/en/value" >> $(RESULTS_DIR)/all_results.log
	@$(OUTPUT) two -f="/c/en/value" > $(RESULTS_DIR)/query2_results.txt 2>&1
	@cat $(RESULTS_DIR)/query2_results.txt >> $(RESULTS_DIR)/all_results.log
	@echo "" >> $(RESULTS_DIR)/all_results.log
	
	@echo "Query 3: Finding predecessors of Q40157"
	@echo "Query 3: Finding predecessors of Q40157" >> $(RESULTS_DIR)/all_results.log
	@$(OUTPUT) three -f="Q40157" > $(RESULTS_DIR)/query3_results.txt 2>&1
	@cat $(RESULTS_DIR)/query3_results.txt >> $(RESULTS_DIR)/all_results.log
	@echo "" >> $(RESULTS_DIR)/all_results.log
	
	@echo "Query 4: Counting predecessors of /c/en/country"
	@echo "Query 4: Counting predecessors of /c/en/country" >> $(RESULTS_DIR)/all_results.log
	@$(OUTPUT) four -f="/c/en/country" > $(RESULTS_DIR)/query4_results.txt 2>&1
	@cat $(RESULTS_DIR)/query4_results.txt >> $(RESULTS_DIR)/all_results.log
	@echo "" >> $(RESULTS_DIR)/all_results.log
	
	@echo "Query 5: Finding neighbors of /c/en/spectrogram"
	@echo "Query 5: Finding neighbors of /c/en/spectrogram" >> $(RESULTS_DIR)/all_results.log
	@$(OUTPUT) five -f="/c/en/spectrogram" > $(RESULTS_DIR)/query5_results.txt 2>&1
	@cat $(RESULTS_DIR)/query5_results.txt >> $(RESULTS_DIR)/all_results.log
	@echo "" >> $(RESULTS_DIR)/all_results.log
	
	@echo "Query 6: Finding successors of /c/en/jar"
	@echo "Query 6: Finding successors of /c/en/jar" >> $(RESULTS_DIR)/all_results.log
	@$(OUTPUT) six -f="/c/en/jar" > $(RESULTS_DIR)/query6_results.txt 2>&1
	@cat $(RESULTS_DIR)/query6_results.txt >> $(RESULTS_DIR)/all_results.log
	@echo "" >> $(RESULTS_DIR)/all_results.log
	
	@echo "Query 7: Finding successors of Q676"
	@echo "Query 7: Finding successors of Q676" >> $(RESULTS_DIR)/all_results.log
	@$(OUTPUT) seven -f="Q676" > $(RESULTS_DIR)/query7_results.txt 2>&1
	@cat $(RESULTS_DIR)/query7_results.txt >> $(RESULTS_DIR)/all_results.log
	@echo "" >> $(RESULTS_DIR)/all_results.log
	
	@echo "Query 8: Finding successors of /c/en/ms_dos"
	@echo "Query 8: Finding successors of /c/en/ms_dos" >> $(RESULTS_DIR)/all_results.log
	@$(OUTPUT) eight -f="/c/en/ms_dos" > $(RESULTS_DIR)/query8_results.txt 2>&1
	@cat $(RESULTS_DIR)/query8_results.txt >> $(RESULTS_DIR)/all_results.log
	@echo "" >> $(RESULTS_DIR)/all_results.log
	
	@echo "Query 9: Counting total nodes"
	@echo "Query 9: Counting total nodes" >> $(RESULTS_DIR)/all_results.log
	@$(OUTPUT) nine > $(RESULTS_DIR)/query9_results.txt 2>&1
	@cat $(RESULTS_DIR)/query9_results.txt >> $(RESULTS_DIR)/all_results.log
	@echo "" >> $(RESULTS_DIR)/all_results.log
	
	@echo "Query 10: Counting nodes without successors"
	@echo "Query 10: Counting nodes without successors" >> $(RESULTS_DIR)/all_results.log
	@$(OUTPUT) ten > $(RESULTS_DIR)/query10_results.txt 2>&1
	@cat $(RESULTS_DIR)/query10_results.txt >> $(RESULTS_DIR)/all_results.log
	@echo "" >> $(RESULTS_DIR)/all_results.log
	
	@echo "Query 11: Counting nodes without predecessors"
	@echo "Query 11: Counting nodes without predecessors" >> $(RESULTS_DIR)/all_results.log
	@$(OUTPUT) eleven > $(RESULTS_DIR)/query11_results.txt 2>&1
	@cat $(RESULTS_DIR)/query11_results.txt >> $(RESULTS_DIR)/all_results.log
	@echo "" >> $(RESULTS_DIR)/all_results.log
	
	@echo "Query 12: Finding node with most neighbors"
	@echo "Query 12: Finding node with most neighbors" >> $(RESULTS_DIR)/all_results.log
	@$(OUTPUT) twelve > $(RESULTS_DIR)/query12_results.txt 2>&1
	@cat $(RESULTS_DIR)/query12_results.txt >> $(RESULTS_DIR)/all_results.log
	@echo "" >> $(RESULTS_DIR)/all_results.log
	
	@echo "Query 13: Counting nodes with single neighbor"
	@echo "Query 13: Counting nodes with single neighbor" >> $(RESULTS_DIR)/all_results.log
	@$(OUTPUT) thirteen > $(RESULTS_DIR)/query13_results.txt 2>&1
	@cat $(RESULTS_DIR)/query13_results.txt >> $(RESULTS_DIR)/all_results.log
	@echo "" >> $(RESULTS_DIR)/all_results.log
	
	@echo "Query 14: Renaming node"
	@echo "Query 14: Renaming node" >> $(RESULTS_DIR)/all_results.log
	@$(OUTPUT) fourteen -o="/c/en/transportation_topic/n" -n="/c/en/movement_topic/n" > $(RESULTS_DIR)/query14_results.txt 2>&1
	@cat $(RESULTS_DIR)/query14_results.txt >> $(RESULTS_DIR)/all_results.log
	@echo "" >> $(RESULTS_DIR)/all_results.log
	
	@echo "Query 15: Finding similar nodes"
	@echo "Query 15: Finding similar nodes" >> $(RESULTS_DIR)/all_results.log
	@$(OUTPUT) fifteen -f="/c/en/emission_nebula" > $(RESULTS_DIR)/query15_results.txt 2>&1
	@cat $(RESULTS_DIR)/query15_results.txt >> $(RESULTS_DIR)/all_results.log
	@echo "" >> $(RESULTS_DIR)/all_results.log
	
	@echo "Query 16: Finding shortest path"
	@echo "Query 16: Finding shortest path" >> $(RESULTS_DIR)/all_results.log
	@$(OUTPUT) sixteen "/c/en/steam_locomotive" "/c/en/value" > $(RESULTS_DIR)/query16_results.txt 2>&1
	@cat $(RESULTS_DIR)/query16_results.txt >> $(RESULTS_DIR)/all_results.log
	@echo "" >> $(RESULTS_DIR)/all_results.log
	
	@echo "Query 17: Finding distant synonyms"
	@echo "Query 17: Finding distant synonyms" >> $(RESULTS_DIR)/all_results.log
	@$(OUTPUT) seventeen "/c/en/good" "2" > $(RESULTS_DIR)/query17_results.txt 2>&1
	@cat $(RESULTS_DIR)/query17_results.txt >> $(RESULTS_DIR)/all_results.log
	@echo "" >> $(RESULTS_DIR)/all_results.log
	
	@echo "Query 18: Finding distant antonyms"
	@echo "Query 18: Finding distant antonyms" >> $(RESULTS_DIR)/all_results.log
	@$(OUTPUT) eighteen "/c/en/good" "2" > $(RESULTS_DIR)/query18_results.txt 2>&1
	@cat $(RESULTS_DIR)/query18_results.txt >> $(RESULTS_DIR)/all_results.log
	@echo "" >> $(RESULTS_DIR)/all_results.log
	
	@echo "========================================" >> $(RESULTS_DIR)/all_results.log
	@echo "All queries completed successfully!" >> $(RESULTS_DIR)/all_results.log
	@echo "Individual results saved in $(RESULTS_DIR)/" >> $(RESULTS_DIR)/all_results.log
	@echo "========================================" >> $(RESULTS_DIR)/all_results.log
	
	@echo ""
	@echo "All queries completed! Results saved in:"
	@echo "  - Individual files: $(RESULTS_DIR)/query*_results.txt"
	@echo "  - Combined log: $(RESULTS_DIR)/all_results.log"

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
	@for file in $(RESULTS_DIR)/query*_results.txt; do \
		if [ -f "$$file" ]; then \
			echo "$$file:"; \
			head -3 "$$file" | sed 's/^/  /'; \
			echo ""; \
		fi; \
	done

.PHONY: build run-parse query-one query-two query-three query-four query-five query-six query-seven query-eight query-nine query-ten query-eleven query-twelve query-thirteen query-fourteen query-fifteen query-sixteen query-seventeen query-eighteen run-all clean-results clean show-results