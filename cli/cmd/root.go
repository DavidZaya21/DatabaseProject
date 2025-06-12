package cmd

import (

	"github.com/fatih/color"
	"github.com/spf13/cobra"
)

var queries = []*cobra.Command{
	// QueryDummyCmd,
	// EdgeBidirectionCmd,
	QueryOneCmd,
	QueryTwoCmd,
	QueryThreeCmd,
	QueryFourCmd,
	QueryFiveCmd,
	QuerySixCmd,
	QuerySevenCmd,
	QueryEightCmd,
	QueryNineCmd,
	QueryTenCmd,
	QueryElevenCmd,
	QueryTwelveCmd,
	QueryThirteenCmd,
	QueryFourteenCmd,
	QueryFifteenCmd,
	QuerySixteenCmd,
	QuerySeventeenCmd,
	QueryEighteenCmd,
}
var rootCmd = &cobra.Command{
	Use:   "dbcli",
	Short: "dbcli is a command-line tool for querying and analyzing graph data",
	Long: `dbcli is a command-line tool with various query commands to analyze graph data.

Available commands and flags:

  one         -f, --from_node       Find successors of a given node
  two         -f, --from_node       Count successors of a given node
  three       -f, --to_node         Find predecessors of a given node
  four        -f, --to_node         Count predecessors of a given node
  five        -f, --node            Find neighbors of a given node
  six         -f, --node            Count neighbors of a given node
  seven       -f, --node            Find grandchildren of a given node
  eight       -f, --node            Find grandparents of a given node
  nine                            Count all nodes in the graph (no flags)
  ten                             Count nodes without successors (no flags)
  eleven                          Count nodes without predecessors (no flags)
  twelve                          Find the node with the most neighbors (no flags)
  thirteen                        Count nodes with a single neighbor (no flags)
  fourteen    -o, --old name
               -n, --new name       Rename a given node
  fifteen     -f, --node            Find similar nodes for a given node
  sixteen     [source] [target]     Find shortest path between two nodes
  seventeen   [node] [depth]        Find distant synonyms
  eighteen    [node] [depth]        Find distant antonyms

Examples:

  dbcli one -f="/c/en/steam_locomotive"
  dbcli fourteen -o="/c/en/transportation_topic/n" -n="/c/en/movement_topic/n"
  dbcli sixteen "/c/en/uchuva" "/c/en/square_sails/n"
  dbcli seventeen "/c/en/defeatable" 2

Use "dbcli [command] --help" for detailed help on a command.
`,
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Help()
	},
}


func Exec() {
	if err := rootCmd.Execute(); err != nil {
		color.Red("Root command exec is failed", err.Error())
	}
}
func init() {
	cliName := "dbcli"
	rootCmd.Flags().Bool(cliName, false, "Help for message")
	mountingCmd()
	QueryOneCmd.Flags().StringVarP(&QueryOneNode, "from_node", "f", "", "Source node to find successors for")
	_ = QueryOneCmd.MarkFlagRequired("from_node")
	QueryTwoCmd.Flags().StringVarP(&QueryTwoFromNode, "from_node", "f", "", "Count all the successors of given node")
	_ = QueryTwoCmd.MarkFlagRequired("from_node")
	QueryThreeCmd.Flags().StringVarP(&QueryThreeNode, "to_node", "f", "", "Find all predecessors of a given node")
	_ = QueryThreeCmd.MarkFlagRequired("to_node")
	QueryFourCmd.Flags().StringVarP(&QueryFourNode, "to_node", "f", "", "Count all the predecessors of given node")
	_ = QueryFourCmd.MarkFlagRequired("to_node")
	QueryFiveCmd.Flags().StringVarP(&QueryFiveNode, "node", "f", "", "Find all neighbors of given node")
	_ = QueryFiveCmd.MarkFlagRequired("node")
	QuerySixCmd.Flags().StringVarP(&QuerySixNode, "node", "f", "", "Count all neighbors of given node")
	_ = QuerySixCmd.MarkFlagRequired("node")
	QuerySevenCmd.Flags().StringVarP(&QuerySevenNode, "node", "f", "", "Find all grandchildren of given node")
	_ = QuerySevenCmd.MarkFlagRequired("node")
	QueryEightCmd.Flags().StringVarP(&QueryEightNode, "node", "f", "", "Find all grandparents of given node")
	_ = QueryEightCmd.MarkFlagRequired("node")
	QueryFourteenCmd.Flags().StringVarP(&QueryFourteenOldName, "old name", "o", "", "Old node name (e.g., /c/en/transportation_topic/n)")
	QueryFourteenCmd.Flags().StringVarP(&QueryFourteenNewName, "new name", "n", "", "New node name (e.g., /c/en/movement_topic/n)")
	_ = QueryFourteenCmd.MarkFlagRequired("old name")
	_ = QueryFourteenCmd.MarkFlagRequired("mew name")
	QueryFifteenCmd.Flags().StringVarP(&QueryFifteenNode, "node", "f", "", "Find all similar nodes of given node")
	_ = QueryFifteenCmd.MarkFlagRequired("node")
}

func mountingCmd() {
	for _, cmd := range queries {
		rootCmd.AddCommand(cmd)
	}
}
