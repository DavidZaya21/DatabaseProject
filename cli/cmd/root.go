package cmd

import (
	"fmt"

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
	Use:   "cli",
	Short: "cli is a simple command-line application",
	Long:  `cli is a longer description of your application that can span multiple lines.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("Welcome to your Cobra CLI app!")
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
