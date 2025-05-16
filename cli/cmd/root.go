package cmd

import (
	"fmt"
	"github.com/fatih/color"
	"github.com/spf13/cobra"
)

var queries = []*cobra.Command{
	QueryDummyCmd,
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
	QueryOneCmd.Flags().StringVarP(&queryOneNode, "from_node", "f", "", "Source node to find successors for")
	_ = QueryOneCmd.MarkFlagRequired("from_node")
	QueryTwoCmd.Flags().StringVarP(&queryTwoFromNode, "from_node", "f", "", "Count all the successors of given node")
	_ = QueryTwoCmd.MarkFlagRequired("from_node")
	QueryThreeCmd.Flags().StringVarP(&queryThreeNode, "to_node", "f", "", "Find all predecessors of a given node")
	_ = QueryThreeCmd.MarkFlagRequired("to_node")
	QueryFourCmd.Flags().StringVarP(&queryFourNode, "to_node", "f", "", "Count all the predecessors of given node")
	_ = QueryFourCmd.MarkFlagRequired("to_node")
}

func mountingCmd() {
	for _, cmd := range queries {
		rootCmd.AddCommand(cmd)
	}
}
