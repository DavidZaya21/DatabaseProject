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
}

func mountingCmd() {
	for _, cmd := range queries {
		rootCmd.AddCommand(cmd)
	}
}
