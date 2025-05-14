package cmd

import "github.com/spf13/cobra"

var QueryTwelveCmd = &cobra.Command{
	Use:     "twelve",
	Aliases: []string{"twelve"},
	Short:   "Query 12 command",
	Run: func(cmd *cobra.Command, args []string) {
		QueryTwelveAction()
	},
}

func QueryTwelveAction() {
	// TODO: implement query 12
}
