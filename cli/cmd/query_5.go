package cmd

import "github.com/spf13/cobra"

var QueryFiveCmd = &cobra.Command{
	Use:     "five",
	Aliases: []string{"five"},
	Short:   "Query 5 command",
	Run: func(cmd *cobra.Command, args []string) {
		QueryFiveAction()
	},
}

func QueryFiveAction() {
	// TODO: implement query 5
}
