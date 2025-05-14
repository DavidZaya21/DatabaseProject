package cmd

import "github.com/spf13/cobra"

var QueryTwoCmd = &cobra.Command{
	Use:     "two",
	Aliases: []string{"two"},
	Short:   "Query 2 command",
	Run: func(cmd *cobra.Command, args []string) {
		QueryTwoAction()
	},
}

func QueryTwoAction() {
	// TODO: implement query 2
}
