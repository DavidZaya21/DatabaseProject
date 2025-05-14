package cmd

import "github.com/spf13/cobra"

var QueryThirteenCmd = &cobra.Command{
	Use:     "thirteen",
	Aliases: []string{"thirteen"},
	Short:   "Query 13 command",
	Run: func(cmd *cobra.Command, args []string) {
		QueryThirteenAction()
	},
}

func QueryThirteenAction() {
	// TODO: implement query 13
}
