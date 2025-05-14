package cmd

import "github.com/spf13/cobra"

var QueryEighteenCmd = &cobra.Command{
	Use:     "eighteen",
	Aliases: []string{"eighteen"},
	Short:   "Query 18 command",
	Run: func(cmd *cobra.Command, args []string) {
		QueryEighteenAction()
	},
}

func QueryEighteenAction() {
	// TODO: implement query 18
}
