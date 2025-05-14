package cmd

import "github.com/spf13/cobra"

var QueryThreeCmd = &cobra.Command{
	Use:     "three",
	Aliases: []string{"three"},
	Short:   "Query 3 command",
	Run: func(cmd *cobra.Command, args []string) {
		QueryThreeAction()
	},
}

func QueryThreeAction() {
	// TODO: implement query 3
}
