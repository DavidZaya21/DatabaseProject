package cmd

import "github.com/spf13/cobra"

var QueryFourCmd = &cobra.Command{
	Use:     "four",
	Aliases: []string{"four"},
	Short:   "Query 4 command",
	Run: func(cmd *cobra.Command, args []string) {
		QueryFourAction()
	},
}

func QueryFourAction() {
	// TODO: implement query 4
}
