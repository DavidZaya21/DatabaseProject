package cmd

import "github.com/spf13/cobra"

var QueryEightCmd = &cobra.Command{
	Use:     "eight",
	Aliases: []string{"eight"},
	Short:   "Query 8 command",
	Run: func(cmd *cobra.Command, args []string) {
		QueryEightAction()
	},
}

func QueryEightAction() {
	// TODO: implement query 8
}
