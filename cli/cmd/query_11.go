package cmd

import "github.com/spf13/cobra"

var QueryElevenCmd = &cobra.Command{
	Use:     "eleven",
	Aliases: []string{"eleven"},
	Short:   "Query 11 command",
	Run: func(cmd *cobra.Command, args []string) {
		QueryElevenAction()
	},
}

func QueryElevenAction() {
	// TODO: implement query 11
}
