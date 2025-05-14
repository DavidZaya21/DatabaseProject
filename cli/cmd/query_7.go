package cmd

import "github.com/spf13/cobra"

var QuerySevenCmd = &cobra.Command{
	Use:     "seven",
	Aliases: []string{"seven"},
	Short:   "Query 7 command",
	Run: func(cmd *cobra.Command, args []string) {
		QuerySevenAction()
	},
}

func QuerySevenAction() {
	// TODO: implement query 7
}
