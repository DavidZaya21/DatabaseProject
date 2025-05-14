package cmd

import "github.com/spf13/cobra"

var QueryNineCmd = &cobra.Command{
	Use:     "nine",
	Aliases: []string{"nine"},
	Short:   "Query 9 command",
	Run: func(cmd *cobra.Command, args []string) {
		QueryNineAction()
	},
}

func QueryNineAction() {
	// TODO: implement query 9
}
