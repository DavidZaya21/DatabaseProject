package cmd

import "github.com/spf13/cobra"

var QuerySixCmd = &cobra.Command{
	Use:     "six",
	Aliases: []string{"six"},
	Short:   "Query 6 command",
	Run: func(cmd *cobra.Command, args []string) {
		QuerySixAction()
	},
}

func QuerySixAction() {
	// TODO: implement query 6
}
