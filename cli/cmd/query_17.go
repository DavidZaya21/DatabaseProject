package cmd

import "github.com/spf13/cobra"

var QuerySeventeenCmd = &cobra.Command{
	Use:     "seventeen",
	Aliases: []string{"seventeen"},
	Short:   "Query 17 command",
	Run: func(cmd *cobra.Command, args []string) {
		QuerySeventeenAction()
	},
}

func QuerySeventeenAction() {
	// TODO: implement query 17
}
