package cmd

import "github.com/spf13/cobra"

var QueryFourteenCmd = &cobra.Command{
	Use:     "fourteen",
	Aliases: []string{"fourteen"},
	Short:   "Query 14 command",
	Run: func(cmd *cobra.Command, args []string) {
		QueryFourteenAction()
	},
}

func QueryFourteenAction() {
	// TODO: implement query 14
}
