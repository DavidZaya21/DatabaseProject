package cmd

import "github.com/spf13/cobra"

var QuerySixteenCmd = &cobra.Command{
	Use:     "sixteen",
	Aliases: []string{"sixteen"},
	Short:   "Query 16 command",
	Run: func(cmd *cobra.Command, args []string) {
		QuerySixteenAction()
	},
}

func QuerySixteenAction() {
	// TODO: implement query 16
}
