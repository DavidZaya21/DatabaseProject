package cmd

import "github.com/spf13/cobra"

var QueryTenCmd = &cobra.Command{
	Use:     "ten",
	Aliases: []string{"ten"},
	Short:   "Query 10 command",
	Run: func(cmd *cobra.Command, args []string) {
		QueryTenAction()
	},
}

func QueryTenAction() {
	// TODO: implement query 10
}
