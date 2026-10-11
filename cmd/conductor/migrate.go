// Copyright 2026 Simon Xu
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"errors"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	schema "github.com/simondxu/conductor/db"
)

var (
	migrateDSN    string
	migrateConfig string
	migrateSteps  int
)

var migrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "Manage the PostgreSQL schema",
	Long: `Apply or inspect Conductor's PostgreSQL schema migrations.
The DSN comes from --dsn, else from eventlog.postgres.dsn in the config file.`,
	SilenceUsage: true,
}

func init() {
	migrateCmd.PersistentFlags().StringVar(&migrateDSN, "dsn", "", "PostgreSQL DSN (default: eventlog.postgres.dsn from the config)")
	migrateCmd.PersistentFlags().StringVar(&migrateConfig, "config", defaultConfigFile, "Path to YAML configuration file")

	up := &cobra.Command{
		Use:   "up",
		Short: "Apply all pending migrations",
		RunE: func(cmd *cobra.Command, args []string) error {
			dsn, err := resolveMigrateDSN(cmd)
			if err != nil {
				return err
			}
			v, err := schema.Up(dsn)
			if err != nil {
				return err
			}
			fmt.Printf("schema version %d\n", v)
			return nil
		},
	}
	down := &cobra.Command{
		Use:   "down",
		Short: "Roll back --steps migrations",
		RunE: func(cmd *cobra.Command, args []string) error {
			dsn, err := resolveMigrateDSN(cmd)
			if err != nil {
				return err
			}
			v, err := schema.Down(dsn, migrateSteps)
			if err != nil {
				return err
			}
			fmt.Printf("schema version %d\n", v)
			return nil
		},
	}
	down.Flags().IntVar(&migrateSteps, "steps", 0, "Number of migrations to roll back (required)")
	version := &cobra.Command{
		Use:   "version",
		Short: "Print the applied schema version",
		RunE: func(cmd *cobra.Command, args []string) error {
			dsn, err := resolveMigrateDSN(cmd)
			if err != nil {
				return err
			}
			v, dirty, err := schema.Version(dsn)
			if err != nil {
				return err
			}
			fmt.Printf("schema version %d (dirty: %t), binary needs %d\n", v, dirty, schema.SchemaVersion)
			return nil
		},
	}
	migrateCmd.AddCommand(up, down, version)
	rootCmd.AddCommand(migrateCmd)
}

func resolveMigrateDSN(cmd *cobra.Command) (string, error) {
	if migrateDSN != "" {
		return migrateDSN, nil
	}
	cfg, err := newConfig(cmd, migrateConfig)
	if err != nil {
		return "", err
	}
	dsn := os.ExpandEnv(cfg.EventLog.PostgresConfig.DSN)
	if dsn == "" {
		return "", errors.New("no PostgreSQL DSN: pass --dsn or set eventlog.postgres.dsn")
	}
	return dsn, nil
}
