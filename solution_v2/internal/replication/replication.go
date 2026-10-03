package replication

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
)

type PgReplicationService struct {
	slotName        string
	publicationName string
	conn            *pgconn.PgConn
}

func NewPgReplicationService(ctx context.Context, pgConn *pgconn.PgConn) *PgReplicationService {
	return &PgReplicationService{
		slotName:        "banksystem_cdc_slot",
		publicationName: "transactions_cdc",
		conn:            pgConn,
	}
}

func (s *PgReplicationService) Initialize(ctx context.Context) error {
	outputPlugin := "pgoutput"
	pluginArguments := []string{
		"proto_version '2'",
		fmt.Sprintf("publication_names '%s'", s.publicationName),
		"messages 'true'",
		"streaming 'false'",
	}

	// We're creating constant slot, so we won't lose our data after crash
	var pgErr *pgconn.PgError
	err := s.createReplicationSlot(ctx, outputPlugin, false)

	// If already exists - skip
	if err != nil {
		if errors.As(err, &pgErr) {
			if pgErr.Code != "42710" {
				return err
			}
		} else {
			return err
		}
	}

	replicationOptions := pglogrepl.StartReplicationOptions{PluginArgs: pluginArguments}
	err = s.startReplication(ctx, replicationOptions)
	if err != nil {
		return err
	}

	return nil
}

// func (s *PgReplicationService) initializePublication(ctx context.Context) error {
// 	query := fmt.Sprintf("DROP PUBLICATION IF EXISTS %s;", s.publicationName)

// 	res := s.conn.Exec(ctx, query)
// 	_, err := res.ReadAll()
// 	if err != nil {
// 		return err
// 	}

// 	query = fmt.Sprintf("CREATE PUBLICATION %s FOR ALL TABLES;", s.publicationName)
// 	res = s.conn.Exec(ctx, query)
// 	_, err = res.ReadAll()
// 	if err != nil {
// 		return err
// 	}

// 	return nil
// }

func (s *PgReplicationService) createReplicationSlot(ctx context.Context, outputPlugin string, isTemporary bool) error {
	if outputPlugin == "" {
		return fmt.Errorf("output plugin is nil")
	}

	_, err := pglogrepl.CreateReplicationSlot(ctx, s.conn, s.slotName, outputPlugin,
		pglogrepl.CreateReplicationSlotOptions{
			Temporary: isTemporary,
		})

	if err != nil {
		return err
	}

	return nil
}

func (s *PgReplicationService) startReplication(ctx context.Context, replicationOptions pglogrepl.StartReplicationOptions) error {
	err := pglogrepl.StartReplication(ctx, s.conn, s.slotName, 0, replicationOptions)
	if err != nil {
		return err
	}

	return nil
}
