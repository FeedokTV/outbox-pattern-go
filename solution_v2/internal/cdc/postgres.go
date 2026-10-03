package cdc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"outbox-pattern-go/solution_v2/internal/outbox"
	"slices"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

type PostgresCDC struct {
	conn *pgconn.PgConn

	pending []*CDCEvent
	ready   []*CDCEvent

	confirmedLSN pglogrepl.LSN

	inTransaction bool
}

func NewPostgresCDC(conn *pgconn.PgConn) *PostgresCDC {
	return &PostgresCDC{
		conn: conn,
	}
}

func (pcdc *PostgresCDC) Next(ctx context.Context) (*CDCEvent, error) {
	for {
		if len(pcdc.ready) > 0 {
			event := pcdc.ready[0]
			pcdc.ready = pcdc.ready[1:]

			return event, nil
		}

		if err := ctx.Err(); err != nil {
			return nil, err
		}

		rawMsg, err := pcdc.conn.ReceiveMessage(ctx)
		if err != nil {
			return nil, fmt.Errorf("receive WAL message: %w", err)
		}

		if err := pcdc.handleMessage(ctx, rawMsg); err != nil {
			return nil, err
		}
	}
}

func (pcdc *PostgresCDC) handleMessage(ctx context.Context, rawMsg pgproto3.BackendMessage) error {
	if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
		return fmt.Errorf("wal parse error: %+v", errMsg)
	}

	msg, ok := rawMsg.(*pgproto3.CopyData)
	if !ok {
		log.Printf("received unexpected message")
		return nil
	}

	switch msg.Data[0] {
	// Postgres sent message to check is connection alive
	case pglogrepl.PrimaryKeepaliveMessageByteID:
		pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
		if err != nil {
			return fmt.Errorf("failed ParsePrimaryKeepaliveMessage: %w", err)
		}

		if pkm.ReplyRequested {
			err = pcdc.sendStandByStatus(ctx)
			if err != nil {
				return fmt.Errorf("failed to send standby status: %w", err)
			}
		}
		return nil
	case pglogrepl.XLogDataByteID:
		xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
		if err != nil {
			return fmt.Errorf("failed to parse xlog data: %w", err)
		}

		// Parse WAL to CDCEvent
		err = pcdc.processWALData(xld.WALData)
		if err != nil {
			return fmt.Errorf("process WAL data: %w", err)
		}

		return nil
	}

	return fmt.Errorf("unknown event")
}

func (pcdc *PostgresCDC) processWALData(walData []byte) error {
	logicalMessage, err := pglogrepl.ParseV2(walData, false)
	if err != nil {
		return fmt.Errorf("parse logical replication message: %w", err)
	}

	switch logicalMessage := logicalMessage.(type) {
	case *pglogrepl.BeginMessage:
		fmt.Println("Processing message: BEGIN")
		if pcdc.inTransaction {
			return errors.New("unexpected BEGIN")
		}

		pcdc.inTransaction = true
		pcdc.pending = nil
	case *pglogrepl.LogicalDecodingMessageV2:
		fmt.Println("Processing message: MESSAGE")

		if logicalMessage.Prefix != outbox.PREFIX {
			return nil
		}

		if !logicalMessage.Transactional {
			return errors.New("non-transactional CDC message")
		}

		if !pcdc.inTransaction {
			return errors.New("CDC message outside transaction")
		}

		var msg outbox.Message
		if err := json.Unmarshal(logicalMessage.Content, &msg); err != nil {
			return fmt.Errorf("cannot decode outbox message content: %w", err)
		}

		pcdc.pending = append(pcdc.pending, &CDCEvent{
			ID:      msg.AggregateID,
			Content: logicalMessage.Content,
		})

	case *pglogrepl.CommitMessage:
		fmt.Println("Processing message: COMMIT")

		if !pcdc.inTransaction {
			return errors.New("unexpected COMMIT")
		}

		pcdc.inTransaction = false

		endLSN := logicalMessage.TransactionEndLSN

		if len(pcdc.pending) == 0 {
			pcdc.confirmedLSN = endLSN
			return nil
		}

		last := pcdc.pending[len(pcdc.pending)-1]

		last.Ack = func(ctx context.Context) error {
			pcdc.confirmedLSN = endLSN
			err := pcdc.sendStandByStatus(ctx)
			return err
		}

		pcdc.ready = slices.Clone(pcdc.pending)
		pcdc.pending = nil
	}

	return nil
}

func (pcdc *PostgresCDC) sendStandByStatus(ctx context.Context) error {
	return pglogrepl.SendStandbyStatusUpdate(
		ctx,
		pcdc.conn,
		pglogrepl.StandbyStatusUpdate{
			WALWritePosition: pcdc.confirmedLSN,
		},
	)
}

func (pcdc *PostgresCDC) Stop(ctx context.Context) error {
	if pcdc.conn.IsClosed() {
		return nil
	}

	// SendStandbyCopyDone ignores ctx and waits forever, so bound it with a socket deadline
	if deadline, ok := ctx.Deadline(); ok {
		_ = pcdc.conn.Conn().SetDeadline(deadline)
		defer pcdc.conn.Conn().SetDeadline(time.Time{})
	}

	if err := pcdc.sendStandByStatus(ctx); err != nil {
		return fmt.Errorf("send final standby status: %w", err)
	}

	if _, err := pglogrepl.SendStandbyCopyDone(ctx, pcdc.conn); err != nil {
		return fmt.Errorf("end replication stream: %w", err)
	}

	return nil
}
