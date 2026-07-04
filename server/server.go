// Package server implements the gRPC KVService over an engine handle,
// translating engine errors into canonical status codes.
package server

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	basalt "github.com/LovRanRan/basalt"
	basaltv1 "github.com/LovRanRan/basalt/api/basalt/v1"
	"github.com/LovRanRan/basalt/internal/sstable"
)

// scanBatchPairs is how many pairs one streamed ScanResponse carries; small
// enough to keep messages bounded, large enough to amortize the stream.
const scanBatchPairs = 256

// KV serves the KVService RPCs. It does not own the DB: the caller opens
// and closes it.
type KV struct {
	basaltv1.UnimplementedKVServiceServer
	db *basalt.DB
}

func New(db *basalt.DB) *KV { return &KV{db: db} }

// toStatus maps engine failures onto canonical codes: caller mistakes are
// InvalidArgument, a closed engine is Unavailable (retry after reopen),
// detected on-disk corruption is DataLoss, anything else Internal.
func toStatus(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, basalt.ErrBatchTooLarge):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, basalt.ErrClosed):
		return status.Error(codes.Unavailable, err.Error())
	case errors.Is(err, sstable.ErrCorruption):
		return status.Error(codes.DataLoss, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

func validKey(key []byte) error {
	if len(key) == 0 {
		return status.Error(codes.InvalidArgument, "key must not be empty")
	}
	return nil
}

func (s *KV) Get(_ context.Context, req *basaltv1.GetRequest) (*basaltv1.GetResponse, error) {
	if err := validKey(req.GetKey()); err != nil {
		return nil, err
	}
	v, err := s.db.Get(req.GetKey())
	if errors.Is(err, basalt.ErrNotFound) {
		return &basaltv1.GetResponse{Found: false}, nil
	}
	if err != nil {
		return nil, toStatus(err)
	}
	return &basaltv1.GetResponse{Found: true, Value: v}, nil
}

func (s *KV) Put(_ context.Context, req *basaltv1.PutRequest) (*basaltv1.PutResponse, error) {
	if err := validKey(req.GetKey()); err != nil {
		return nil, err
	}
	if err := s.db.Put(req.GetKey(), req.GetValue()); err != nil {
		return nil, toStatus(err)
	}
	return &basaltv1.PutResponse{}, nil
}

func (s *KV) Delete(_ context.Context, req *basaltv1.DeleteRequest) (*basaltv1.DeleteResponse, error) {
	if err := validKey(req.GetKey()); err != nil {
		return nil, err
	}
	if err := s.db.Delete(req.GetKey()); err != nil {
		return nil, toStatus(err)
	}
	return &basaltv1.DeleteResponse{}, nil
}

// Scan streams the range in fixed-size batches, honoring the client's limit
// and checking stream-context cancellation between batches so a slow or
// gone client cannot pin the snapshot iterator forever.
func (s *KV) Scan(req *basaltv1.ScanRequest, stream basaltv1.KVService_ScanServer) error {
	var start, end []byte
	if len(req.GetStart()) > 0 {
		start = req.GetStart()
	}
	if len(req.GetEnd()) > 0 {
		end = req.GetEnd()
	}
	it := s.db.Scan(start, end)
	defer it.Close()

	var sent uint64
	batch := make([]*basaltv1.KeyValue, 0, scanBatchPairs)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := stream.Send(&basaltv1.ScanResponse{Pairs: batch}); err != nil {
			return err
		}
		batch = make([]*basaltv1.KeyValue, 0, scanBatchPairs)
		return nil
	}
	for ; it.Valid(); it.Next() {
		// Key/Value are invalidated by Next: copy into the message.
		batch = append(batch, &basaltv1.KeyValue{
			Key:   append([]byte(nil), it.Key()...),
			Value: append([]byte(nil), it.Value()...),
		})
		sent++
		if req.GetLimit() > 0 && sent >= req.GetLimit() {
			break
		}
		if len(batch) == scanBatchPairs {
			if err := stream.Context().Err(); err != nil {
				return status.FromContextError(err).Err()
			}
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := it.Error(); err != nil {
		return toStatus(err)
	}
	return flush()
}
