package grpcserver

import (
	clientpb "client-manager/gen"
	"context"
	"errors"

	"client-manager/handler"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	clientpb.UnimplementedClientManagerServer
	h handler.Service
}

func New(h handler.Service) *Server { return &Server{h: h} }

func Register(s *grpc.Server, h handler.Service) {
	clientpb.RegisterClientManagerServer(s, New(h))
}

func (s *Server) CreateClient(ctx context.Context, req *clientpb.CreateClientRequest) (*clientpb.CreateClientResponse, error) {
	if req.GetClientId() == "" {
		return nil, status.Error(codes.InvalidArgument, "client_id required")
	}
	if err := s.h.CreateClient(req.ClientId, req.InitialBalanceMinor, req.NormalPriceMinor, req.PriorityPriceMinor); err != nil {
		return nil, status.Errorf(codes.Internal, "create: %v", err)
	}
	return &clientpb.CreateClientResponse{ClientId: req.ClientId, BalanceMinor: req.InitialBalanceMinor}, nil
}

func (s *Server) GetClient(ctx context.Context, req *clientpb.GetClientRequest) (*clientpb.GetClientResponse, error) {
	c, err := s.h.GetClient(req.GetClientId())
	if err != nil {
		if errors.Is(err, handler.ErrNotFound) {
			return nil, status.Error(codes.NotFound, "client_not_found")
		}
		return nil, status.Errorf(codes.Internal, "db: %v", err)
	}
	return &clientpb.GetClientResponse{
		Client: &clientpb.Client{
			ClientId:      c.ClientID,
			BalanceMinor:  c.BalanceMinor,
			CreatedAtUnix: c.CreatedAt.Unix(),
			UpdatedAtUnix: c.UpdatedAt.Unix(),
		},
	}, nil
}

func (s *Server) GetPricePlan(ctx context.Context, req *clientpb.GetPricePlanRequest) (*clientpb.GetPricePlanResponse, error) {
	p, err := s.h.GetPricePlan(req.GetClientId())
	if err != nil {
		if errors.Is(err, handler.ErrNotFound) {
			return nil, status.Error(codes.NotFound, "priceplan_not_found")
		}
		return nil, status.Errorf(codes.Internal, "db: %v", err)
	}
	return &clientpb.GetPricePlanResponse{
		PricePlan: &clientpb.PricePlan{
			ClientId:           p.ClientID,
			NormalPriceMinor:   p.NormalPriceMinor,
			PriorityPriceMinor: p.PriorityPriceMinor,
		},
	}, nil
}

func (s *Server) Debit(ctx context.Context, req *clientpb.MoneyRequest) (*clientpb.MoneyResponse, error) {
	after, err := s.h.Debit(req.GetClientId(), req.GetAmountMinor(), req.GetRef())
	if err != nil {
		if errors.Is(err, handler.ErrInsufficientFunds) {
			return nil, status.Error(codes.FailedPrecondition, "insufficient_funds")
		}
		if errors.Is(err, handler.ErrNotFound) {
			return nil, status.Error(codes.NotFound, "client_not_found")
		}
		return nil, status.Errorf(codes.Internal, "debit: %v", err)
	}
	return &clientpb.MoneyResponse{BalanceAfter: after}, nil
}

func (s *Server) Refund(ctx context.Context, req *clientpb.MoneyRequest) (*clientpb.MoneyResponse, error) {
	after, err := s.h.Refund(req.GetClientId(), req.GetAmountMinor(), req.GetRef())
	if err != nil {
		if errors.Is(err, handler.ErrNotFound) {
			return nil, status.Error(codes.NotFound, "client_not_found")
		}
		return nil, status.Errorf(codes.Internal, "refund: %v", err)
	}
	return &clientpb.MoneyResponse{BalanceAfter: after}, nil
}
