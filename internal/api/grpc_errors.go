package api

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func errInvalidArgument(err error) error {
	return status.Error(codes.InvalidArgument, err.Error())
}

func errNotFound(err error) error {
	return status.Error(codes.NotFound, err.Error())
}

func errFailedPrecondition(err error) error {
	return status.Error(codes.FailedPrecondition, err.Error())
}

func errInternal(err error) error {
	return status.Error(codes.Internal, "internal error")
}
