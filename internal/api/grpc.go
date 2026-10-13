package api

import (
        "context"
        "encoding/json"
        "errors"
        "fmt"
        "time"

        conduitv1 "github.com/Nerotek01/Conduit/pkg/api/conduit/v1"
        "github.com/Nerotek01/Conduit/internal/model"
        "github.com/Nerotek01/Conduit/internal/storage"
)

type GRPCServer struct {
        conduitv1.UnimplementedJobServiceServer
        svc *Service
}

func NewGRPCServer(svc *Service) *GRPCServer {
        return &GRPCServer{svc: svc}
}

func (g *GRPCServer) SubmitJob(ctx context.Context, req *conduitv1.SubmitJobRequest) (*conduitv1.SubmitJobResponse, error) {
        var payload json.RawMessage
        if req.GetPayloadJson() != "" {
                if !json.Valid([]byte(req.GetPayloadJson())) {
                        return nil, fmt.Errorf("payload_json is not valid json")
                }
                payload = json.RawMessage(req.GetPayloadJson())
        }
        var maxRetries *int
        if req.GetMaxRetries() > 0 {
                mr := int(req.GetMaxRetries())
                maxRetries = &mr
        }
        var idemKey *string
        if req.GetIdempotencyKey() != "" {
                k := req.GetIdempotencyKey()
                idemKey = &k
        }
        job, duplicate, err := g.svc.SubmitJob(ctx, SubmitOptions{
                Type:           req.GetType(),
                Payload:        payload,
                Priority:       int(req.GetPriority()),
                MaxRetries:     maxRetries,
                IdempotencyKey: idemKey,
                DelayMS:        req.GetDelayMs(),
                TimeoutMS:      req.GetTimeoutMs(),
        })
        if err != nil {
                return nil, mapError(err)
        }
        return &conduitv1.SubmitJobResponse{Job: toProtoJob(job), Duplicate: duplicate}, nil
}

func (g *GRPCServer) GetJob(ctx context.Context, req *conduitv1.GetJobRequest) (*conduitv1.Job, error) {
        job, err := g.svc.GetJob(ctx, req.GetId())
        if err != nil {
                return nil, mapError(err)
        }
        return toProtoJob(job), nil
}

func (g *GRPCServer) ListJobs(ctx context.Context, req *conduitv1.ListJobsRequest) (*conduitv1.ListJobsResponse, error) {
        res, err := g.svc.ListJobs(ctx, ListOptions{
                Status: req.GetStatus(),
                Type:   req.GetType(),
                Limit:  int(req.GetLimit()),
        })
        if err != nil {
                return nil, mapError(err)
        }
        jobs := make([]*conduitv1.Job, 0, len(res.Jobs))
        for i := range res.Jobs {
                jobs = append(jobs, toProtoJob(&res.Jobs[i]))
        }
        return &conduitv1.ListJobsResponse{Jobs: jobs}, nil
}

func (g *GRPCServer) CancelJob(ctx context.Context, req *conduitv1.CancelJobRequest) (*conduitv1.Job, error) {
        job, err := g.svc.CancelJob(ctx, req.GetId())
        if err != nil {
                return nil, mapError(err)
        }
        return toProtoJob(job), nil
}

func (g *GRPCServer) RequeueJob(ctx context.Context, req *conduitv1.RequeueJobRequest) (*conduitv1.Job, error) {
        job, err := g.svc.RequeueJob(ctx, req.GetId())
        if err != nil {
                return nil, mapError(err)
        }
        return toProtoJob(job), nil
}

func (g *GRPCServer) HealthCheck(ctx context.Context, _ *conduitv1.HealthCheckRequest) (*conduitv1.HealthCheckResponse, error) {
        if err := g.svc.Healthy(ctx); err != nil {
                return &conduitv1.HealthCheckResponse{Status: "unhealthy"}, nil
        }
        return &conduitv1.HealthCheckResponse{Status: "ok"}, nil
}

func toProtoJob(job *model.Job) *conduitv1.Job {
        return &conduitv1.Job{
                Id:             job.ID,
                Type:           job.Type,
                PayloadJson:    string(job.Payload),
                Status:         string(job.Status),
                Priority:       int32(job.Priority),
                Attempts:       int32(job.Attempts),
                MaxRetries:     int32(job.MaxRetries),
                IdempotencyKey: derefString(job.IdempotencyKey),
                ScheduledAt:    formatTime(job.ScheduledAt),
                StartedAt:      formatTime(job.StartedAt),
                FinishedAt:     formatTime(job.FinishedAt),
                LastError:      derefString(job.LastError),
                WorkerId:       derefString(job.WorkerID),
                ResultJson:     string(job.Result),
                RunId:          derefString(job.RunID),
                NodeId:         derefString(job.NodeID),
                CreatedAt:      formatTime(&job.CreatedAt),
        }
}

func derefString(s *string) string {
        if s == nil {
                return ""
        }
        return *s
}

func formatTime(t *time.Time) string {
        if t == nil {
                return ""
        }
        return t.UTC().Format(time.RFC3339Nano)
}

func mapError(err error) error {
        var invalidInput *InvalidInputError
        switch {
        case errors.As(err, &invalidInput):
                return errInvalidArgument(err)
        case errors.Is(err, storage.ErrNotFound):
                return errNotFound(err)
        case errors.Is(err, storage.ErrStatusConflict), errors.Is(err, storage.ErrDuplicateKey):
                return errFailedPrecondition(err)
        default:
                return errInternal(err)
        }
}
