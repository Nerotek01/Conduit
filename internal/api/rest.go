package api

import (
        "encoding/json"
        "errors"
        "io"
        "net/http"
        "strconv"
        "time"

        "github.com/google/uuid"

        "github.com/Nerotek01/Conduit/internal/model"
        "github.com/Nerotek01/Conduit/internal/storage"
)

func writeJSON(w http.ResponseWriter, status int, body any) {
        w.Header().Set("Content-Type", "application/json")
        w.WriteHeader(status)
        _ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, msg string) {
        writeJSON(w, status, map[string]string{"error": msg})
}

func readBody(w http.ResponseWriter, r *http.Request, maxBytes int64, dst any) bool {
        r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
        body, err := io.ReadAll(r.Body)
        if err != nil {
                var maxErr *http.MaxBytesError
                if errors.As(err, &maxErr) {
                        writeError(w, http.StatusRequestEntityTooLarge, "request body too large")
                        return false
                }
                writeError(w, http.StatusBadRequest, "unreadable body")
                return false
        }
        if len(body) == 0 {
                writeError(w, http.StatusBadRequest, "empty body")
                return false
        }
        if err := json.Unmarshal(body, dst); err != nil {
                writeError(w, http.StatusBadRequest, "invalid json: "+err.Error())
                return false
        }
        return true
}

func (s *Service) handleSubmitJob(w http.ResponseWriter, r *http.Request) {
        start := time.Now()
        var opts SubmitOptions
        if !readBody(w, r, s.maxPayloadBytes+4096, &opts) {
                return
        }
        job, duplicate, err := s.SubmitJob(r.Context(), opts)
        if err != nil {
                s.writeServiceError(w, err)
                return
        }
        s.met.ObserveSubmission(start)
        status := http.StatusCreated
        if duplicate {
                status = http.StatusOK
        }
        writeJSON(w, status, map[string]any{"job": job, "duplicate": duplicate})
}

func (s *Service) handleGetJob(w http.ResponseWriter, r *http.Request) {
        id := r.PathValue("id")
        job, err := s.GetJob(r.Context(), id)
        if err != nil {
                s.writeServiceError(w, err)
                return
        }
        writeJSON(w, http.StatusOK, map[string]any{"job": job})
}

func (s *Service) handleListJobs(w http.ResponseWriter, r *http.Request) {
        q := r.URL.Query()
        limit := 50
        if v := q.Get("limit"); v != "" {
                n, err := strconv.Atoi(v)
                if err != nil || n < 1 || n > 200 {
                        writeError(w, http.StatusBadRequest, "limit must be 1-200")
                        return
                }
                limit = n
        }
        var cursor *time.Time
        beforeID := ""
        if v := q.Get("cursor"); v != "" {
                t, err := time.Parse(time.RFC3339Nano, v)
                if err != nil {
                        writeError(w, http.StatusBadRequest, "invalid cursor")
                        return
                }
                cursor = &t
        }
        if v := q.Get("before_id"); v != "" {
                if _, err := uuid.Parse(v); err != nil {
                        writeError(w, http.StatusBadRequest, "invalid before_id")
                        return
                }
                if cursor == nil {
                        writeError(w, http.StatusBadRequest, "before_id requires cursor (pass both next_cursor and next_before_id from the previous response)")
                        return
                }
                beforeID = v
        }
        res, err := s.ListJobs(r.Context(), ListOptions{
                Status:   q.Get("status"),
                Type:     q.Get("type"),
                Limit:    limit,
                Cursor:   cursor,
                BeforeID: beforeID,
        })
        if err != nil {
                s.writeServiceError(w, err)
                return
        }
        writeJSON(w, http.StatusOK, res)
}

func (s *Service) handleCancelJob(w http.ResponseWriter, r *http.Request) {
        id := r.PathValue("id")
        job, err := s.CancelJob(r.Context(), id)
        if err != nil {
                s.writeServiceError(w, err)
                return
        }
        writeJSON(w, http.StatusOK, map[string]any{"job": job})
}

func (s *Service) handleRequeueJob(w http.ResponseWriter, r *http.Request) {
        id := r.PathValue("id")
        job, err := s.RequeueJob(r.Context(), id)
        if err != nil {
                s.writeServiceError(w, err)
                return
        }
        writeJSON(w, http.StatusOK, map[string]any{"job": job})
}

func (s *Service) handleCreateWorkflow(w http.ResponseWriter, r *http.Request) {
        var def model.WorkflowDefinition
        if !readBody(w, r, 2<<20, &def) {
                return
        }
        wf, err := s.CreateWorkflow(r.Context(), def)
        if err != nil {
                s.writeServiceError(w, err)
                return
        }
        writeJSON(w, http.StatusCreated, map[string]any{"workflow": wf})
}

func (s *Service) handleListWorkflows(w http.ResponseWriter, r *http.Request) {
        limit := 50
        if v := r.URL.Query().Get("limit"); v != "" {
                n, err := strconv.Atoi(v)
                if err != nil || n < 1 || n > 200 {
                        writeError(w, http.StatusBadRequest, "limit must be 1-200")
                        return
                }
                limit = n
        }
        workflows, err := s.ListWorkflows(r.Context(), limit)
        if err != nil {
                s.writeServiceError(w, err)
                return
        }
        if workflows == nil {
                workflows = []model.Workflow{}
        }
        writeJSON(w, http.StatusOK, map[string]any{"workflows": workflows})
}

func (s *Service) handleGetWorkflow(w http.ResponseWriter, r *http.Request) {
        id := r.PathValue("id")
        wf, err := s.GetWorkflow(r.Context(), id)
        if err != nil {
                s.writeServiceError(w, err)
                return
        }
        writeJSON(w, http.StatusOK, map[string]any{"workflow": wf})
}

func (s *Service) handleRunWorkflow(w http.ResponseWriter, r *http.Request) {
        id := r.PathValue("id")
        run, err := s.RunWorkflow(r.Context(), id)
        if err != nil {
                s.writeServiceError(w, err)
                return
        }
        writeJSON(w, http.StatusCreated, map[string]any{"run": run})
}

func (s *Service) handleListRuns(w http.ResponseWriter, r *http.Request) {
        id := r.PathValue("id")
        limit := 20
        if v := r.URL.Query().Get("limit"); v != "" {
                n, err := strconv.Atoi(v)
                if err != nil || n < 1 || n > 100 {
                        writeError(w, http.StatusBadRequest, "limit must be 1-100")
                        return
                }
                limit = n
        }
        runs, err := s.ListRuns(r.Context(), id, limit)
        if err != nil {
                s.writeServiceError(w, err)
                return
        }
        writeJSON(w, http.StatusOK, map[string]any{"runs": runs})
}

func (s *Service) handleQueues(w http.ResponseWriter, r *http.Request) {
        snap, err := s.QueueSnapshot(r.Context())
        if err != nil {
                s.writeServiceError(w, err)
                return
        }
        writeJSON(w, http.StatusOK, snap)
}

func (s *Service) handleWorkers(w http.ResponseWriter, r *http.Request) {
        workers, err := s.ListWorkers(r.Context())
        if err != nil {
                s.writeServiceError(w, err)
                return
        }
        if workers == nil {
                workers = []model.WorkerInfo{}
        }
        writeJSON(w, http.StatusOK, map[string]any{"workers": workers})
}

type scheduleInput struct {
        JobType    string          `json:"job_type"`
        Payload    json.RawMessage `json:"payload"`
        Cron       string          `json:"cron"`
        Timezone   string          `json:"timezone"`
        Priority   int             `json:"priority"`
        MaxRetries int             `json:"max_retries"`
}

func (s *Service) handleCreateSchedule(w http.ResponseWriter, r *http.Request) {
        var in scheduleInput
        if !readBody(w, r, s.maxPayloadBytes+4096, &in) {
                return
        }
        next, err := nextScheduleRun(in.Cron, in.Timezone)
        if err != nil {
                writeError(w, http.StatusBadRequest, err.Error())
                return
        }
        sch, err := s.CreateSchedule(r.Context(), model.Schedule{
                JobType:    in.JobType,
                Payload:    normalizePayload(in.Payload),
                Cron:       in.Cron,
                Timezone:   in.Timezone,
                Priority:   in.Priority,
                MaxRetries: in.MaxRetries,
        }, next)
        if err != nil {
                s.writeServiceError(w, err)
                return
        }
        writeJSON(w, http.StatusCreated, map[string]any{"schedule": sch})
}

func (s *Service) handleListSchedules(w http.ResponseWriter, r *http.Request) {
        schedules, err := s.ListSchedules(r.Context())
        if err != nil {
                s.writeServiceError(w, err)
                return
        }
        if schedules == nil {
                schedules = []model.Schedule{}
        }
        writeJSON(w, http.StatusOK, map[string]any{"schedules": schedules})
}

func (s *Service) handleDeleteSchedule(w http.ResponseWriter, r *http.Request) {
        id := r.PathValue("id")
        if err := s.DeleteSchedule(r.Context(), id); err != nil {
                s.writeServiceError(w, err)
                return
        }
        w.WriteHeader(http.StatusNoContent)
}

func (s *Service) handleHealthz(w http.ResponseWriter, r *http.Request) {
        if err := s.Healthy(r.Context()); err != nil {
                writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unhealthy", "error": err.Error()})
                return
        }
        writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Service) writeServiceError(w http.ResponseWriter, err error) {
        var invalidInput *InvalidInputError
        switch {
        case errors.As(err, &invalidInput):
                writeError(w, http.StatusBadRequest, invalidInput.Error())
        case errors.Is(err, storage.ErrNotFound):
                writeError(w, http.StatusNotFound, err.Error())
        case errors.Is(err, storage.ErrStatusConflict):
                writeError(w, http.StatusConflict, err.Error())
        case errors.Is(err, storage.ErrDuplicateKey):
                writeError(w, http.StatusConflict, err.Error())
        default:
                s.logger.Error("service error", "error", err)
                writeError(w, http.StatusInternalServerError, "internal error")
        }
}
