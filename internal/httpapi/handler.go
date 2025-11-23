package httpapi

import (
	"encoding/json"
	"net/http"

	"github.com/Mavichy/AvitoNovember/internal/model"
	"github.com/Mavichy/AvitoNovember/internal/service"
)

type Handler struct {
	svc *service.Service
}

func NewHandler(svc *service.Service) http.Handler {
	h := &Handler{svc: svc}

	mux := http.NewServeMux()

	mux.Handle("/team/add", method("POST", h.handleTeamAdd))
	mux.Handle("/team/get", method("GET", h.handleTeamGet))
	mux.Handle("/team/deactivateAndReassign", method("POST", h.handleTeamDeactivateAndReassign))

	mux.Handle("/users/setIsActive", method("POST", h.handleUsersSetIsActive))
	mux.Handle("/users/getReview", method("GET", h.handleUsersGetReview))

	mux.Handle("/pullRequest/create", method("POST", h.handlePRCreate))
	mux.Handle("/pullRequest/merge", method("POST", h.handlePRMerge))
	mux.Handle("/pullRequest/reassign", method("POST", h.handlePRReassign))

	mux.Handle("/stats/reviewers", method("GET", h.handleStatsReviewers))

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	return mux
}

func method(method string, h func(http.ResponseWriter, *http.Request)) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != method {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		h(w, r)
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeDomainError(w http.ResponseWriter, err *service.DomainError, defaultStatus int) {
	var status int
	switch err.Code {
	case model.ErrorCodeTeamExists:
		status = http.StatusBadRequest
	case model.ErrorCodePRExists,
		model.ErrorCodePRMerged,
		model.ErrorCodeNotAssigned,
		model.ErrorCodeNoCandidate:
		status = http.StatusConflict
	case model.ErrorCodeNotFound:
		status = http.StatusNotFound
	default:
		status = defaultStatus
	}

	writeJSON(w, status, model.ErrorResponse{
		Error: model.ErrorDetail{
			Code:    err.Code,
			Message: err.Message,
		},
	})
}

func writeError(w http.ResponseWriter, err error) {
	if de, ok := service.AsDomainError(err); ok {
		writeDomainError(w, de, http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusInternalServerError, model.ErrorResponse{
		Error: model.ErrorDetail{
			Code:    model.ErrorCodeNotFound,
			Message: "internal server error",
		},
	})
}

// POST /team/add
func (h *Handler) handleTeamAdd(w http.ResponseWriter, r *http.Request) {
	var req model.Team
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, model.ErrorResponse{
			Error: model.ErrorDetail{
				Code:    model.ErrorCodeNotFound,
				Message: "invalid json",
			},
		})
		return
	}

	team, err := h.svc.AddTeam(r.Context(), req)
	if err != nil {
		writeError(w, err)
		return
	}

	writeJSON(w, http.StatusCreated, map[string]any{
		"team": team,
	})
}

// GET /team/get?team_name=...
func (h *Handler) handleTeamGet(w http.ResponseWriter, r *http.Request) {
	teamName := r.URL.Query().Get("team_name")
	if teamName == "" {
		writeJSON(w, http.StatusBadRequest, model.ErrorResponse{
			Error: model.ErrorDetail{
				Code:    model.ErrorCodeNotFound,
				Message: "team_name is required",
			},
		})
		return
	}

	team, err := h.svc.GetTeam(r.Context(), teamName)
	if err != nil {
		writeError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, team)
}

// POST /users/setIsActive
type setIsActiveRequest struct {
	UserID   string `json:"user_id"`
	IsActive bool   `json:"is_active"`
}

type teamDeactivateRequest struct {
	TeamName string   `json:"team_name"`
	UserIDs  []string `json:"user_ids"`
}

type teamDeactivateResponse struct {
	TeamName             string   `json:"team_name"`
	Deactivated          []string `json:"deactivated"`
	ReassignedReviewers  int      `json:"reassigned_reviewers"`
	RemovedReviewers     int      `json:"removed_reviewers"`
	AffectedPullRequests int      `json:"affected_pull_requests"`
}

func (h *Handler) handleUsersSetIsActive(w http.ResponseWriter, r *http.Request) {
	var req setIsActiveRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, model.ErrorResponse{
			Error: model.ErrorDetail{
				Code:    model.ErrorCodeNotFound,
				Message: "invalid json",
			},
		})
		return
	}

	user, err := h.svc.SetUserIsActive(r.Context(), req.UserID, req.IsActive)
	if err != nil {
		writeError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"user": user,
	})
}

// GET /users/getReview?user_id=...
func (h *Handler) handleUsersGetReview(w http.ResponseWriter, r *http.Request) {
	userID := r.URL.Query().Get("user_id")
	if userID == "" {
		writeJSON(w, http.StatusBadRequest, model.ErrorResponse{
			Error: model.ErrorDetail{
				Code:    model.ErrorCodeNotFound,
				Message: "user_id is required",
			},
		})
		return
	}

	id, prs, err := h.svc.GetUserReviews(r.Context(), userID)
	if err != nil {
		writeError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"user_id":       id,
		"pull_requests": prs,
	})
}

// POST /pullRequest/create
type createPRRequest struct {
	ID       string `json:"pull_request_id"`
	Name     string `json:"pull_request_name"`
	AuthorID string `json:"author_id"`
}

func (h *Handler) handlePRCreate(w http.ResponseWriter, r *http.Request) {
	var req createPRRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, model.ErrorResponse{
			Error: model.ErrorDetail{
				Code:    model.ErrorCodeNotFound,
				Message: "invalid json",
			},
		})
		return
	}

	pr, err := h.svc.CreatePR(r.Context(), service.CreatePRInput{
		ID:       req.ID,
		Name:     req.Name,
		AuthorID: req.AuthorID,
	})
	if err != nil {
		writeError(w, err)
		return
	}

	writeJSON(w, http.StatusCreated, map[string]any{
		"pr": pr,
	})
}

// POST /pullRequest/merge
type mergePRRequest struct {
	ID string `json:"pull_request_id"`
}

func (h *Handler) handlePRMerge(w http.ResponseWriter, r *http.Request) {
	var req mergePRRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, model.ErrorResponse{
			Error: model.ErrorDetail{
				Code:    model.ErrorCodeNotFound,
				Message: "invalid json",
			},
		})
		return
	}

	pr, err := h.svc.MergePR(r.Context(), req.ID)
	if err != nil {
		writeError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"pr": pr,
	})
}

// POST /pullRequest/reassign
type reassignPRRequest struct {
	PullRequestID string `json:"pull_request_id"`
	OldUserID     string `json:"old_user_id"`
	OldReviewerID string `json:"old_reviewer_id"` // из-за example
}

func (h *Handler) handlePRReassign(w http.ResponseWriter, r *http.Request) {
	var req reassignPRRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, model.ErrorResponse{
			Error: model.ErrorDetail{
				Code:    model.ErrorCodeNotFound,
				Message: "invalid json",
			},
		})
		return
	}

	if req.OldUserID == "" {
		req.OldUserID = req.OldReviewerID
	}

	res, err := h.svc.ReassignReviewer(r.Context(), req.PullRequestID, req.OldUserID)
	if err != nil {
		writeError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"pr":          res.PR,
		"replaced_by": res.ReplacedBy,
	})
}

// GET /stats/reviewers
func (h *Handler) handleStatsReviewers(w http.ResponseWriter, r *http.Request) {
	stats, err := h.svc.GetReviewerStats(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"items": stats,
	})
}

// POST /team/deactivateAndReassign
func (h *Handler) handleTeamDeactivateAndReassign(w http.ResponseWriter, r *http.Request) {
	var req teamDeactivateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, model.ErrorResponse{
			Error: model.ErrorDetail{
				Code:    model.ErrorCodeNotFound,
				Message: "invalid json",
			},
		})
		return
	}

	if req.TeamName == "" || len(req.UserIDs) == 0 {
		writeJSON(w, http.StatusBadRequest, model.ErrorResponse{
			Error: model.ErrorDetail{
				Code:    model.ErrorCodeNotFound,
				Message: "team_name and user_ids are required",
			},
		})
		return
	}

	res, err := h.svc.DeactivateTeamUsersAndReassign(r.Context(), req.TeamName, req.UserIDs)
	if err != nil {
		writeError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, teamDeactivateResponse{
		TeamName:             res.TeamName,
		Deactivated:          res.Deactivated,
		ReassignedReviewers:  res.ReassignedReviewers,
		RemovedReviewers:     res.RemovedReviewers,
		AffectedPullRequests: res.AffectedPullRequests,
	})
}
