package model

import "time"

type ErrorCode string

const (
	ErrorCodeTeamExists  ErrorCode = "TEAM_EXISTS"
	ErrorCodePRExists    ErrorCode = "PR_EXISTS"
	ErrorCodePRMerged    ErrorCode = "PR_MERGED"
	ErrorCodeNotAssigned ErrorCode = "NOT_ASSIGNED"
	ErrorCodeNoCandidate ErrorCode = "NO_CANDIDATE"
	ErrorCodeNotFound    ErrorCode = "NOT_FOUND"
)

type ErrorDetail struct {
	Code    ErrorCode `json:"code"`
	Message string    `json:"message"`
}

type ErrorResponse struct {
	Error ErrorDetail `json:"error"`
}

type TeamMember struct {
	UserID   string `json:"user_id"`
	Username string `json:"username"`
	IsActive bool   `json:"is_active"`
}

type Team struct {
	TeamName string       `json:"team_name"`
	Members  []TeamMember `json:"members"`
}

type User struct {
	UserID   string `json:"user_id"`
	Username string `json:"username"`
	TeamName string `json:"team_name"`
	IsActive bool   `json:"is_active"`
}

type PullRequestStatus string

const (
	StatusOpen   PullRequestStatus = "OPEN"
	StatusMerged PullRequestStatus = "MERGED"
)

type PullRequest struct {
	ID                string            `json:"pull_request_id"`
	Name              string            `json:"pull_request_name"`
	AuthorID          string            `json:"author_id"`
	Status            PullRequestStatus `json:"status"`
	AssignedReviewers []string          `json:"assigned_reviewers"`
	CreatedAt         *time.Time        `json:"createdAt,omitempty"`
	MergedAt          *time.Time        `json:"mergedAt,omitempty"`
}

type PullRequestShort struct {
	ID       string            `json:"pull_request_id"`
	Name     string            `json:"pull_request_name"`
	AuthorID string            `json:"author_id"`
	Status   PullRequestStatus `json:"status"`
}

type ReviewerStatsItem struct {
	UserID      string `json:"user_id"`
	ReviewCount int    `json:"review_count"`
}
