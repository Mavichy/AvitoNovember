package repository

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/Mavichy/AvitoNovember/internal/model"
)

var (
	ErrTeamExists   = errors.New("team already exists")
	ErrUserNotFound = errors.New("user not found")
	ErrTeamNotFound = errors.New("team not found")
	ErrPRExists     = errors.New("pull request already exists")
	ErrPRNotFound   = errors.New("pull request not found")
)

type Repository struct {
	db *sql.DB
}

func NewRepository(db *sql.DB) *Repository {
	return &Repository{db: db}
}

const schemaSQL = `
CREATE TABLE IF NOT EXISTS teams (
    name TEXT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    username TEXT NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    team_name TEXT NOT NULL REFERENCES teams(name)
);

CREATE TABLE IF NOT EXISTS pull_requests (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    author_id TEXT NOT NULL REFERENCES users(id),
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    merged_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS pull_request_reviewers (
    pull_request_id TEXT NOT NULL REFERENCES pull_requests(id) ON DELETE CASCADE,
    reviewer_id TEXT NOT NULL REFERENCES users(id),
    PRIMARY KEY (pull_request_id, reviewer_id)
);
`

func (r *Repository) Migrate(ctx context.Context) error {
	_, err := r.db.ExecContext(ctx, schemaSQL)
	return err
}

func (r *Repository) CreateTeam(ctx context.Context, teamName string, members []model.TeamMember) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var exists bool
	if err := tx.QueryRowContext(ctx,
		"SELECT EXISTS(SELECT 1 FROM teams WHERE name=$1)", teamName).
		Scan(&exists); err != nil {
		return err
	}
	if exists {
		return ErrTeamExists
	}

	if _, err := tx.ExecContext(ctx,
		"INSERT INTO teams (name) VALUES ($1)", teamName); err != nil {
		return err
	}

	for _, m := range members {
		_, err := tx.ExecContext(ctx, `
			INSERT INTO users (id, username, is_active, team_name)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (id) DO UPDATE
			SET username = EXCLUDED.username,
			    is_active = EXCLUDED.is_active,
			    team_name = EXCLUDED.team_name
		`, m.UserID, m.Username, m.IsActive, teamName)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (r *Repository) GetTeam(ctx context.Context, teamName string) (model.Team, error) {
	var exists bool
	if err := r.db.QueryRowContext(ctx,
		"SELECT EXISTS(SELECT 1 FROM teams WHERE name=$1)", teamName).
		Scan(&exists); err != nil {
		return model.Team{}, err
	}
	if !exists {
		return model.Team{}, ErrTeamNotFound
	}

	rows, err := r.db.QueryContext(ctx, `
		SELECT id, username, is_active
		FROM users
		WHERE team_name = $1
		ORDER BY id
	`, teamName)
	if err != nil {
		return model.Team{}, err
	}
	defer rows.Close()

	var members []model.TeamMember
	for rows.Next() {
		var m model.TeamMember
		if err := rows.Scan(&m.UserID, &m.Username, &m.IsActive); err != nil {
			return model.Team{}, err
		}
		members = append(members, m)
	}

	return model.Team{
		TeamName: teamName,
		Members:  members,
	}, nil
}

func (r *Repository) SetUserActive(ctx context.Context, userID string, active bool) (model.User, error) {
	row := r.db.QueryRowContext(ctx, `
		UPDATE users
		SET is_active = $2
		WHERE id = $1
		RETURNING id, username, team_name, is_active
	`, userID, active)

	var u model.User
	if err := row.Scan(&u.UserID, &u.Username, &u.TeamName, &u.IsActive); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return model.User{}, ErrUserNotFound
		}
		return model.User{}, err
	}
	return u, nil
}

func (r *Repository) GetUser(ctx context.Context, userID string) (model.User, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT id, username, team_name, is_active
		FROM users
		WHERE id = $1
	`, userID)

	var u model.User
	if err := row.Scan(&u.UserID, &u.Username, &u.TeamName, &u.IsActive); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return model.User{}, ErrUserNotFound
		}
		return model.User{}, err
	}
	return u, nil
}

func (r *Repository) GetActiveUsersByTeam(ctx context.Context, teamName string) ([]model.User, error) {
	var exists bool
	if err := r.db.QueryRowContext(ctx,
		"SELECT EXISTS(SELECT 1 FROM teams WHERE name=$1)", teamName).
		Scan(&exists); err != nil {
		return nil, err
	}
	if !exists {
		return nil, ErrTeamNotFound
	}

	rows, err := r.db.QueryContext(ctx, `
		SELECT id, username, team_name, is_active
		FROM users
		WHERE team_name = $1 AND is_active = TRUE
	`, teamName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var users []model.User
	for rows.Next() {
		var u model.User
		if err := rows.Scan(&u.UserID, &u.Username, &u.TeamName, &u.IsActive); err != nil {
			return nil, err
		}
		users = append(users, u)
	}
	return users, nil
}

func (r *Repository) CreatePRWithReviewers(ctx context.Context, pr model.PullRequest) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var exists bool
	if err := tx.QueryRowContext(ctx,
		"SELECT EXISTS(SELECT 1 FROM pull_requests WHERE id=$1)", pr.ID).
		Scan(&exists); err != nil {
		return err
	}
	if exists {
		return ErrPRExists
	}

	now := time.Now().UTC()
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO pull_requests (id, name, author_id, status, created_at)
		VALUES ($1, $2, $3, $4, $5)
	`, pr.ID, pr.Name, pr.AuthorID, string(pr.Status), now); err != nil {
		return err
	}

	for _, reviewer := range pr.AssignedReviewers {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO pull_request_reviewers (pull_request_id, reviewer_id)
			VALUES ($1, $2)
		`, pr.ID, reviewer); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (r *Repository) GetPR(ctx context.Context, prID string) (model.PullRequest, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT id, name, author_id, status, created_at, merged_at
		FROM pull_requests
		WHERE id = $1
	`, prID)

	var (
		id, name, authorID, statusStr string
		createdAt                     time.Time
		mergedAt                      *time.Time
	)
	if err := row.Scan(&id, &name, &authorID, &statusStr, &createdAt, &mergedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return model.PullRequest{}, ErrPRNotFound
		}
		return model.PullRequest{}, err
	}

	reviewerRows, err := r.db.QueryContext(ctx, `
		SELECT reviewer_id
		FROM pull_request_reviewers
		WHERE pull_request_id = $1
		ORDER BY reviewer_id
	`, prID)
	if err != nil {
		return model.PullRequest{}, err
	}
	defer reviewerRows.Close()

	var reviewers []string
	for reviewerRows.Next() {
		var rid string
		if err := reviewerRows.Scan(&rid); err != nil {
			return model.PullRequest{}, err
		}
		reviewers = append(reviewers, rid)
	}

	return model.PullRequest{
		ID:                id,
		Name:              name,
		AuthorID:          authorID,
		Status:            model.PullRequestStatus(statusStr),
		AssignedReviewers: reviewers,
		CreatedAt:         &createdAt,
		MergedAt:          mergedAt,
	}, nil
}

func (r *Repository) MarkPRMerged(ctx context.Context, prID string) (model.PullRequest, error) {
	row := r.db.QueryRowContext(ctx, `
		UPDATE pull_requests
		SET status = 'MERGED',
		    merged_at = COALESCE(merged_at, now())
		WHERE id = $1
		RETURNING id, name, author_id, status, created_at, merged_at
	`, prID)

	var (
		id, name, authorID, statusStr string
		createdAt                     time.Time
		mergedAt                      *time.Time
	)

	if err := row.Scan(&id, &name, &authorID, &statusStr, &createdAt, &mergedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return model.PullRequest{}, ErrPRNotFound
		}
		return model.PullRequest{}, err
	}

	reviewerRows, err := r.db.QueryContext(ctx, `
		SELECT reviewer_id
		FROM pull_request_reviewers
		WHERE pull_request_id = $1
		ORDER BY reviewer_id
	`, prID)
	if err != nil {
		return model.PullRequest{}, err
	}
	defer reviewerRows.Close()

	var reviewers []string
	for reviewerRows.Next() {
		var rid string
		if err := reviewerRows.Scan(&rid); err != nil {
			return model.PullRequest{}, err
		}
		reviewers = append(reviewers, rid)
	}

	return model.PullRequest{
		ID:                id,
		Name:              name,
		AuthorID:          authorID,
		Status:            model.PullRequestStatus(statusStr),
		AssignedReviewers: reviewers,
		CreatedAt:         &createdAt,
		MergedAt:          mergedAt,
	}, nil
}

func (r *Repository) ReassignReviewer(ctx context.Context, prID, oldReviewerID, newReviewerID string) error {
	res, err := r.db.ExecContext(ctx, `
		UPDATE pull_request_reviewers
		SET reviewer_id = $3
		WHERE pull_request_id = $1 AND reviewer_id = $2
	`, prID, oldReviewerID, newReviewerID)
	if err != nil {
		return err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return errors.New("no reviewer row updated")
	}
	return nil
}

func (r *Repository) GetPRsForReviewer(ctx context.Context, userID string) ([]model.PullRequestShort, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT p.id, p.name, p.author_id, p.status
		FROM pull_requests p
		JOIN pull_request_reviewers r ON p.id = r.pull_request_id
		WHERE r.reviewer_id = $1
		ORDER BY p.created_at DESC
	`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var res []model.PullRequestShort
	for rows.Next() {
		var s model.PullRequestShort
		var status string
		if err := rows.Scan(&s.ID, &s.Name, &s.AuthorID, &status); err != nil {
			return nil, err
		}
		s.Status = model.PullRequestStatus(status)
		res = append(res, s)
	}
	return res, nil
}

func (r *Repository) GetReviewerStats(ctx context.Context) ([]model.ReviewerStatsItem, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT reviewer_id, COUNT(*) AS cnt
		FROM pull_request_reviewers
		GROUP BY reviewer_id
		ORDER BY cnt DESC, reviewer_id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var res []model.ReviewerStatsItem
	for rows.Next() {
		var item model.ReviewerStatsItem
		if err := rows.Scan(&item.UserID, &item.ReviewCount); err != nil {
			return nil, err
		}
		res = append(res, item)
	}
	return res, nil
}
func (r *Repository) RemoveReviewer(ctx context.Context, prID, reviewerID string) error {
	_, err := r.db.ExecContext(ctx, `
		DELETE FROM pull_request_reviewers
		WHERE pull_request_id = $1 AND reviewer_id = $2
	`, prID, reviewerID)
	return err
}
