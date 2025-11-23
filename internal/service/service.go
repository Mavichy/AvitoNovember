package service

import (
	"context"
	"errors"
	"math/rand"
	"time"

	"github.com/Mavichy/AvitoNovember/internal/model"
	"github.com/Mavichy/AvitoNovember/internal/repository"
)

type DomainError struct {
	Code    model.ErrorCode
	Message string
}

func (e *DomainError) Error() string { return e.Message }

func NewDomainError(code model.ErrorCode, msg string) *DomainError {
	return &DomainError{Code: code, Message: msg}
}

func AsDomainError(err error) (*DomainError, bool) {
	var de *DomainError
	if errors.As(err, &de) {
		return de, true
	}
	return nil, false
}

type Service struct {
	repo *repository.Repository
	rand *rand.Rand
}

func NewService(repo *repository.Repository) *Service {
	return &Service{
		repo: repo,
		rand: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (s *Service) AddTeam(ctx context.Context, team model.Team) (model.Team, error) {
	err := s.repo.CreateTeam(ctx, team.TeamName, team.Members)
	if err != nil {
		if errors.Is(err, repository.ErrTeamExists) {
			return model.Team{}, NewDomainError(model.ErrorCodeTeamExists, "team_name already exists")
		}
		return model.Team{}, err
	}
	return s.repo.GetTeam(ctx, team.TeamName)
}

func (s *Service) GetTeam(ctx context.Context, teamName string) (model.Team, error) {
	team, err := s.repo.GetTeam(ctx, teamName)
	if err != nil {
		if errors.Is(err, repository.ErrTeamNotFound) {
			return model.Team{}, NewDomainError(model.ErrorCodeNotFound, "team not found")
		}
		return model.Team{}, err
	}
	return team, nil
}

func (s *Service) SetUserIsActive(ctx context.Context, userID string, isActive bool) (model.User, error) {
	u, err := s.repo.SetUserActive(ctx, userID, isActive)
	if err != nil {
		if errors.Is(err, repository.ErrUserNotFound) {
			return model.User{}, NewDomainError(model.ErrorCodeNotFound, "user not found")
		}
		return model.User{}, err
	}
	return u, nil
}

func (s *Service) GetUserReviews(ctx context.Context, userID string) (string, []model.PullRequestShort, error) {
	prs, err := s.repo.GetPRsForReviewer(ctx, userID)
	return userID, prs, err
}

type CreatePRInput struct {
	ID       string
	Name     string
	AuthorID string
}

func (s *Service) CreatePR(ctx context.Context, in CreatePRInput) (model.PullRequest, error) {
	author, err := s.repo.GetUser(ctx, in.AuthorID)
	if err != nil {
		if errors.Is(err, repository.ErrUserNotFound) {
			return model.PullRequest{}, NewDomainError(model.ErrorCodeNotFound, "author not found")
		}
		return model.PullRequest{}, err
	}

	candidates, err := s.repo.GetActiveUsersByTeam(ctx, author.TeamName)
	if err != nil {
		if errors.Is(err, repository.ErrTeamNotFound) {
			return model.PullRequest{}, NewDomainError(model.ErrorCodeNotFound, "team not found")
		}
		return model.PullRequest{}, err
	}

	var reviewerIDs []string
	for _, u := range candidates {
		if u.UserID == author.UserID {
			continue
		}
		reviewerIDs = append(reviewerIDs, u.UserID)
	}

	s.shuffle(reviewerIDs)
	if len(reviewerIDs) > 2 {
		reviewerIDs = reviewerIDs[:2]
	}

	pr := model.PullRequest{
		ID:                in.ID,
		Name:              in.Name,
		AuthorID:          in.AuthorID,
		Status:            model.StatusOpen,
		AssignedReviewers: reviewerIDs,
	}

	if err := s.repo.CreatePRWithReviewers(ctx, pr); err != nil {
		if errors.Is(err, repository.ErrPRExists) {
			return model.PullRequest{}, NewDomainError(model.ErrorCodePRExists, "PR id already exists")
		}
		return model.PullRequest{}, err
	}

	return s.repo.GetPR(ctx, in.ID)
}

func (s *Service) MergePR(ctx context.Context, prID string) (model.PullRequest, error) {
	pr, err := s.repo.MarkPRMerged(ctx, prID)
	if err != nil {
		if errors.Is(err, repository.ErrPRNotFound) {
			return model.PullRequest{}, NewDomainError(model.ErrorCodeNotFound, "pull request not found")
		}
		return model.PullRequest{}, err
	}
	return pr, nil
}

type ReassignResult struct {
	PR         model.PullRequest
	ReplacedBy string
}

type BulkDeactivateResult struct {
	TeamName             string   `json:"team_name"`
	Deactivated          []string `json:"deactivated"`
	ReassignedReviewers  int      `json:"reassigned_reviewers"`
	RemovedReviewers     int      `json:"removed_reviewers"`
	AffectedPullRequests int      `json:"affected_pull_requests"`
}

func (s *Service) ReassignReviewer(ctx context.Context, prID, oldUserID string) (ReassignResult, error) {
	pr, err := s.repo.GetPR(ctx, prID)
	if err != nil {
		if errors.Is(err, repository.ErrPRNotFound) {
			return ReassignResult{}, NewDomainError(model.ErrorCodeNotFound, "pull request not found")
		}
		return ReassignResult{}, err
	}

	if pr.Status == model.StatusMerged {
		return ReassignResult{}, NewDomainError(model.ErrorCodePRMerged, "cannot reassign on merged PR")
	}

	oldUser, err := s.repo.GetUser(ctx, oldUserID)
	if err != nil {
		if errors.Is(err, repository.ErrUserNotFound) {
			return ReassignResult{}, NewDomainError(model.ErrorCodeNotFound, "user not found")
		}
		return ReassignResult{}, err
	}

	isAssigned := false
	for _, rid := range pr.AssignedReviewers {
		if rid == oldUserID {
			isAssigned = true
			break
		}
	}
	if !isAssigned {
		return ReassignResult{}, NewDomainError(model.ErrorCodeNotAssigned, "reviewer is not assigned to this PR")
	}

	candidates, err := s.repo.GetActiveUsersByTeam(ctx, oldUser.TeamName)
	if err != nil {
		if errors.Is(err, repository.ErrTeamNotFound) {
			return ReassignResult{}, NewDomainError(model.ErrorCodeNotFound, "team not found")
		}
		return ReassignResult{}, err
	}

	assignedSet := make(map[string]struct{}, len(pr.AssignedReviewers))
	for _, rid := range pr.AssignedReviewers {
		assignedSet[rid] = struct{}{}
	}
	assignedSet[oldUserID] = struct{}{}
	assignedSet[pr.AuthorID] = struct{}{}

	var eligible []string
	for _, u := range candidates {
		if _, bad := assignedSet[u.UserID]; bad {
			continue
		}
		eligible = append(eligible, u.UserID)
	}

	if len(eligible) == 0 {
		return ReassignResult{}, NewDomainError(model.ErrorCodeNoCandidate, "no active replacement candidate in team")
	}

	s.shuffle(eligible)
	newReviewer := eligible[0]

	if err := s.repo.ReassignReviewer(ctx, prID, oldUserID, newReviewer); err != nil {
		return ReassignResult{}, err
	}

	updated, err := s.repo.GetPR(ctx, prID)
	if err != nil {
		return ReassignResult{}, err
	}

	return ReassignResult{
		PR:         updated,
		ReplacedBy: newReviewer,
	}, nil
}

func (s *Service) shuffle(ids []string) {
	for i := range ids {
		j := s.rand.Intn(i + 1)
		ids[i], ids[j] = ids[j], ids[i]
	}
}

func (s *Service) DeactivateTeamUsersAndReassign(ctx context.Context, teamName string, userIDs []string) (BulkDeactivateResult, error) {
	res := BulkDeactivateResult{
		TeamName: teamName,
	}

	if len(userIDs) == 0 {
		return res, nil
	}

	var toProcess []string
	for _, uid := range userIDs {
		user, err := s.repo.GetUser(ctx, uid)
		if err != nil {
			if errors.Is(err, repository.ErrUserNotFound) {
				return res, NewDomainError(model.ErrorCodeNotFound, "user "+uid+" not found")
			}
			return res, err
		}
		if user.TeamName != teamName {
			return res, NewDomainError(model.ErrorCodeNotFound, "user "+uid+" does not belong to team "+teamName)
		}

		if user.IsActive {
			if _, err := s.repo.SetUserActive(ctx, uid, false); err != nil {
				if errors.Is(err, repository.ErrUserNotFound) {
					return res, NewDomainError(model.ErrorCodeNotFound, "user "+uid+" not found")
				}
				return res, err
			}
		}

		toProcess = append(toProcess, uid)
		res.Deactivated = append(res.Deactivated, uid)
	}

	affectedPRs := make(map[string]struct{})

	for _, uid := range toProcess {
		_, prs, err := s.GetUserReviews(ctx, uid)
		if err != nil {
			return res, err
		}

		for _, prShort := range prs {
			if prShort.Status != model.StatusOpen {
				continue
			}

			rr, err := s.ReassignReviewer(ctx, prShort.ID, uid)
			if err == nil {
				res.ReassignedReviewers++
				affectedPRs[rr.PR.ID] = struct{}{}
				continue
			}

			if de, ok := AsDomainError(err); ok && de.Code == model.ErrorCodeNoCandidate {
				if err := s.repo.RemoveReviewer(ctx, prShort.ID, uid); err != nil {
					return res, err
				}
				res.RemovedReviewers++
				affectedPRs[prShort.ID] = struct{}{}
				continue
			}

			return res, err
		}
	}

	res.AffectedPullRequests = len(affectedPRs)
	return res, nil
}

func (s *Service) GetReviewerStats(ctx context.Context) ([]model.ReviewerStatsItem, error) {
	return s.repo.GetReviewerStats(ctx)
}
