package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"feature-flag-service/internal/constants"
	"feature-flag-service/internal/mgmt/model"
	"feature-flag-service/internal/mgmt/repository"
	"feature-flag-service/internal/pkg/id"
)

type Service struct {
	repo *repository.Repository
}

func New(repo *repository.Repository) *Service {
	return &Service{repo: repo}
}

func validateCaller(caller string) error {
	if strings.TrimSpace(caller) == "" {
		return errors.New(constants.ErrMsgCallerIdentityRequired)
	}
	return nil
}

func validateRules(rulesJSON []byte) error {
	if len(rulesJSON) == 0 || string(rulesJSON) == "[]" {
		return nil
	}
	var rules []model.Rule
	if err := json.Unmarshal(rulesJSON, &rules); err != nil {
		return fmt.Errorf("%s: %w", constants.ErrMsgInvalidRequestBody, err)
	}
	for _, r := range rules {
		if r.Order <= 0 {
			return errors.New(constants.ErrMsgInvalidRuleOrder)
		}
		if r.RolloutPercentage != nil {
			if *r.RolloutPercentage < 0 || *r.RolloutPercentage > 100 {
				return errors.New(constants.ErrMsgInvalidRolloutPct)
			}
		}
		if r.Segment == "" && r.RolloutPercentage == nil {
			return errors.New(constants.ErrMsgInvalidRuleDefinition)
		}
	}
	return nil
}

// --- Tenants & Environments ---

func (s *Service) CreateTenant(ctx context.Context, req model.CreateTenantRequest) (*model.TenantResponse, error) {
	name := strings.TrimSpace(req.Name)
	if name == "" {
		return nil, errors.New(constants.ErrMsgTenantNameRequired)
	}
	tenantID := id.New(constants.PrefixTenant)
	tenant, err := s.repo.CreateTenant(ctx, tenantID, name)
	if err != nil {
		return nil, err
	}
	return model.ToTenantResponse(tenant), nil
}

func (s *Service) GetTenant(ctx context.Context, tenantID string) (*model.TenantResponse, error) {
	tenant, err := s.repo.GetTenant(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	return model.ToTenantResponse(tenant), nil
}

func (s *Service) ListTenants(ctx context.Context) ([]model.TenantResponse, error) {
	tenants, err := s.repo.ListTenants(ctx)
	if err != nil {
		return nil, err
	}
	return model.ToTenantResponses(tenants), nil
}

func (s *Service) CreateEnvironment(ctx context.Context, tenantID string, req model.CreateEnvironmentRequest) (*model.EnvironmentResponse, error) {
	name := strings.TrimSpace(req.Name)
	if name == "" {
		return nil, errors.New(constants.ErrMsgEnvironmentNameRequired)
	}
	envID := id.New(constants.PrefixEnvironment)
	env, err := s.repo.CreateEnvironment(ctx, envID, tenantID, name)
	if err != nil {
		return nil, err
	}
	return model.ToEnvironmentResponse(env), nil
}

func (s *Service) GetEnvironment(ctx context.Context, envID string) (*model.EnvironmentResponse, error) {
	env, err := s.repo.GetEnvironment(ctx, envID)
	if err != nil {
		return nil, err
	}
	return model.ToEnvironmentResponse(env), nil
}

func (s *Service) ListEnvironments(ctx context.Context, tenantID string) ([]model.EnvironmentResponse, error) {
	envs, err := s.repo.ListEnvironments(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	return model.ToEnvironmentResponses(envs), nil
}

// --- SDK Keys ---

func (s *Service) CreateSDKKey(ctx context.Context, envID string, req model.CreateSDKKeyRequest) (*model.SDKKeyResponse, error) {
	if req.KeyType != constants.KeyTypeClient && req.KeyType != constants.KeyTypeServer {
		return nil, errors.New(constants.ErrMsgInvalidKeyType)
	}
	key, err := s.repo.CreateSDKKey(ctx, envID, req)
	if err != nil {
		return nil, err
	}
	return model.ToSDKKeyResponse(key), nil
}

func (s *Service) ListSDKKeys(ctx context.Context, envID string) ([]model.SDKKeyResponse, error) {
	keys, err := s.repo.ListSDKKeys(ctx, envID)
	if err != nil {
		return nil, err
	}
	return model.ToSDKKeyResponses(keys), nil
}

// --- Flags ---

func (s *Service) CreateFlag(ctx context.Context, envID string, req model.CreateFlagRequest, changedBy string) (*model.FlagResponse, error) {
	if err := validateCaller(changedBy); err != nil {
		return nil, err
	}
	if strings.TrimSpace(req.Key) == "" {
		return nil, errors.New(constants.ErrMsgFlagKeyRequired)
	}
	if req.State != "" && req.State != constants.FlagStateOn && req.State != constants.FlagStateOff {
		return nil, errors.New(constants.ErrMsgInvalidFlagState)
	}
	if err := validateRules(req.Rules); err != nil {
		return nil, err
	}
	flag, err := s.repo.CreateFlag(ctx, envID, req, changedBy)
	if err != nil {
		return nil, err
	}
	return model.ToFlagResponse(flag), nil
}

func (s *Service) GetFlag(ctx context.Context, envID, key string) (*model.FlagResponse, error) {
	flag, err := s.repo.GetFlag(ctx, envID, key)
	if err != nil {
		return nil, err
	}
	return model.ToFlagResponse(flag), nil
}

func (s *Service) ListFlags(ctx context.Context, envID string) ([]model.FlagResponse, error) {
	flags, err := s.repo.ListFlags(ctx, envID)
	if err != nil {
		return nil, err
	}
	return model.ToFlagResponses(flags), nil
}

func (s *Service) UpdateFlag(ctx context.Context, envID, key string, req model.UpdateFlagRequest, ifMatchHeader string, changedBy string) (*model.FlagResponse, error) {
	if err := validateCaller(changedBy); err != nil {
		return nil, err
	}

	var expectedVersion *int64
	if ifMatchHeader != "" {
		clean := strings.Trim(ifMatchHeader, `"`)
		ver, err := strconv.ParseInt(clean, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", constants.ErrMsgInvalidIfMatch, err)
		}
		expectedVersion = &ver
	}

	if req.State != nil && *req.State != constants.FlagStateOn && *req.State != constants.FlagStateOff {
		return nil, errors.New(constants.ErrMsgInvalidFlagState)
	}

	if len(req.Rules) > 0 {
		if err := validateRules(req.Rules); err != nil {
			return nil, err
		}
	}

	flag, err := s.repo.UpdateFlag(ctx, envID, key, req, expectedVersion, changedBy)
	if err != nil {
		return nil, err
	}
	return model.ToFlagResponse(flag), nil
}

func (s *Service) KillFlag(ctx context.Context, envID, key, changedBy string) (*model.FlagResponse, error) {
	if err := validateCaller(changedBy); err != nil {
		return nil, err
	}
	flag, err := s.repo.KillFlag(ctx, envID, key, changedBy)
	if err != nil {
		return nil, err
	}
	return model.ToFlagResponse(flag), nil
}

func (s *Service) RollbackFlag(ctx context.Context, envID, key string, targetVersion int64, changedBy string) (*model.FlagResponse, error) {
	if err := validateCaller(changedBy); err != nil {
		return nil, err
	}
	if targetVersion <= 0 {
		return nil, errors.New(constants.ErrMsgTargetVersionPositive)
	}
	flag, err := s.repo.RollbackFlag(ctx, envID, key, targetVersion, changedBy)
	if err != nil {
		return nil, err
	}
	return model.ToFlagResponse(flag), nil
}

func (s *Service) GetFlagHistory(ctx context.Context, envID, key string) ([]model.FlagHistoryResponse, error) {
	history, err := s.repo.GetFlagHistory(ctx, envID, key)
	if err != nil {
		return nil, err
	}
	return model.ToFlagHistoryResponses(history), nil
}

// --- Segments ---

func (s *Service) CreateSegment(ctx context.Context, envID string, req model.CreateSegmentRequest) (*model.SegmentResponse, error) {
	if strings.TrimSpace(req.Key) == "" {
		return nil, errors.New(constants.ErrMsgSegmentKeyRequired)
	}
	seg, err := s.repo.CreateSegment(ctx, envID, req)
	if err != nil {
		return nil, err
	}
	return model.ToSegmentResponse(seg), nil
}

func (s *Service) GetSegment(ctx context.Context, envID, key string) (*model.SegmentResponse, error) {
	seg, err := s.repo.GetSegment(ctx, envID, key)
	if err != nil {
		return nil, err
	}
	return model.ToSegmentResponse(seg), nil
}

func (s *Service) ListSegments(ctx context.Context, envID string) ([]model.SegmentResponse, error) {
	segs, err := s.repo.ListSegments(ctx, envID)
	if err != nil {
		return nil, err
	}
	return model.ToSegmentResponses(segs), nil
}

func (s *Service) UpdateSegment(ctx context.Context, envID, key string, req model.UpdateSegmentRequest, ifMatchHeader string) (*model.SegmentResponse, error) {
	var expectedVersion *int64
	if ifMatchHeader != "" {
		clean := strings.Trim(ifMatchHeader, `"`)
		ver, err := strconv.ParseInt(clean, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", constants.ErrMsgInvalidIfMatch, err)
		}
		expectedVersion = &ver
	}

	seg, err := s.repo.UpdateSegment(ctx, envID, key, req, expectedVersion)
	if err != nil {
		return nil, err
	}
	return model.ToSegmentResponse(seg), nil
}
