package token

import (
	"context"
	"fmt"
	"sync"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/go-client/pkg/request"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/rollback"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Manager struct {
	keboolaProjectAPI *keboola.API
}

type dependencies interface {
	KeboolaProjectAPI() *keboola.API
}

func NewManager(d dependencies) *Manager {
	return &Manager{keboolaProjectAPI: d.KeboolaProjectAPI()}
}

func (m *Manager) CreateTokens(ctx context.Context, rb rollback.Builder, receiver *model.Receiver) error {
	rb = rb.AddParallel()
	wg := &sync.WaitGroup{}
	errs := errors.NewMultiError()
	for i := range receiver.Exports {
		export := &receiver.Exports[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := m.CreateToken(ctx, rb, export); err != nil {
				errs.Append(err)
			}
		}()
	}
	wg.Wait()
	return errs.ErrorOrNil()
}

func (m *Manager) CreateToken(ctx context.Context, rb rollback.Builder, export *model.Export) error {
	permissions := keboola.BucketPermissions{export.Mapping.TableID.BucketID: keboola.BucketPermissionWrite}
	token, err := m.createTokenRequest(export.ExportKey, permissions).Send(ctx)
	if err != nil {
		return err
	}

	rb.Add(func(ctx context.Context) error {
		_, err := m.keboolaProjectAPI.DeleteTokenRequest(token.ID).Send(ctx)
		return err
	})

	export.Token = model.Token{ExportKey: export.ExportKey, StorageToken: *token}
	return nil
}

func (m *Manager) RefreshTokens(ctx context.Context, rb rollback.Builder, tokens []model.Token) error {
	rb = rb.AddParallel()
	errs := errors.NewMultiError()
	wg := &sync.WaitGroup{}
	for i := range tokens {
		token := &tokens[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := m.RefreshToken(ctx, rb, token); err != nil {
				errs.Append(err)
			}
		}()
	}
	wg.Wait()
	return errs.ErrorOrNil()
}

func (m *Manager) RefreshToken(ctx context.Context, rb rollback.Builder, token *model.Token) error {
	// Try refresh token
	newToken, err := m.keboolaProjectAPI.RefreshTokenRequest(token.ID).Send(ctx)

	// Create a new token, if it doesn't exist
	var apiErr *keboola.StorageError
	if errors.As(err, &apiErr) && apiErr.ErrCode == "storage.tokens.notFound" {
		newToken, err = m.createTokenRequest(token.ExportKey, token.BucketPermissions).Send(ctx)
		if err == nil {
			rb.Add(func(ctx context.Context) error {
				_, err := m.keboolaProjectAPI.DeleteTokenRequest(newToken.ID).Send(ctx)
				return err
			})
		}
	}

	// Unexpected error
	if err != nil {
		return err
	}

	token.StorageToken = *newToken
	return nil
}

func (m *Manager) createTokenRequest(exportKey key.ExportKey, permissions keboola.BucketPermissions) request.APIRequest[*keboola.Token] {
	return m.keboolaProjectAPI.
		CreateTokenRequest(
			keboola.WithDescription(
				// Max length of description is 255 characters,
				// this will be at most receiverId (48) + exportId (48) + extra chars (40) = 136 characters.
				fmt.Sprintf("[_internal] Buffer Export %s for Receiver %s", exportKey.ReceiverID, exportKey.ExportID),
			),
			keboola.WithBucketPermissions(permissions),
			keboola.WithCanReadAllFileUploads(true),
		)
}
