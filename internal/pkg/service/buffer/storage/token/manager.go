package token

import (
	"context"
	"fmt"
	"sync"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/storageapi"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/rollback"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Manager struct {
	client client.Sender
}

type dependencies interface {
	StorageAPIClient() client.Sender
}

func NewManager(d dependencies) *Manager {
	return &Manager{client: d.StorageAPIClient()}
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
	permissions := storageapi.BucketPermissions{export.Mapping.TableID.BucketID: storageapi.BucketPermissionWrite}
	token, err := m.createTokenRequest(export.ExportKey, permissions).Send(ctx, m.client)
	if err != nil {
		return err
	}

	rb.Add(func(ctx context.Context) error {
		_, err := storageapi.DeleteTokenRequest(token.ID).Send(ctx, m.client)
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
	newToken, err := storageapi.RefreshTokenRequest(token.ID).Send(ctx, m.client)

	// Create a new token, if it doesn't exist
	var apiErr *storageapi.Error
	if errors.As(err, &apiErr) && apiErr.ErrCode == "storage.tokens.notFound" {
		newToken, err = m.createTokenRequest(token.ExportKey, token.BucketPermissions).Send(ctx, m.client)
		if err == nil {
			rb.Add(func(ctx context.Context) error {
				_, err := storageapi.DeleteTokenRequest(newToken.ID).Send(ctx, m.client)
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

func (m *Manager) createTokenRequest(exportKey key.ExportKey, permissions storageapi.BucketPermissions) client.APIRequest[*storageapi.Token] {
	return storageapi.
		CreateTokenRequest(
			storageapi.WithDescription(
				// Max length of description is 255 characters,
				// this will be at most receiverId (48) + exportId (48) + extra chars (40) = 136 characters.
				fmt.Sprintf("[_internal] Buffer Export %s for Receiver %s", exportKey.ReceiverID, exportKey.ExportID),
			),
			storageapi.WithBucketPermissions(permissions),
		)
}
