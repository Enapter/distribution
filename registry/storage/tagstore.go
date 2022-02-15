package storage

import (
	"context"
	"path"
	"sync"

	"github.com/docker/distribution"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/opencontainers/go-digest"
)

var _ distribution.TagService = &tagStore{}

// tagStore provides methods to manage manifest tags in a backend storage driver.
// This implementation uses the same on-disk layout as the (now deleted) tag
// store.  This provides backward compatibility with current registry deployments
// which only makes use of the Digest field of the returned distribution.Descriptor
// but does not enable full roundtripping of Descriptor objects
type tagStore struct {
	repository *repository
	blobStore  *blobStore
}

// All returns all tags
func (ts *tagStore) All(ctx context.Context) ([]string, error) {
	var tags []string

	pathSpec, err := pathFor(manifestTagPathSpec{
		name: ts.repository.Named().Name(),
	})
	if err != nil {
		return tags, err
	}

	entries, err := ts.blobStore.driver.List(ctx, pathSpec)
	if err != nil {
		switch err := err.(type) {
		case storagedriver.PathNotFoundError:
			return tags, distribution.ErrRepositoryUnknown{Name: ts.repository.Named().Name()}
		default:
			return tags, err
		}
	}

	for _, entry := range entries {
		_, filename := path.Split(entry)
		tags = append(tags, filename)
	}

	return tags, nil
}

// Tag tags the digest with the given tag, updating the the store to point at
// the current tag. The digest must point to a manifest.
func (ts *tagStore) Tag(ctx context.Context, tag string, desc distribution.Descriptor) error {
	currentPath, err := pathFor(manifestTagCurrentPathSpec{
		name: ts.repository.Named().Name(),
		tag:  tag,
	})

	if err != nil {
		return err
	}

	lbs := ts.linkedBlobStore(ctx, tag)

	// Link into the index
	if err := lbs.linkBlob(ctx, desc); err != nil {
		return err
	}

	// Overwrite the current link
	return ts.blobStore.link(ctx, currentPath, desc.Digest)
}

// resolve the current revision for name and tag.
func (ts *tagStore) Get(ctx context.Context, tag string) (distribution.Descriptor, error) {
	currentPath, err := pathFor(manifestTagCurrentPathSpec{
		name: ts.repository.Named().Name(),
		tag:  tag,
	})

	if err != nil {
		return distribution.Descriptor{}, err
	}

	revision, err := ts.blobStore.readlink(ctx, currentPath)
	if err != nil {
		switch err.(type) {
		case storagedriver.PathNotFoundError:
			return distribution.Descriptor{}, distribution.ErrTagUnknown{Tag: tag}
		}

		return distribution.Descriptor{}, err
	}

	return distribution.Descriptor{Digest: revision}, nil
}

// Untag removes the tag association
func (ts *tagStore) Untag(ctx context.Context, tag string) error {
	tagPath, err := pathFor(manifestTagPathSpec{
		name: ts.repository.Named().Name(),
		tag:  tag,
	})
	if err != nil {
		return err
	}

	if err := ts.blobStore.driver.Delete(ctx, tagPath); err != nil {
		switch err.(type) {
		case storagedriver.PathNotFoundError:
			return nil // Untag is idempotent, we don't care if it didn't exist
		default:
			return err
		}
	}

	return nil
}

// linkedBlobStore returns the linkedBlobStore for the named tag, allowing one
// to index manifest blobs by tag name. While the tag store doesn't map
// precisely to the linked blob store, using this ensures the links are
// managed via the same code path.
func (ts *tagStore) linkedBlobStore(ctx context.Context, tag string) *linkedBlobStore {
	return &linkedBlobStore{
		blobStore:  ts.blobStore,
		repository: ts.repository,
		ctx:        ctx,
		linkPathFns: []linkPathFunc{func(name string, dgst digest.Digest) (string, error) {
			return pathFor(manifestTagIndexEntryLinkPathSpec{
				name:     name,
				tag:      tag,
				revision: dgst,
			})

		}},
	}
}

// Lookup recovers a list of tags which refer to this digest.  When a manifest is deleted by
// digest, tag entries which point to it need to be recovered to avoid dangling tags.
func (ts *tagStore) Lookup(ctx context.Context, desc distribution.Descriptor) ([]string, error) {
	allTags, err := ts.All(ctx)
	switch err.(type) {
	case distribution.ErrRepositoryUnknown:
		// This tag store has been initialized but not yet populated
		break
	case nil:
		break
	default:
		return nil, err
	}

	limiter := make(chan struct{}, 10)
	errChan := make(chan error, len(allTags))
	tagChan := make(chan string, len(allTags))
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var tags []string
	wg := sync.WaitGroup{}
	wg.Add(len(allTags))
	for _, tag := range allTags {
		limiter <- struct{}{}
		go func(tag string) {
			defer wg.Done()
			defer func() {
				<-limiter
			}()
			tagLinkPathSpec := manifestTagCurrentPathSpec{
				name: ts.repository.Named().Name(),
				tag:  tag,
			}

			tagLinkPath, _ := pathFor(tagLinkPathSpec)
			// If context is done stop short as the parent has exited with an error
			if ctx.Err() != nil {
				return
			}
			tagDigest, err := ts.blobStore.readlink(ctx, tagLinkPath)
			if err != nil {
				switch err.(type) {
				case storagedriver.PathNotFoundError:
					return
				}
				select {
				case <-ctx.Done():
					return
				default:
				}
				errChan <- err
				return
			}

			if tagDigest == desc.Digest {

				select {
				case <-ctx.Done():
					return
				default:
				}
				tagChan <- tag
				return
			}
		}(tag)
	}

	go func() {
		wg.Wait()
		close(tagChan)

	}()
read:
	for {
		select {
		case err, workRemaining := <-errChan:
			if !workRemaining {
				break read
			}
			cancel()
			return nil, err
		case tag, workRemaining := <-tagChan:
			if !workRemaining {
				close(errChan)
				break read
			}
			tags = append(tags, tag)
		}

	}

	return tags, nil
}
