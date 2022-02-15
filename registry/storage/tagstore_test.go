package storage

import (
	"context"
	"testing"

	"errors"
	"github.com/docker/distribution"
	"github.com/docker/distribution/reference"
	"github.com/docker/distribution/registry/storage/driver/inmemory"
	"io"
	"reflect"
	"strconv"
	"testing"

	"github.com/distribution/distribution/v3"
	"github.com/distribution/distribution/v3/manifest"
	"github.com/distribution/distribution/v3/manifest/schema2"
	"github.com/distribution/distribution/v3/reference"
	storagedriver "github.com/distribution/distribution/v3/registry/storage/driver"
	"github.com/distribution/distribution/v3/registry/storage/driver/base"
	"github.com/distribution/distribution/v3/registry/storage/driver/inmemory"
	digest "github.com/opencontainers/go-digest"
)

type tagsTestEnv struct {
	ts         distribution.TagService
	ctx        context.Context
	mockDriver *mockInMemory
}

type mockInMemory struct {
	base.Base
	driver          *inmemory.Driver
	GetContentError error
}

var _ storagedriver.StorageDriver = &mockInMemory{}

func (m *mockInMemory) Name() string {
	return m.driver.Name()
}
func (m *mockInMemory) GetContent(ctx context.Context, path string) ([]byte, error) {
	if m.GetContentError != nil {
		return nil, m.GetContentError
	}
	return m.driver.GetContent(ctx, path)
}
func (m *mockInMemory) PutContent(ctx context.Context, path string, content []byte) error {
	return m.driver.PutContent(ctx, path, content)
}
func (m *mockInMemory) Reader(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {
	return m.driver.Reader(ctx, path, offset)
}
func (m *mockInMemory) Writer(ctx context.Context, path string, append bool) (storagedriver.FileWriter, error) {
	return m.driver.Writer(ctx, path, append)
}
func (m *mockInMemory) List(ctx context.Context, path string) ([]string, error) {
	return m.driver.List(ctx, path)
}
func (m *mockInMemory) Move(ctx context.Context, sourcePath string, destPath string) error {
	return m.driver.Move(ctx, sourcePath, destPath)
}
func (m *mockInMemory) Delete(ctx context.Context, path string) error {
	return m.driver.Delete(ctx, path)
}
func (m *mockInMemory) URLFor(ctx context.Context, path string, options map[string]interface{}) (string, error) {
	return m.driver.URLFor(ctx, path, options)
}
func (m *mockInMemory) Walk(ctx context.Context, path string, f storagedriver.WalkFn) error {
	return m.driver.Walk(ctx, path, f)
}

func testTagStore(t *testing.T) *tagsTestEnv {
	ctx := context.Background()
	d := inmemory.New()
	mockDriver := mockInMemory{driver: d, Base: d.Base}
	reg, err := NewRegistry(ctx, &mockDriver)
	if err != nil {
		t.Fatal(err)
	}

	repoRef, _ := reference.WithName("a/b")
	repo, err := reg.Repository(ctx, repoRef)
	if err != nil {
		t.Fatal(err)
	}

	return &tagsTestEnv{
		ctx:        ctx,
		ts:         repo.Tags(ctx),
		mockDriver: &mockDriver,
	}
}

func TestTagStoreTag(t *testing.T) {
	env := testTagStore(t)
	tags := env.ts
	ctx := env.ctx

	d := distribution.Descriptor{}
	err := tags.Tag(ctx, "latest", d)
	if err == nil {
		t.Errorf("unexpected error putting malformed descriptor : %s", err)
	}

	d.Digest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	err = tags.Tag(ctx, "latest", d)
	if err != nil {
		t.Error(err)
	}

	d1, err := tags.Get(ctx, "latest")
	if err != nil {
		t.Error(err)
	}

	if d1.Digest != d.Digest {
		t.Error("put and get digest differ")
	}

	// Overwrite existing
	d.Digest = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	err = tags.Tag(ctx, "latest", d)
	if err != nil {
		t.Error(err)
	}

	d1, err = tags.Get(ctx, "latest")
	if err != nil {
		t.Error(err)
	}

	if d1.Digest != d.Digest {
		t.Error("put and get digest differ")
	}
}

func TestTagStoreUnTag(t *testing.T) {
	env := testTagStore(t)
	tags := env.ts
	ctx := env.ctx
	desc := distribution.Descriptor{Digest: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}

	err := tags.Untag(ctx, "latest")
	if err != nil {
		t.Error(err)
	}

	err = tags.Tag(ctx, "latest", desc)
	if err != nil {
		t.Error(err)
	}

	err = tags.Untag(ctx, "latest")
	if err != nil {
		t.Error(err)
	}

	errExpect := distribution.ErrTagUnknown{Tag: "latest"}.Error()
	_, err = tags.Get(ctx, "latest")
	if err == nil || err.Error() != errExpect {
		t.Error("Expected error getting untagged tag")
	}
}

func TestTagStoreAll(t *testing.T) {
	env := testTagStore(t)
	tagStore := env.ts
	ctx := env.ctx

	alpha := "abcdefghijklmnopqrstuvwxyz"
	for i := 0; i < len(alpha); i++ {
		tag := alpha[i]
		desc := distribution.Descriptor{Digest: "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"}
		err := tagStore.Tag(ctx, string(tag), desc)
		if err != nil {
			t.Error(err)
		}
	}

	all, err := tagStore.All(ctx)
	if err != nil {
		t.Error(err)
	}
	if len(all) != len(alpha) {
		t.Errorf("Unexpected count returned from enumerate")
	}

	for i, c := range all {
		if c != string(alpha[i]) {
			t.Errorf("unexpected tag in enumerate %s", c)
		}
	}

	removed := "a"
	err = tagStore.Untag(ctx, removed)
	if err != nil {
		t.Error(err)
	}

	all, err = tagStore.All(ctx)
	if err != nil {
		t.Error(err)
	}
	for _, tag := range all {
		if tag == removed {
			t.Errorf("unexpected tag in enumerate %s", removed)
		}
	}

}

func TestTagLookup(t *testing.T) {
	env := testTagStore(t)
	tagStore := env.ts
	ctx := env.ctx

	descA := distribution.Descriptor{Digest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
	desc0 := distribution.Descriptor{Digest: "sha256:0000000000000000000000000000000000000000000000000000000000000000"}

	tags, err := tagStore.Lookup(ctx, descA)
	if err != nil {
		t.Fatal(err)
	}
	if len(tags) != 0 {
		t.Fatalf("Lookup returned > 0 tags from empty store")
	}

	err = tagStore.Tag(ctx, "a", descA)
	if err != nil {
		t.Fatal(err)
	}

	err = tagStore.Tag(ctx, "b", descA)
	if err != nil {
		t.Fatal(err)
	}

	err = tagStore.Tag(ctx, "0", desc0)
	if err != nil {
		t.Fatal(err)
	}

	err = tagStore.Tag(ctx, "1", desc0)
	if err != nil {
		t.Fatal(err)
	}

	tags, err = tagStore.Lookup(ctx, descA)
	if err != nil {
		t.Fatal(err)
	}

	if len(tags) != 2 {
		t.Errorf("Lookup of descA returned %d tags, expected 2", len(tags))
	}

	tags, err = tagStore.Lookup(ctx, desc0)
	if err != nil {
		t.Fatal(err)
	}

	if len(tags) != 2 {
		t.Errorf("Lookup of descB returned %d tags, expected 2", len(tags))
	}

	/// Should handle error looking up tag
	env.mockDriver.GetContentError = errors.New("Lookup failure")

	for i := 2; i < 15; i++ {
		err = tagStore.Tag(ctx, strconv.Itoa(i), desc0)
		if err != nil {
			t.Fatal(err)
		}
	}

	tags, err = tagStore.Lookup(ctx, desc0)
	if err == nil {
		t.Fatal("Expected error but none retrieved")
	}
	if len(tags) > 0 {
		t.Errorf("Expected 0 tags on an error but got %d tags", len(tags))
	}

	// Should not error for a path not found
	env.mockDriver.GetContentError = storagedriver.PathNotFoundError{}

	tags, err = tagStore.Lookup(ctx, desc0)
	if err != nil {
		t.Fatal(err)
	}
	if len(tags) > 0 {
		t.Errorf("Expected 0 tags on path not found but got %d tags", len(tags))
	}
}
