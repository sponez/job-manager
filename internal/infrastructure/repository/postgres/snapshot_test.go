package postgres

import (
	"context"
	"testing"
	"uuid"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/sponez/job-manager/internal/application/snapshot"
)

func TestSnapshotRepositorySaveAndCompensate(t *testing.T) {
	db := repositoryTestPool(t)
	ctx := context.Background()
	id := uuid.New()
	dbID := pgtype.UUID{Bytes: [16]byte(id), Valid: true}
	_, err := db.Exec(ctx, `INSERT INTO workflows (id, type, definition_version, status)
		VALUES ($1, 'fetch_page', 1, 'pending')`, dbID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = db.Exec(context.Background(), `DELETE FROM workflows WHERE id = $1`, dbID) })

	repository := NewSnapshotRepository(db)
	if exists, err := repository.Exists(ctx, id); err != nil || exists {
		t.Fatalf("snapshot exists before save = %v, %v", exists, err)
	}
	value := snapshot.Snapshot{SourceURL: "https://example.com/page", ContentType: "text/html", Body: []byte("first")}
	if err := repository.Save(ctx, id, value); err != nil {
		t.Fatal(err)
	}
	if exists, err := repository.Exists(ctx, id); err != nil || !exists {
		t.Fatalf("snapshot exists after save = %v, %v", exists, err)
	}
	value.Body = []byte("second")
	if err := repository.Save(ctx, id, value); err != nil {
		t.Fatal(err)
	}
	var body []byte
	if err := db.QueryRow(ctx, `SELECT body FROM snapshots WHERE workflow_id = $1`, dbID).Scan(&body); err != nil {
		t.Fatal(err)
	}
	if string(body) != "first" {
		t.Fatalf("body = %q; retry should preserve first save", body)
	}
	for range 2 {
		if err := repository.Delete(ctx, id); err != nil {
			t.Fatal(err)
		}
	}
	if exists, err := repository.Exists(ctx, id); err != nil || exists {
		t.Fatalf("snapshot exists after delete = %v, %v", exists, err)
	}
	var count int
	if err := db.QueryRow(ctx, `SELECT count(*) FROM snapshots WHERE workflow_id = $1`, dbID).Scan(&count); err != nil || count != 0 {
		t.Fatalf("remaining snapshots = %d, error = %v", count, err)
	}
}
