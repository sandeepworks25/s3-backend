package meta

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"s3store/backend/internal/types"
)

type PostgresRepository struct {
	pool *pgxpool.Pool
}

func OpenPostgres(ctx context.Context, databaseURL string, logger *slog.Logger) (Repository, error) {
	if databaseURL == "" {
		return nil, errors.New("DATABASE_URL is empty")
	}
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	repo := &PostgresRepository{pool: pool}
	if err := repo.migrate(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	logger.Info("postgres metadata repository ready")
	return repo, nil
}

func (r *PostgresRepository) Close() { r.pool.Close() }

func (r *PostgresRepository) Health(ctx context.Context) error { return r.pool.Ping(ctx) }

func (r *PostgresRepository) migrate(ctx context.Context) error {
	_, err := r.pool.Exec(ctx, schemaSQL)
	return err
}

func (r *PostgresRepository) CreateBucket(ctx context.Context, name, policy string) (types.Bucket, error) {
	var b types.Bucket
	if policy == "" {
		policy = "private"
	}
	err := r.pool.QueryRow(ctx, `insert into buckets(name, policy) values($1, $2) returning name, policy, created_at`, name, policy).Scan(&b.Name, &b.Policy, &b.CreatedAt)
	if pgErr := (*pgconn.PgError)(nil); errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return types.Bucket{}, ErrBucketExists
	}
	return b, err
}

func (r *PostgresRepository) ListBuckets(ctx context.Context) ([]types.Bucket, error) {
	rows, err := r.pool.Query(ctx, `select name, policy, created_at from buckets order by name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []types.Bucket
	for rows.Next() {
		var b types.Bucket
		if err := rows.Scan(&b.Name, &b.Policy, &b.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

func (r *PostgresRepository) GetBucket(ctx context.Context, name string) (types.Bucket, error) {
	var b types.Bucket
	err := r.pool.QueryRow(ctx, `select name, policy, created_at from buckets where name=$1`, name).Scan(&b.Name, &b.Policy, &b.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return types.Bucket{}, ErrBucketNotFound
	}
	return b, err
}

func (r *PostgresRepository) UpdateBucket(ctx context.Context, oldName, newName, policy string) (types.Bucket, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return types.Bucket{}, err
	}
	defer tx.Rollback(ctx)
	if policy == "" {
		policy = "private"
	}
	if oldName == newName {
		var b types.Bucket
		err := tx.QueryRow(ctx, `update buckets set policy=$2 where name=$1 returning name, policy, created_at`, oldName, policy).Scan(&b.Name, &b.Policy, &b.CreatedAt)
		if errors.Is(err, pgx.ErrNoRows) {
			return types.Bucket{}, ErrBucketNotFound
		}
		if err != nil {
			return types.Bucket{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return types.Bucket{}, err
		}
		return b, nil
	}

	var b types.Bucket
	err = tx.QueryRow(ctx, `insert into buckets(name, policy) values($1, $2) returning name, policy, created_at`, newName, policy).Scan(&b.Name, &b.Policy, &b.CreatedAt)
	if pgErr := (*pgconn.PgError)(nil); errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return types.Bucket{}, ErrBucketExists
	}
	if err != nil {
		return types.Bucket{}, err
	}
	tag, err := tx.Exec(ctx, `update objects set bucket=$1, physical_path=replace(physical_path, $2, $1), updated_at=now() where bucket=$2`, newName, oldName)
	if err != nil {
		return types.Bucket{}, err
	}
	_ = tag
	if _, err := tx.Exec(ctx, `update multipart_uploads set bucket=$1 where bucket=$2`, newName, oldName); err != nil {
		return types.Bucket{}, err
	}
	if _, err := tx.Exec(ctx, `update streams set bucket=$1 where bucket=$2`, newName, oldName); err != nil {
		return types.Bucket{}, err
	}
	tag, err = tx.Exec(ctx, `delete from buckets where name=$1`, oldName)
	if err != nil {
		return types.Bucket{}, err
	}
	if tag.RowsAffected() == 0 {
		return types.Bucket{}, ErrBucketNotFound
	}
	if err := tx.Commit(ctx); err != nil {
		return types.Bucket{}, err
	}
	return b, nil
}

func (r *PostgresRepository) DeleteBucket(ctx context.Context, name string) error {
	var objectCount int
	if err := r.pool.QueryRow(ctx, `select count(*) from objects where bucket=$1`, name).Scan(&objectCount); err != nil {
		return err
	}
	if objectCount > 0 {
		return ErrBucketNotEmpty
	}
	tag, err := r.pool.Exec(ctx, `delete from buckets where name=$1`, name)
	if pgErr := (*pgconn.PgError)(nil); errors.As(err, &pgErr) && pgErr.Code == "23503" {
		return ErrBucketNotEmpty
	}
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrBucketNotFound
	}
	return nil
}

func (r *PostgresRepository) DeleteBucketRecursive(ctx context.Context, name string) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `delete from objects where bucket=$1`, name); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `delete from multipart_uploads where bucket=$1`, name); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `delete from streams where bucket=$1`, name); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `delete from buckets where name=$1`, name)
	if pgErr := (*pgconn.PgError)(nil); errors.As(err, &pgErr) && pgErr.Code == "23503" {
		if _, cleanupErr := tx.Exec(ctx, `delete from objects where bucket in (select name from buckets where name=$1)`, name); cleanupErr != nil {
			return cleanupErr
		}
		tag, err = tx.Exec(ctx, `delete from buckets where name=$1`, name)
	}
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrBucketNotFound
	}
	return tx.Commit(ctx)
}

func (r *PostgresRepository) UpsertObject(ctx context.Context, obj types.Object) error {
	metaJSON, _ := json.Marshal(obj.Metadata)
	_, err := r.pool.Exec(ctx, `
		insert into objects(bucket, key, size_bytes, etag, content_type, storage_backend, physical_path, metadata, created_at, updated_at)
		values($1,$2,$3,$4,$5,$6,$7,$8,now(),now())
		on conflict(bucket, key) do update set
			size_bytes=excluded.size_bytes,
			etag=excluded.etag,
			content_type=excluded.content_type,
			storage_backend=excluded.storage_backend,
			physical_path=excluded.physical_path,
			metadata=excluded.metadata,
			updated_at=now()
	`, obj.Bucket, obj.Key, obj.Size, obj.ETag, obj.ContentType, obj.StorageBackend, obj.PhysicalPath, metaJSON)
	if pgErr := (*pgconn.PgError)(nil); errors.As(err, &pgErr) && pgErr.Code == "23503" {
		return ErrBucketNotFound
	}
	return err
}

func (r *PostgresRepository) GetObject(ctx context.Context, bucket, key string) (types.Object, error) {
	obj, err := scanObject(r.pool.QueryRow(ctx, `
		select bucket, key, size_bytes, etag, content_type, storage_backend, physical_path, metadata, created_at, updated_at
		from objects where bucket=$1 and key=$2
	`, bucket, key))
	if errors.Is(err, pgx.ErrNoRows) {
		return types.Object{}, ErrObjectNotFound
	}
	return obj, err
}

func (r *PostgresRepository) DeleteObject(ctx context.Context, bucket, key string) error {
	tag, err := r.pool.Exec(ctx, `delete from objects where bucket=$1 and key=$2`, bucket, key)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrObjectNotFound
	}
	return nil
}

func (r *PostgresRepository) ListObjects(ctx context.Context, bucket, prefix string, limit int, cursor string) ([]types.Object, string, error) {
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}
	rows, err := r.pool.Query(ctx, `
		select bucket, key, size_bytes, etag, content_type, storage_backend, physical_path, metadata, created_at, updated_at
		from objects
		where bucket=$1 and key like $2 and key > $3
		order by key
		limit $4
	`, bucket, prefix+"%", cursor, limit+1)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	var out []types.Object
	for rows.Next() {
		obj, err := scanObject(rows)
		if err != nil {
			return nil, "", err
		}
		out = append(out, obj)
	}
	if len(out) <= limit {
		return out, "", rows.Err()
	}
	next := out[limit-1].Key
	return out[:limit], next, rows.Err()
}

func (r *PostgresRepository) CreateMultipartUpload(ctx context.Context, upload types.MultipartUpload) error {
	_, err := r.pool.Exec(ctx, `
		insert into multipart_uploads(id, bucket, key, content_type, created_at)
		values($1,$2,$3,$4,$5)
	`, upload.ID, upload.Bucket, upload.Key, upload.ContentType, upload.CreatedAt)
	return err
}

func (r *PostgresRepository) GetMultipartUpload(ctx context.Context, uploadID string) (types.MultipartUpload, error) {
	var u types.MultipartUpload
	err := r.pool.QueryRow(ctx, `
		select id, bucket, key, content_type, created_at from multipart_uploads where id=$1
	`, uploadID).Scan(&u.ID, &u.Bucket, &u.Key, &u.ContentType, &u.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return types.MultipartUpload{}, ErrUploadNotFound
	}
	return u, err
}

func (r *PostgresRepository) SaveMultipartPart(ctx context.Context, part types.MultipartPart) error {
	_, err := r.pool.Exec(ctx, `
		insert into multipart_parts(upload_id, part_number, etag, size_bytes, path, created_at)
		values($1,$2,$3,$4,$5,$6)
		on conflict(upload_id, part_number) do update set etag=excluded.etag, size_bytes=excluded.size_bytes, path=excluded.path
	`, part.UploadID, part.PartNumber, part.ETag, part.Size, part.Path, part.CreatedAt)
	return err
}

func (r *PostgresRepository) ListMultipartParts(ctx context.Context, uploadID string) ([]types.MultipartPart, error) {
	rows, err := r.pool.Query(ctx, `
		select upload_id, part_number, etag, size_bytes, path, created_at
		from multipart_parts where upload_id=$1 order by part_number
	`, uploadID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []types.MultipartPart
	for rows.Next() {
		var p types.MultipartPart
		if err := rows.Scan(&p.UploadID, &p.PartNumber, &p.ETag, &p.Size, &p.Path, &p.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func (r *PostgresRepository) DeleteMultipartUpload(ctx context.Context, uploadID string) error {
	_, err := r.pool.Exec(ctx, `delete from multipart_uploads where id=$1`, uploadID)
	return err
}

func (r *PostgresRepository) CreateStream(ctx context.Context, stream types.Stream) error {
	_, err := r.pool.Exec(ctx, `insert into streams(id, name, bucket, status, created_at) values($1,$2,$3,$4,$5)`, stream.ID, stream.Name, stream.Bucket, stream.Status, stream.CreatedAt)
	return err
}

func (r *PostgresRepository) ListStreams(ctx context.Context) ([]types.Stream, error) {
	rows, err := r.pool.Query(ctx, `select id, name, bucket, status, created_at from streams order by created_at desc`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []types.Stream
	for rows.Next() {
		var s types.Stream
		if err := rows.Scan(&s.ID, &s.Name, &s.Bucket, &s.Status, &s.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

func (r *PostgresRepository) GetStream(ctx context.Context, id string) (types.Stream, error) {
	var s types.Stream
	err := r.pool.QueryRow(ctx, `select id, name, bucket, status, created_at from streams where id=$1`, id).Scan(&s.ID, &s.Name, &s.Bucket, &s.Status, &s.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return types.Stream{}, ErrStreamNotFound
	}
	return s, err
}

func (r *PostgresRepository) SaveStreamSegment(ctx context.Context, segment types.StreamSegment) error {
	_, err := r.pool.Exec(ctx, `
		insert into stream_segments(stream_id, rendition, sequence_number, duration_ms, object_key, size_bytes, etag, status, created_at)
		values($1,$2,$3,$4,$5,$6,$7,$8,$9)
		on conflict(stream_id, rendition, sequence_number) do update set
			duration_ms=excluded.duration_ms, object_key=excluded.object_key, size_bytes=excluded.size_bytes, etag=excluded.etag, status=excluded.status
	`, segment.StreamID, segment.Rendition, segment.SequenceNumber, segment.DurationMS, segment.ObjectKey, segment.Size, segment.ETag, segment.Status, segment.CreatedAt)
	return err
}

func (r *PostgresRepository) ListStreamSegments(ctx context.Context, streamID string) ([]types.StreamSegment, error) {
	rows, err := r.pool.Query(ctx, `
		select stream_id, rendition, sequence_number, duration_ms, object_key, size_bytes, etag, status, created_at
		from stream_segments where stream_id=$1 order by rendition, sequence_number
	`, streamID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []types.StreamSegment
	for rows.Next() {
		var s types.StreamSegment
		if err := rows.Scan(&s.StreamID, &s.Rendition, &s.SequenceNumber, &s.DurationMS, &s.ObjectKey, &s.Size, &s.ETag, &s.Status, &s.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Rendition == out[j].Rendition {
			return out[i].SequenceNumber < out[j].SequenceNumber
		}
		return out[i].Rendition < out[j].Rendition
	})
	return out, rows.Err()
}

func (r *PostgresRepository) AddAudit(ctx context.Context, actor, action, resource, result string) error {
	_, err := r.pool.Exec(ctx, `insert into audit_logs(actor, action, resource, result) values($1,$2,$3,$4)`, actor, action, resource, result)
	return err
}

func (r *PostgresRepository) ListAudit(ctx context.Context, limit int) ([]AuditLog, error) {
	if limit <= 0 || limit > 200 {
		limit = 100
	}
	rows, err := r.pool.Query(ctx, `select id::text, actor, action, resource, result, created_at::text from audit_logs order by created_at desc limit $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []AuditLog
	for rows.Next() {
		var a AuditLog
		if err := rows.Scan(&a.ID, &a.Actor, &a.Action, &a.Resource, &a.Result, &a.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

func (r *PostgresRepository) UsageSummary(ctx context.Context) (UsageSummary, error) {
	var u UsageSummary
	err := r.pool.QueryRow(ctx, `
		select
			(select count(*) from buckets),
			(select count(*) from objects),
			coalesce((select sum(size_bytes) from objects), 0),
			(select count(*) from streams where lower(status) = 'active')
	`).Scan(&u.BucketCount, &u.ObjectCount, &u.TotalBytes, &u.StreamCount)
	return u, err
}

func (r *PostgresRepository) UpsertAdminUser(ctx context.Context, user AdminUser) error {
	if user.Role == "" {
		user.Role = "root"
	}
	if user.Status == "" {
		user.Status = "Active"
	}
	if user.Name == "" {
		user.Name = user.Email
	}
	if user.ID == "" {
		user.ID = randomID()
	}
	_, err := r.pool.Exec(ctx, `
		insert into admin_users(id, name, email, password_hash, role, status, created_at, updated_at)
		values($1,$2,$3,$4,$5,$6,now(),now())
		on conflict(email) do update set
			name=excluded.name,
			password_hash=excluded.password_hash,
			role=excluded.role,
			status=excluded.status,
			updated_at=now()
	`, user.ID, user.Name, user.Email, user.PasswordHash, user.Role, user.Status)
	return err
}

func (r *PostgresRepository) GetAdminUserByEmail(ctx context.Context, email string) (AdminUser, error) {
	var user AdminUser
	err := r.pool.QueryRow(ctx, `
		select id, name, email, password_hash, role, status, coalesce(last_seen::text, ''), created_at::text, updated_at::text
		from admin_users where email=$1
	`, email).Scan(&user.ID, &user.Name, &user.Email, &user.PasswordHash, &user.Role, &user.Status, &user.LastSeen, &user.CreatedAt, &user.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return AdminUser{}, ErrAdminNotFound
	}
	return user, err
}

func (r *PostgresRepository) ListAdminUsers(ctx context.Context) ([]AdminUser, error) {
	rows, err := r.pool.Query(ctx, `
		select id, name, email, password_hash, role, status, coalesce(last_seen::text, ''), created_at::text, updated_at::text
		from admin_users
		order by created_at desc
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var users []AdminUser
	for rows.Next() {
		var user AdminUser
		if err := rows.Scan(&user.ID, &user.Name, &user.Email, &user.PasswordHash, &user.Role, &user.Status, &user.LastSeen, &user.CreatedAt, &user.UpdatedAt); err != nil {
			return nil, err
		}
		users = append(users, user)
	}
	return users, rows.Err()
}

func (r *PostgresRepository) DeleteAdminUser(ctx context.Context, email string) error {
	tag, err := r.pool.Exec(ctx, `delete from admin_users where email=$1`, email)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrAdminNotFound
	}
	return nil
}

func (r *PostgresRepository) UpsertAccessKey(ctx context.Context, key AccessKey) error {
	if key.ID == "" {
		key.ID = randomID()
	}
	if key.Status == "" {
		key.Status = "Active"
	}
	permissions, err := json.Marshal(key.Permissions)
	if err != nil {
		return err
	}
	_, err = r.pool.Exec(ctx, `
		insert into access_keys(id, label, access_key_id, secret_key, owner_email, permissions, status, created_at, updated_at)
		values($1,$2,$3,$4,$5,$6,$7,now(),now())
		on conflict(id) do update set
			label=excluded.label,
			access_key_id=excluded.access_key_id,
			secret_key=excluded.secret_key,
			owner_email=excluded.owner_email,
			permissions=excluded.permissions,
			status=excluded.status,
			updated_at=now()
	`, key.ID, key.Label, key.AccessKeyID, key.SecretKey, key.OwnerEmail, string(permissions), key.Status)
	return err
}

func (r *PostgresRepository) ListAccessKeys(ctx context.Context) ([]AccessKey, error) {
	rows, err := r.pool.Query(ctx, `
		select id, label, access_key_id, secret_key, owner_email, permissions, status, coalesce(last_used::text, ''), created_at::text, updated_at::text
		from access_keys
		order by created_at desc
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var keys []AccessKey
	for rows.Next() {
		key, err := scanAccessKey(rows)
		if err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	return keys, rows.Err()
}

func (r *PostgresRepository) GetAccessKey(ctx context.Context, id string) (AccessKey, error) {
	key, err := scanAccessKey(r.pool.QueryRow(ctx, `
		select id, label, access_key_id, secret_key, owner_email, permissions, status, coalesce(last_used::text, ''), created_at::text, updated_at::text
		from access_keys where id=$1
	`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return AccessKey{}, ErrAccessKeyNotFound
	}
	return key, err
}

func (r *PostgresRepository) GetAccessKeyByAccessKeyID(ctx context.Context, accessKeyID string) (AccessKey, error) {
	key, err := scanAccessKey(r.pool.QueryRow(ctx, `
		select id, label, access_key_id, secret_key, owner_email, permissions, status, coalesce(last_used::text, ''), created_at::text, updated_at::text
		from access_keys where access_key_id=$1
	`, accessKeyID))
	if errors.Is(err, pgx.ErrNoRows) {
		return AccessKey{}, ErrAccessKeyNotFound
	}
	return key, err
}

func (r *PostgresRepository) DeleteAccessKey(ctx context.Context, id string) error {
	tag, err := r.pool.Exec(ctx, `delete from access_keys where id=$1`, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrAccessKeyNotFound
	}
	return nil
}

func (r *PostgresRepository) TouchAccessKey(ctx context.Context, accessKeyID string) error {
	_, err := r.pool.Exec(ctx, `update access_keys set last_used=now(), updated_at=now() where access_key_id=$1`, accessKeyID)
	return err
}

func (r *PostgresRepository) CreatePolicy(ctx context.Context, policy Policy) (Policy, error) {
	if policy.ID == "" {
		policy.ID = randomID()
	}
	actions, err := json.Marshal(policy.Actions)
	if err != nil {
		return Policy{}, err
	}
	document, err := json.Marshal(policy.Document)
	if err != nil {
		return Policy{}, err
	}
	err = r.pool.QueryRow(ctx, `
		insert into policies(id, name, description, effect, actions, resource, document, created_at, updated_at)
		values($1,$2,$3,$4,$5,$6,$7,now(),now())
		returning id, name, description, effect, actions, resource, document, created_at::text, updated_at::text
	`, policy.ID, policy.Name, policy.Description, policy.Effect, string(actions), policy.Resource, string(document)).Scan(
		&policy.ID, &policy.Name, &policy.Description, &policy.Effect, &actions, &policy.Resource, &document, &policy.CreatedAt, &policy.UpdatedAt,
	)
	if err != nil {
		return Policy{}, err
	}
	_ = json.Unmarshal(actions, &policy.Actions)
	_ = json.Unmarshal(document, &policy.Document)
	return policy, nil
}

func (r *PostgresRepository) ListPolicies(ctx context.Context) ([]Policy, error) {
	rows, err := r.pool.Query(ctx, `
		select id, name, description, effect, actions, resource, document, created_at::text, updated_at::text
		from policies
		order by created_at desc
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	policies := []Policy{}
	for rows.Next() {
		policy, err := scanPolicy(rows)
		if err != nil {
			return nil, err
		}
		policies = append(policies, policy)
	}
	return policies, rows.Err()
}

func (r *PostgresRepository) GetPolicy(ctx context.Context, id string) (Policy, error) {
	policy, err := scanPolicy(r.pool.QueryRow(ctx, `
		select id, name, description, effect, actions, resource, document, created_at::text, updated_at::text
		from policies where id=$1
	`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Policy{}, ErrObjectNotFound
	}
	return policy, err
}

func (r *PostgresRepository) UpdatePolicy(ctx context.Context, policy Policy) (Policy, error) {
	actions, err := json.Marshal(policy.Actions)
	if err != nil {
		return Policy{}, err
	}
	document, err := json.Marshal(policy.Document)
	if err != nil {
		return Policy{}, err
	}
	err = r.pool.QueryRow(ctx, `
		update policies
		set name=$2, description=$3, effect=$4, actions=$5, resource=$6, document=$7, updated_at=now()
		where id=$1
		returning id, name, description, effect, actions, resource, document, created_at::text, updated_at::text
	`, policy.ID, policy.Name, policy.Description, policy.Effect, string(actions), policy.Resource, string(document)).Scan(
		&policy.ID, &policy.Name, &policy.Description, &policy.Effect, &actions, &policy.Resource, &document, &policy.CreatedAt, &policy.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return Policy{}, ErrObjectNotFound
	}
	if err != nil {
		return Policy{}, err
	}
	_ = json.Unmarshal(actions, &policy.Actions)
	_ = json.Unmarshal(document, &policy.Document)
	return policy, nil
}

func (r *PostgresRepository) DeletePolicy(ctx context.Context, id string) error {
	tag, err := r.pool.Exec(ctx, `delete from policies where id=$1`, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrObjectNotFound
	}
	return nil
}

func (r *PostgresRepository) GetSystemSettings(ctx context.Context) (SystemSettings, error) {
	var raw []byte
	var updatedAt string
	err := r.pool.QueryRow(ctx, `select value, updated_at::text from system_settings where id='system'`).Scan(&raw, &updatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return defaultSystemSettings(), nil
	}
	if err != nil {
		return SystemSettings{}, err
	}
	settings := defaultSystemSettings()
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &settings)
	}
	settings = normalizeSystemSettings(settings)
	settings.UpdatedAt = updatedAt
	return settings, nil
}

func (r *PostgresRepository) SaveSystemSettings(ctx context.Context, settings SystemSettings) (SystemSettings, error) {
	settings = normalizeSystemSettings(settings)
	raw, err := json.Marshal(settings)
	if err != nil {
		return SystemSettings{}, err
	}
	err = r.pool.QueryRow(ctx, `
		insert into system_settings(id, value, updated_at)
		values('system', $1, now())
		on conflict(id) do update set value=excluded.value, updated_at=now()
		returning updated_at::text
	`, string(raw)).Scan(&settings.UpdatedAt)
	return settings, err
}

type objectScanner interface {
	Scan(dest ...any) error
}

type accessKeyScanner interface {
	Scan(dest ...any) error
}

type policyScanner interface {
	Scan(dest ...any) error
}

func scanAccessKey(row accessKeyScanner) (AccessKey, error) {
	var key AccessKey
	var permissions []byte
	err := row.Scan(&key.ID, &key.Label, &key.AccessKeyID, &key.SecretKey, &key.OwnerEmail, &permissions, &key.Status, &key.LastUsed, &key.CreatedAt, &key.UpdatedAt)
	if err != nil {
		return AccessKey{}, err
	}
	if len(permissions) > 0 {
		_ = json.Unmarshal(permissions, &key.Permissions)
	}
	return key, nil
}

func scanPolicy(row policyScanner) (Policy, error) {
	var policy Policy
	var actions []byte
	var document []byte
	err := row.Scan(&policy.ID, &policy.Name, &policy.Description, &policy.Effect, &actions, &policy.Resource, &document, &policy.CreatedAt, &policy.UpdatedAt)
	if err != nil {
		return Policy{}, err
	}
	if len(actions) > 0 {
		_ = json.Unmarshal(actions, &policy.Actions)
	}
	if len(document) > 0 {
		_ = json.Unmarshal(document, &policy.Document)
	}
	return policy, nil
}

func scanObject(row objectScanner) (types.Object, error) {
	var obj types.Object
	var metadata []byte
	err := row.Scan(&obj.Bucket, &obj.Key, &obj.Size, &obj.ETag, &obj.ContentType, &obj.StorageBackend, &obj.PhysicalPath, &metadata, &obj.CreatedAt, &obj.UpdatedAt)
	if err != nil {
		return types.Object{}, err
	}
	if len(metadata) > 0 {
		_ = json.Unmarshal(metadata, &obj.Metadata)
	}
	return obj, nil
}

var _ = time.UTC
