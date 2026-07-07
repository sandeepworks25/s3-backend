package meta

const schemaSQL = `
create table if not exists buckets (
	name text primary key,
	policy text not null default 'private',
	created_at timestamptz not null default now()
);

alter table buckets add column if not exists policy text not null default 'private';

create table if not exists objects (
	bucket text not null references buckets(name) on delete restrict,
	key text not null,
	size_bytes bigint not null,
	etag text not null,
	content_type text not null default 'application/octet-stream',
	storage_backend text not null,
	physical_path text not null,
	metadata jsonb not null default '{}'::jsonb,
	created_at timestamptz not null default now(),
	updated_at timestamptz not null default now(),
	primary key(bucket, key)
);

create index if not exists objects_bucket_key_idx on objects(bucket, key);

create table if not exists multipart_uploads (
	id text primary key,
	bucket text not null references buckets(name) on delete cascade,
	key text not null,
	content_type text not null default 'application/octet-stream',
	created_at timestamptz not null default now()
);

create table if not exists multipart_parts (
	upload_id text not null references multipart_uploads(id) on delete cascade,
	part_number integer not null,
	etag text not null,
	size_bytes bigint not null,
	path text not null,
	created_at timestamptz not null default now(),
	primary key(upload_id, part_number)
);

create table if not exists streams (
	id text primary key,
	name text not null,
	bucket text not null references buckets(name) on delete restrict,
	status text not null,
	created_at timestamptz not null default now()
);

create table if not exists stream_segments (
	stream_id text not null references streams(id) on delete cascade,
	rendition text not null,
	sequence_number bigint not null,
	duration_ms bigint not null default 0,
	object_key text not null,
	size_bytes bigint not null,
	etag text not null,
	status text not null,
	created_at timestamptz not null default now(),
	primary key(stream_id, rendition, sequence_number)
);

create table if not exists usage_events (
	id text primary key default md5(random()::text || clock_timestamp()::text),
	event_type text not null,
	bucket text,
	object_key text,
	bytes bigint not null default 0,
	created_at timestamptz not null default now()
);

create table if not exists audit_logs (
	id text primary key default md5(random()::text || clock_timestamp()::text),
	actor text not null,
	action text not null,
	resource text not null,
	result text not null,
	created_at timestamptz not null default now()
);

create table if not exists admin_users (
	id text not null default md5(random()::text || clock_timestamp()::text),
	name text not null default '',
	email text primary key,
	password_hash text not null,
	role text not null default 'root',
	status text not null default 'Active',
	last_seen timestamptz,
	created_at timestamptz not null default now(),
	updated_at timestamptz not null default now()
);

alter table admin_users add column if not exists id text not null default md5(random()::text || clock_timestamp()::text);
alter table admin_users add column if not exists name text not null default '';
alter table admin_users add column if not exists status text not null default 'Active';
alter table admin_users add column if not exists last_seen timestamptz;

create table if not exists access_keys (
	id text primary key,
	label text not null,
	access_key_id text not null unique,
	secret_key text not null,
	owner_email text not null references admin_users(email) on delete cascade,
	permissions jsonb not null default '[]'::jsonb,
	status text not null default 'Active',
	last_used timestamptz,
	created_at timestamptz not null default now(),
	updated_at timestamptz not null default now()
);

create table if not exists policies (
	id text primary key,
	name text not null unique,
	description text not null default '',
	effect text not null default 'Allow',
	actions jsonb not null default '[]'::jsonb,
	resource text not null,
	document jsonb not null default '{}'::jsonb,
	created_at timestamptz not null default now(),
	updated_at timestamptz not null default now()
);

create table if not exists system_settings (
	id text primary key,
	value jsonb not null default '{}'::jsonb,
	updated_at timestamptz not null default now()
);

alter table objects drop constraint if exists objects_bucket_fkey;
alter table objects add constraint objects_bucket_fkey foreign key(bucket) references buckets(name) on delete cascade;

alter table streams drop constraint if exists streams_bucket_fkey;
alter table streams add constraint streams_bucket_fkey foreign key(bucket) references buckets(name) on delete cascade;
`
