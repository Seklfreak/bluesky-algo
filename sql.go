package main

var migrations = `
CREATE TABLE IF NOT EXISTS "posts" (
    uri VARCHAR PRIMARY KEY,
    cid VARCHAR,
    replyParent VARCHAR,
	replyRoot VARCHAR,
	indexedAt TIMESTAMP WITH TIME ZONE,
	text VARCHAR,
	createdAt TIMESTAMP WITH TIME ZONE
)
;

CREATE INDEX IF NOT EXISTS "posts_created_at" ON "posts" (createdAt)
;
`
