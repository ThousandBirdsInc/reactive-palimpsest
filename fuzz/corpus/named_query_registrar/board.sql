-- name: BoardCards :many
SELECT id FROM posts WHERE author_id = $1 AND published = true ORDER BY id LIMIT $2;

-- name: ByTitle :one
SELECT id FROM posts WHERE title = sqlc.arg(title) OR id = ANY($1);
