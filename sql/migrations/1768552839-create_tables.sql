--create:users
--replace table_name with your table
--write your create query
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,
    address TEXT,
    phone TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT now()
);


--drop:users
--replace table_name with your table
--write your drop query
DROP TABLE IF EXISTS users;