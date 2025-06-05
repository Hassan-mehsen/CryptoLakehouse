-- ============================================================================
-- Script : init_roles.sql
-- Purpose: Create DWH roles and assign privileges (without passwords)
-- Usage  : Execute once to initialize PostgreSQL roles and permissions
-- Note   : Passwords are set separately via 'set_passwords.sh'
-- ============================================================================

-- 1. Create roles (without passwords)
CREATE ROLE admin NOINHERIT CREATEDB CREATEROLE LOGIN;
CREATE ROLE data_engineer NOINHERIT LOGIN;
CREATE ROLE qa_data NOINHERIT LOGIN;
CREATE ROLE data_scientist NOINHERIT LOGIN;
CREATE ROLE analyst NOINHERIT LOGIN;
CREATE ROLE bi_user NOINHERIT LOGIN;


-- 2. Create work schemas
CREATE SCHEMA IF NOT EXISTS sandbox;
CREATE SCHEMA IF NOT EXISTS sandbox_ds;
CREATE SCHEMA IF NOT EXISTS sandbox_qa;

-- 3. Permissions on public schema
GRANT USAGE ON SCHEMA public TO data_engineer, qa_data, data_scientist, analyst, bi_user;
-- Only admin can create objects in public
GRANT CREATE ON SCHEMA public TO admin;

-- 4. Rights on existing tables in public
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO admin;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO data_engineer;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO qa_data, data_scientist, analyst, bi_user;

-- 5. Rights on sandbox schemas
GRANT USAGE, CREATE ON SCHEMA sandbox_ds TO data_scientist;
GRANT USAGE, CREATE ON SCHEMA sandbox TO analyst;
GRANT USAGE, CREATE ON SCHEMA sandbox_qa TO qa_data;

-- 6. Temporary table rights for session-based work
-- Allows roles to create TEMP tables (session-scoped, auto-deleted) without altering schemas
GRANT TEMP ON DATABASE crypto_dw TO data_engineer, qa_data, data_scientist, analyst;

-- 7. Future default privileges (when new tables are created)
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO admin;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO data_engineer;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO qa_data, data_scientist, analyst, bi_user;

-- 8. Security cleanup: revoke default CREATE on public from everyone
REVOKE CREATE ON SCHEMA public FROM public;
