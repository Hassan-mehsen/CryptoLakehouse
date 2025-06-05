#!/bin/bash

# ============================================================================
# Script : set_passwords.sh
# Purpose: Securely assign PostgreSQL role passwords for Crypto DWH project
# Author : Hassan Mehsen
# Usage  : ./set_passwords.sh (requires ../../.env to exist)
# ============================================================================

# Stop on first error
set -e  

SCRIPT_DIR="$(dirname "$0")"
ENV_FILE="$SCRIPT_DIR/../../.env"

# === 1. Load .env securely
ENV_FILE="$(dirname "$0")/../../.env"

if [ ! -f "$ENV_FILE" ]; then
    echo "ERROR: .env file not found at $ENV_FILE. source: $SCRIPT_DIR/set_password.sh"
    exit 1
fi

echo "Loading environment variables... source: $SCRIPT_DIR/set_passwords.sh"

# Export all variables from .env without requiring 'export' in the file
set -a
source "$ENV_FILE"
set +a

# === 2. Export password for PostgreSQL connection
export PGPASSWORD="$DATABASE_PASSWORD"

# === 3. Execute ALTER ROLE commands to set passwords
psql -U "$DATABASE_USER" -h "$DB_HOST" -d "$DB_NAME" -c "ALTER ROLE admin WITH PASSWORD '$ADMIN_PASSWORD';"
psql -U "$DATABASE_USER" -h "$DB_HOST" -d "$DB_NAME" -c "ALTER ROLE data_engineer WITH PASSWORD '$ENGINEER_PASSWORD';"
psql -U "$DATABASE_USER" -h "$DB_HOST" -d "$DB_NAME" -c "ALTER ROLE qa_data WITH PASSWORD '$QA_PASSWORD';"
psql -U "$DATABASE_USER" -h "$DB_HOST" -d "$DB_NAME" -c "ALTER ROLE data_scientist WITH PASSWORD '$SCIENTIST_PASSWORD';"
psql -U "$DATABASE_USER" -h "$DB_HOST" -d "$DB_NAME" -c "ALTER ROLE analyst WITH PASSWORD '$ANALYST_PASSWORD';"
psql -U "$DATABASE_USER" -h "$DB_HOST" -d "$DB_NAME" -c "ALTER ROLE bi_user WITH PASSWORD '$BI_PASSWORD';"


# === 4. Cleanup
unset PGPASSWORD

echo "Passwords successfully assigned to all roles. source: $SCRIPT_DIR/set_passwords.sh"