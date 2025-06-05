#!/bin/bash

# ============================================================================
# Script : init_dwh.sh
# Purpose: Initialize PostgreSQL roles and permissions for the DWH project
# Author : Hassan Mehsen
# Usage  : ./init_dwh.sh (must be run from project root or inside admin/)
# Notes  :
#   - Requires the database to exist
#   - Executes both the SQL role creation and password setup
#   - Assumes .env exists at ../../.env relative to this script
# ============================================================================

# Stop on first error
set -e 

SCRIPT_DIR="$(dirname "$0")"
ENV_FILE="$SCRIPT_DIR/../../.env"

# === 1. Load .env securely
if [ ! -f "$ENV_FILE" ]; then
    echo "ERROR: .env file not found at $ENV_FILE. source: $SCRIPT_DIR/init_dwh.sh"
    exit 1
fi

echo "Loading environment variables... source: $SCRIPT_DIR/init_dwh.sh"
set -a
source "$ENV_FILE"
set +a

# === 2. Run SQL to create roles and assign privileges
echo "Creating roles and granting permissions... source: $SCRIPT_DIR/init_roles.sql"
PGPASSWORD="$DATABASE_PASSWORD" psql -U "$DATABASE_USER" -d "$DB_NAME" -f "$SCRIPT_DIR/../scripts/dcl/init_roles.sql"

# === 3. Run the password assignment script
echo "Setting passwords... source: $SCRIPT_DIR/set_passwords.sh"
bash "$SCRIPT_DIR/set_passwords.sh"

# === 4. Done
echo "PostgreSQL DWH roles successfully initialized. source: $SCRIPT_DIR/init_dwh.sh"
