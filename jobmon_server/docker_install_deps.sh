#!/bin/bash
set -e

echo "Starting dependency installation script..."
echo "Received EXTRAS_ARG: '${EXTRAS_ARG}'"
echo "Received EDITABLE_ARG: '${EDITABLE_ARG}'"
echo "Valid core extras: '${VALID_CORE_EXTRAS}'"
echo "Valid server extras: '${VALID_SERVER_EXTRAS}'"

APP_HOME_DIR=${APP_HOME:-/app} # Use APP_HOME from Docker ENV or default to /app

CORE_EXTRAS_STRING=""
SERVER_EXTRAS_STRING=""
INSTALL_PACKAGES=""

# --- jobmon_core ---
TEMP_CORE_EXTRAS=""
# Ensure EXTRAS_ARG is not empty before attempting to process it
if [ -n "$EXTRAS_ARG" ]; then
    for EXTRA_ITEM in $(echo "${EXTRAS_ARG}" | tr "," " "); do
        # Check if EXTRA_ITEM is in VALID_CORE_EXTRAS
        if echo "$VALID_CORE_EXTRAS" | grep -qw "$EXTRA_ITEM"; then
            TEMP_CORE_EXTRAS="${TEMP_CORE_EXTRAS},${EXTRA_ITEM}"
        fi
    done
fi
if [ -n "$TEMP_CORE_EXTRAS" ]; then CORE_EXTRAS_STRING="[${TEMP_CORE_EXTRAS#,}]"; fi
echo "jobmon_core extras to install: ${CORE_EXTRAS_STRING}"

CORE_INSTALL_SPEC="${APP_HOME_DIR}/jobmon_core${CORE_EXTRAS_STRING}"
if [ "$EDITABLE_ARG" = "true" ]; then CORE_INSTALL_SPEC="-e ${CORE_INSTALL_SPEC}"; fi
INSTALL_PACKAGES="${CORE_INSTALL_SPEC}"

# --- jobmon_server ---
TEMP_SERVER_EXTRAS=""
# Ensure EXTRAS_ARG is not empty
if [ -n "$EXTRAS_ARG" ]; then
    for EXTRA_ITEM in $(echo "${EXTRAS_ARG}" | tr "," " "); do
        # Check if EXTRA_ITEM is in VALID_SERVER_EXTRAS
        if echo "$VALID_SERVER_EXTRAS" | grep -qw "$EXTRA_ITEM"; then
            TEMP_SERVER_EXTRAS="${TEMP_SERVER_EXTRAS},${EXTRA_ITEM}"
        fi
    done
fi
if [ -n "$TEMP_SERVER_EXTRAS" ]; then SERVER_EXTRAS_STRING="[${TEMP_SERVER_EXTRAS#,}]"; fi
echo "jobmon_server extras to install: ${SERVER_EXTRAS_STRING}"

SERVER_INSTALL_SPEC="${APP_HOME_DIR}/jobmon_server${SERVER_EXTRAS_STRING}"
if [ "$EDITABLE_ARG" = "true" ]; then SERVER_INSTALL_SPEC="-e ${SERVER_INSTALL_SPEC}"; fi
INSTALL_PACKAGES="${INSTALL_PACKAGES} ${SERVER_INSTALL_SPEC}"

echo "Installing packages with uv: ${INSTALL_PACKAGES}"
uv pip install --system --no-cache-dir ${INSTALL_PACKAGES}

echo "Dependency installation script finished." 