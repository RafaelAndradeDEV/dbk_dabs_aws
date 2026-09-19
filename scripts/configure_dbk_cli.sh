#!/bin/bash

# Script to configure Databricks CLI
# Uses OAuth if client credentials are available, otherwise uses token authentication

set -e # Exit on any error

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}Starting Databricks configuration...${NC}"

# Check if DATABRICKS_TOKEN is set (required in both cases)
if [[ -z "$DATABRICKS_TOKEN" ]]; then
    echo -e "${RED}Error: DATABRICKS_TOKEN environment variable is not set${NC}"
    exit 1
fi

# Check if both client credentials are set
if [[ -n "$DATABRICKS_CLIENT_SECRET" && -n "$DATABRICKS_CLIENT_ID" ]]; then
    echo -e "${GREEN}✓ Client credentials found - configuring with OAuth and removing token from config${NC}"
    USE_OAUTH=true
else
    echo -e "${YELLOW}Client credentials not found - using token authentication${NC}"
    USE_OAUTH=false
fi

# Run databricks configure with token
echo -e "${YELLOW}Configuring Databricks CLI for profile $DATABRICKS_PROFILE...${NC}"
databricks configure --token "$DATABRICKS_TOKEN" --profile "$DATABRICKS_PROFILE"

# Check if configuration was successful
if [[ $? -eq 0 ]]; then
    echo -e "${GREEN}✓ Databricks configuration completed successfully${NC}"
else
    echo -e "${RED}Error: Failed to configure Databricks CLI${NC}"
    exit 1
fi

# Only remove token line if using OAuth (client credentials are set)
if [[ "$USE_OAUTH" == true ]]; then
    # Check if config file exists
    if [[ ! -f "${DATABRICKS_CONFIG_FILE:=$HOME/.databrickscfg}" ]]; then
        echo -e "${RED}Error: Databricks config file not found at $DATABRICKS_CONFIG_FILE${NC}"
        exit 1
    fi

    echo -e "${YELLOW}Removing token line from config file for OAuth setup...${NC}"

    # Remove the token line from the config file
    sed -i '/^token\s*=/d' "$DATABRICKS_CONFIG_FILE"

    # Verify the token line was removed
    if grep -q "^token\s*=" "$DATABRICKS_CONFIG_FILE"; then
        echo -e "${RED}Warning: Token line may still be present in config file${NC}"
    else
        echo -e "${GREEN}✓ Token line successfully removed from config file${NC}"
    fi

    echo -e "${GREEN}Databricks configuration completed with OAuth authentication!${NC}"
else
    echo -e "${GREEN}Databricks configuration completed with token authentication!${NC}"
fi
