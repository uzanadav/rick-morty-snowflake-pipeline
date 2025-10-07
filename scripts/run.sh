#!/bin/bash

###############################################################################
# Rick and Morty Data Pipeline - Execution Script
# 
# This script runs the data pipeline.
# If setup hasn't been done, it will run setup.sh first.
###############################################################################

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

echo ""
echo "=========================================="
echo "  Rick and Morty Data Pipeline"
echo "=========================================="
echo ""

# Check if virtual environment exists and is set up
if [ ! -d "venv" ] || [ ! -f "venv/.requirements_installed" ]; then
    echo -e "${YELLOW}⚠ Setup not complete${NC}"
    echo -e "${BLUE}Running setup script...${NC}"
    echo ""
    bash "$SCRIPT_DIR/setup.sh"
    echo ""
    echo -e "${GREEN}✓ Setup complete, continuing with pipeline execution...${NC}"
    echo ""
fi

# Activate virtual environment
echo -e "${BLUE}Activating virtual environment...${NC}"
source venv/bin/activate
echo -e "${GREEN}✓ Virtual environment activated${NC}"

# Run the pipeline
echo ""
echo -e "${BLUE}Starting pipeline execution...${NC}"
echo ""

python main.py "$@"

EXIT_CODE=$?

echo ""
if [ $EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}=========================================="
    echo -e "  ✓ Pipeline completed successfully!"
    echo -e "==========================================${NC}"
    echo ""
    echo -e "${BLUE}Raw data saved to:${NC} ./data/raw/"
    echo ""
else
    echo -e "${RED}=========================================="
    echo -e "  ✗ Pipeline failed with exit code $EXIT_CODE"
    echo -e "==========================================${NC}"
    echo ""
fi

exit $EXIT_CODE