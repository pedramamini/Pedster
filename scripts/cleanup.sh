#!/bin/bash
# Pedster cleanup script to remove temporary files and caches

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print step headers
print_step() {
    echo -e "\n${BLUE}=== $1 ===${NC}"
}

# Function to print success messages
print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

# Function to print warnings
print_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

# Check if we're in the right directory
if [ ! -d "pedster" ] || [ ! -d "tests" ]; then
    echo -e "${RED}✗ Please run this script from the root of the Pedster project${NC}"
    exit 1
fi

# Parse command line arguments
remove_venv=false
remove_config=false
force=false

for arg in "$@"; do
    case $arg in
        --remove-venv)
            remove_venv=true
            shift
            ;;
        --remove-config)
            remove_config=true
            shift
            ;;
        --force|-f)
            force=true
            shift
            ;;
        --all)
            remove_venv=true
            remove_config=true
            shift
            ;;
    esac
done

# Confirm destructive actions if not forced
if [ "$force" = false ]; then
    if [ "$remove_venv" = true ] || [ "$remove_config" = true ]; then
        echo -e "${YELLOW}Warning: You are about to perform destructive actions:${NC}"
        if [ "$remove_venv" = true ]; then
            echo -e " - Remove virtual environment"
        fi
        if [ "$remove_config" = true ]; then
            echo -e " - Remove configuration file"
        fi
        
        read -p "Are you sure you want to continue? (y/N) " confirm
        if [[ "$confirm" != [yY] && "$confirm" != [yY][eE][sS] ]]; then
            echo -e "${RED}Aborted${NC}"
            exit 1
        fi
    fi
fi

# Remove Python cache files
print_step "Removing Python cache files"
find . -type d -name "__pycache__" -exec rm -rf {} +
find . -type f -name "*.pyc" -delete
find . -type f -name "*.pyo" -delete
find . -type f -name "*.pyd" -delete
find . -type f -name ".coverage" -delete
find . -type d -name "*.egg-info" -exec rm -rf {} +
find . -type d -name "*.egg" -exec rm -rf {} +
find . -type d -name ".pytest_cache" -exec rm -rf {} +
find . -type d -name "htmlcov" -exec rm -rf {} +
find . -type d -name "coverage_html" -exec rm -rf {} +
print_success "Removed Python cache files"

# Remove virtual environment if requested
if [ "$remove_venv" = true ]; then
    print_step "Removing virtual environment"
    if [ -d "venv" ]; then
        rm -rf venv
        print_success "Removed virtual environment"
    else
        print_warning "No virtual environment found"
    fi
fi

# Remove configuration if requested
if [ "$remove_config" = true ]; then
    print_step "Removing configuration"
    config_file="$HOME/.config/pedster/config.toml"
    if [ -f "$config_file" ]; then
        rm "$config_file"
        print_success "Removed configuration file"
    else
        print_warning "No configuration file found"
    fi
fi

# Remove build artifacts
print_step "Removing build artifacts"
rm -rf build/ dist/ .eggs/
print_success "Removed build artifacts"

print_step "Cleanup complete"
echo -e "The project has been cleaned up. To set up again, run ${GREEN}./scripts/init.sh${NC}"

exit 0