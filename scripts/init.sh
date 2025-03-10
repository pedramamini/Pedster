#!/bin/bash
# Pedster initialization script

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

# Function to print errors
print_error() {
    echo -e "${RED}✗ $1${NC}"
}

# Check if we're in the right directory
if [ ! -d "pedster" ] || [ ! -d "tests" ]; then
    print_error "Please run this script from the root of the Pedster project"
    exit 1
fi

# Parse command line arguments
skip_venv=false
skip_config=false

for arg in "$@"; do
    case $arg in
        --skip-venv)
            skip_venv=true
            shift
            ;;
        --skip-config)
            skip_config=true
            shift
            ;;
    esac
done

# Create and activate virtual environment
if [ "$skip_venv" = false ]; then
    print_step "Setting up virtual environment"
    
    if [ -d "venv" ]; then
        print_warning "Virtual environment already exists. Using existing environment."
    else
        python3 -m venv venv
        print_success "Created virtual environment"
    fi
    
    # Determine the correct activation script based on OS
    if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "win32" ]]; then
        source venv/Scripts/activate
    else
        source venv/bin/activate
    fi
    
    print_success "Activated virtual environment"
fi

# Install the package in development mode
print_step "Installing Pedster package"
pip install -e ".[dev]"
print_success "Installed Pedster in development mode"

# Set up configuration
if [ "$skip_config" = false ]; then
    print_step "Setting up configuration"
    
    # Create config directory if it doesn't exist
    config_dir="$HOME/.config/pedster"
    mkdir -p "$config_dir"
    
    # Check if config file already exists
    config_file="$config_dir/config.toml"
    if [ -f "$config_file" ]; then
        print_warning "Configuration file already exists at $config_file"
    else
        # Copy example config
        cp config.toml.example "$config_file"
        print_success "Created configuration file at $config_file"
        print_warning "Please edit $config_file with your own settings"
    fi
    
    # Export environment variables
    print_success "Loading environment variables from config"
    eval $(python scripts/setup_env.py)
fi

# Run a basic test to verify the installation
print_step "Verifying installation"
python scripts/run_tests.py --pattern test_models.py

# Print success message
print_step "Initialization complete"
echo -e "To start working with Pedster, run:"
echo -e "  ${GREEN}source venv/bin/activate${NC}  # If not already activated"
echo -e "  ${GREEN}eval \$(python scripts/setup_env.py)${NC}  # Load environment variables"
echo -e "  ${GREEN}dagster dev${NC}  # Start Dagster UI"
echo -e "\nTo run tests:"
echo -e "  ${GREEN}python scripts/run_tests.py${NC}  # Run all tests"
echo -e "  ${GREEN}python scripts/run_tests.py --coverage${NC}  # Run tests with coverage"

# Return to the user with success
exit 0