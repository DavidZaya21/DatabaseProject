#!/bin/bash

# Cassandra Installation Script for macOS
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if we're on macOS
check_macos() {
    if [[ "$(uname -s)" != "Darwin" ]]; then
        print_error "This script is for macOS only"
        exit 1
    fi
    print_status "Detected macOS $(sw_vers -productVersion)"
}

# Install Homebrew if not present
install_homebrew() {
    if ! command -v brew &> /dev/null; then
        print_status "Installing Homebrew..."
        /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
        
        # Add Homebrew to PATH for Apple Silicon Macs
        if [[ -f "/opt/homebrew/bin/brew" ]]; then
            echo 'eval "$(/opt/homebrew/bin/brew shellenv)"' >> ~/.zshrc
            eval "$(/opt/homebrew/bin/brew shellenv)"
        fi
    else
        print_status "Homebrew already installed"
        brew update
    fi
}

# Install Java
install_java() {
    print_status "Installing Java..."
    
    # Check if Java is already installed
    if java -version &>/dev/null; then
        print_status "Java already installed"
        java -version
        return
    fi
    
    # Install OpenJDK 8
    brew install openjdk@8
    
    # Create symlink for system Java (may require sudo)
    print_warning "You may be prompted for your password to create Java symlink"
    sudo ln -sfn $(brew --prefix)/opt/openjdk@8/libexec/openjdk.jdk /Library/Java/JavaVirtualMachines/openjdk-8.jdk 2>/dev/null || true
    
    # Add to PATH
    echo 'export PATH="$(brew --prefix)/opt/openjdk@8/bin:$PATH"' >> ~/.zshrc
    export PATH="$(brew --prefix)/opt/openjdk@8/bin:$PATH"
    
    print_status "Java installed successfully"
}

# Install Cassandra
install_cassandra() {
    print_status "Installing Cassandra..."
    
    # Install Cassandra
    brew install cassandra
    
    print_status "Cassandra installed successfully"
}

# Configure and start Cassandra
start_cassandra() {
    print_status "Starting Cassandra..."
    
    # Start Cassandra service
    brew services start cassandra
    
    print_status "Cassandra service started"
    print_status "Waiting for Cassandra to initialize (this may take 30-60 seconds)..."
    
    # Wait for Cassandra to be ready
    local max_attempts=30
    local attempt=1
    
    while [ $attempt -le $max_attempts ]; do
        if brew services list | grep cassandra | grep -q started; then
            sleep 2
            if timeout 5s cqlsh -e "DESCRIBE KEYSPACES;" &>/dev/null; then
                print_status "✅ Cassandra is ready!"
                break
            fi
        fi
        
        echo -n "."
        sleep 2
        attempt=$((attempt + 1))
    done
    
    if [ $attempt -gt $max_attempts ]; then
        print_warning "Cassandra may still be starting up. Try connecting in a few minutes."
    fi
}

# Install Python driver (optional)
install_python_driver() {
    print_status "Installing Python Cassandra driver..."
    
    if command -v pip3 &> /dev/null; then
        pip3 install cassandra-driver
        print_status "Python driver installed"
    elif command -v pip &> /dev/null; then
        pip install cassandra-driver
        print_status "Python driver installed"
    else
        print_warning "pip not found. Install Python first if you need the Python driver"
    fi
}

# Show connection info
show_info() {
    print_status "🎉 Installation completed!"
    echo
    print_status "Quick start commands:"
    echo "  Connect to Cassandra: cqlsh"
    echo "  Start service:        brew services start cassandra"
    echo "  Stop service:         brew services stop cassandra"
    echo "  Restart service:      brew services restart cassandra"
    echo "  Service status:       brew services list | grep cassandra"
    echo
    print_status "Configuration files:"
    echo "  Config: $(brew --prefix)/etc/cassandra/cassandra.yaml"
    echo "  Logs:   $(brew --prefix)/var/log/cassandra/"
    echo
    print_status "Test your installation:"
    echo "  cqlsh"
    echo "  CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"
}

# Main installation
main() {
    print_status "🚀 Starting Cassandra installation on macOS..."
    
    check_macos
    install_homebrew
    install_java
    install_cassandra
    start_cassandra
    
    # Ask about Python driver
    echo
    read -p "Do you want to install Python Cassandra driver? (y/n): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        install_python_driver
    fi
    
    show_info
}

# Run installation
main "$@"