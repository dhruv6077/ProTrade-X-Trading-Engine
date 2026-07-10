#!/bin/bash

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}================================================${NC}"
echo -e "${BLUE}   Trading Platform - Dependency Installer (macOS)    ${NC}"
echo -e "${BLUE}================================================${NC}"

# Check for Homebrew
if ! command -v brew &> /dev/null; then
    echo -e "${RED}Homebrew is not installed.${NC}"
    echo "Please install Homebrew first: https://brew.sh/"
    echo "Run: /bin/bash -c \"\$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)\""
    exit 1
fi

echo -e "${GREEN}Checking dependencies...${NC}"

# Check Java
if ! command -v java &> /dev/null; then
    echo "Java not found. Installing OpenJDK 17..."
    brew install openjdk@17
    
    # Link Java
    sudo ln -sfn /usr/local/opt/openjdk@17/libexec/openjdk.jdk /Library/Java/JavaVirtualMachines/openjdk-17.jdk
else
    echo -e "✅ Java is installed: $(java -version 2>&1 | head -n 1)"
fi

# Check Maven
if ! command -v mvn &> /dev/null; then
    echo "Maven not found. Installing Maven..."
    brew install maven
else
    echo -e "✅ Maven is installed: $(mvn -version | head -n 1)"
fi

echo -e "${GREEN}All dependencies are ready!${NC}"
echo "You can now run './run_pro.sh' to start the platform."
