#!/bin/bash

# Trading Platform Launcher

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}Starting Trading Platform Engine...${NC}"

# Ensure we are in the script directory
cd "$(dirname "$0")"

# Create output directory
mkdir -p target/classes

# Compile
echo -e "${GREEN}Compiling source code...${NC}"
find src -name "*.java" > sources.txt

# Build classpath dynamically from Maven dependencies
if [ ! -d "target/lib" ]; then
    echo "Downloading dependencies..."
    mvn dependency:copy-dependencies -DoutputDirectory=target/lib -q
fi

CLASSPATH="target/classes"
for jar in target/lib/*.jar; do
    CLASSPATH="$CLASSPATH:$jar"
done

# Attempt compilation
javac -cp "$CLASSPATH" -d target/classes --release 17 @sources.txt

if [ $? -ne 0 ]; then
    echo -e "${RED}Compilation failed!${NC}"
    exit 1
fi

# Copy resources
cp src/logback.xml target/classes/
cp -r src/config target/classes/ 2>/dev/null || true
cp -r src/main/resources/public target/classes/ 2>/dev/null || true

# Run
echo -e "${GREEN}Launching Web Dashboard...${NC}"
echo -e "${BLUE}Open http://localhost:8080 in your browser${NC}"
java -cp "$CLASSPATH" Main
