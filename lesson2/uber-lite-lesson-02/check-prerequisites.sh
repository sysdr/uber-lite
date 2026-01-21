#!/bin/bash
echo "🔍 Checking Prerequisites for Uber-Lite Lesson 02"
echo ""

# Check Java
if command -v java &> /dev/null; then
    JAVA_VERSION=$(java -version 2>&1 | head -n 1)
    echo "✅ Java found: $JAVA_VERSION"
    java -version 2>&1 | head -1
else
    echo "❌ Java not found"
    echo "   Install with: sudo apt install -y openjdk-21-jdk"
fi

echo ""

# Check Maven
if command -v mvn &> /dev/null; then
    MVN_VERSION=$(mvn -version | head -n 1)
    echo "✅ Maven found: $MVN_VERSION"
    mvn -version | head -1
else
    echo "❌ Maven not found"
    echo "   Install with: sudo apt install -y maven"
fi

echo ""

# Check Docker
if command -v docker &> /dev/null; then
    echo "✅ Docker found"
    docker --version | head -1
else
    echo "❌ Docker not found"
fi

echo ""

# Check Docker Compose
if command -v docker-compose &> /dev/null || docker compose version &> /dev/null; then
    echo "✅ Docker Compose found"
    docker-compose --version 2>/dev/null || docker compose version
else
    echo "❌ Docker Compose not found"
fi

echo ""

# Check if services are running
if docker ps | grep -q "kafka\|zookeeper"; then
    echo "✅ Docker services (Kafka/Zookeeper) are running"
    docker ps | grep -E "kafka|zookeeper"
else
    echo "⚠️  Docker services not running"
    echo "   Start with: cd uber-lite-lesson-02 && docker-compose up -d"
fi

echo ""

