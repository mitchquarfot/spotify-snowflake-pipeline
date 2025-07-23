#!/bin/bash

# Spotify to Snowflake Pipeline - Quick Start Script

set -e  # Exit on any error

echo "🎵 Spotify to Snowflake Pipeline Setup"
echo "====================================="

# Check Python version
echo "📋 Checking prerequisites..."
python_version=$(python3 --version 2>&1 | cut -d' ' -f2 | cut -d'.' -f1-2)
required_version="3.8"

if [ "$(printf '%s\n' "$required_version" "$python_version" | sort -V | head -n1)" != "$required_version" ]; then
    echo "❌ Python 3.8+ required. Found: $python_version"
    exit 1
fi
echo "✅ Python version: $python_version"

# Check if virtual environment should be created
if [ ! -d "venv" ]; then
    read -p "📦 Create virtual environment? (recommended) [Y/n]: " create_venv
    if [ "$create_venv" != "n" ] && [ "$create_venv" != "N" ]; then
        echo "Creating virtual environment..."
        python3 -m venv venv
        echo "✅ Virtual environment created"
        echo "🔧 To activate: source venv/bin/activate"
    fi
fi

# Install dependencies
echo "📦 Installing dependencies..."
if [ -d "venv" ] && [ -z "$VIRTUAL_ENV" ]; then
    echo "⚠️  Virtual environment detected but not activated"
    echo "   Run: source venv/bin/activate"
    echo "   Then rerun this script"
    exit 1
fi

pip install -r requirements.txt
echo "✅ Dependencies installed"

# Run setup if .env doesn't exist
if [ ! -f ".env" ]; then
    echo "🔧 Running interactive setup..."
    python3 setup.py
else
    echo "✅ Configuration found (.env exists)"
fi

# Test the setup
echo "🔍 Testing configuration..."
if python3 main.py test; then
    echo "✅ All tests passed!"
else
    echo "❌ Setup incomplete. Please check your configuration."
    exit 1
fi

echo ""
echo "🎉 Setup complete! Next steps:"
echo ""
echo "1. Run once:           python3 main.py run-once"
echo "2. Backfill data:      python3 main.py backfill --days 7"  
echo "3. Run continuously:   python3 main.py run-continuous"
echo "4. Check stats:        python3 main.py stats"
echo ""
echo "📚 For Snowflake setup, see: snowflake_setup.sql"
echo "📖 Full documentation: README.md"
echo ""
echo "Happy listening! 🎵📊" 