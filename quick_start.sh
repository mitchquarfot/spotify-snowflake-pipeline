#!/bin/bash

# Spotify Analytics Pipeline - Enhanced Quick Start Script
# Complete setup for personal Spotify analytics with genre enhancement and medallion architecture

set -e  # Exit on any error

echo "🎵 Spotify Analytics Pipeline Setup"
echo "=============================================="
echo "Features: Genre Enhancement • Medallion Architecture • Natural Language Queries"
echo ""

# Check Python version
echo "📋 Checking prerequisites..."
python_version=$(python3 --version 2>&1 | cut -d' ' -f2 | cut -d'.' -f1-2)
required_version="3.8"

if [ "$(printf '%s\n' "$required_version" "$python_version" | sort -V | head -n1)" != "$required_version" ]; then
    echo "❌ Python 3.8+ required. Found: $python_version"
    echo "   Please install Python 3.8 or higher and try again."
    exit 1
fi
echo "✅ Python version: $python_version"

# Check for required tools
if ! command -v git &> /dev/null; then
    echo "❌ Git is required but not installed."
    exit 1
fi
echo "✅ Git available"

# Check if virtual environment should be created
if [ ! -d "venv" ]; then
    read -p "📦 Create virtual environment? (recommended) [Y/n]: " create_venv
    if [ "$create_venv" != "n" ] && [ "$create_venv" != "N" ]; then
        echo "Creating virtual environment..."
        python3 -m venv venv
        echo "✅ Virtual environment created"
        echo ""
    fi
fi

# Activate virtual environment
if [ -d "venv" ]; then
    if [ -z "$VIRTUAL_ENV" ]; then
        echo "🔧 Activating virtual environment..."
        source venv/bin/activate
        echo "✅ Virtual environment activated"
    else
        echo "✅ Virtual environment already active"
    fi
fi

# Install dependencies
echo ""
echo "📦 Installing dependencies..."
pip install --upgrade pip
pip install -r requirements.txt
echo "✅ Dependencies installed"

# Run setup if .env doesn't exist
echo ""
if [ ! -f ".env" ]; then
    echo "🔧 Running interactive setup..."
    echo "You'll need:"
    echo "  • Spotify Developer App credentials"
    echo "  • AWS account with S3 access"
    echo "  • (Optional) Snowflake account"
    echo ""
    python3 setup.py
else
    echo "✅ Configuration found (.env exists)"
    echo "   To reconfigure, delete .env and run this script again"
fi

# Test the setup
echo ""
echo "🔍 Testing configuration..."
if python3 main.py test; then
    echo "✅ All tests passed!"
else
    echo "❌ Setup incomplete. Please check your configuration."
    echo "   Common issues:"
    echo "   • Incorrect Spotify credentials"
    echo "   • AWS permissions not set"
    echo "   • Network connectivity issues"
    exit 1
fi

# Success message with next steps
echo ""
echo "🎉 Setup complete! Your Spotify analytics pipeline is ready."
echo ""
echo "==============================================="
echo "🚀 NEXT STEPS:"
echo "==============================================="
echo ""
echo "1. 📊 COLLECT YOUR FIRST DATA:"
echo "   python3 main.py run-once --enable-artist-genre-processing"
echo ""
echo "2. 📈 CHECK YOUR STATS:"
echo "   python3 main.py stats --enable-artist-genre-processing"
echo ""
echo "3. 🔄 BACKFILL HISTORICAL DATA (last 7 days):"
echo "   python3 main.py backfill --days 7 --enable-artist-genre-processing"
echo ""
echo "4. ⚡ CONTINUOUS COLLECTION:"
echo "   python3 main.py run-continuous --enable-artist-genre-processing"
echo ""
echo "==============================================="
echo "🏗️  ADVANCED SETUP:"
echo "==============================================="
echo ""
echo "5. 🏔️  SET UP SNOWFLAKE (Data Warehouse):"
echo "   • Run SQL scripts in order:"
echo "   • snowflake_setup.sql           (tables & ingestion)"
echo "   • medallion_architecture_views.sql (bronze/silver/gold)"
echo "   • fix_medallion_simple_approach.sql (enhanced analytics)"
echo ""
echo "6. 🤖 ENABLE NATURAL LANGUAGE QUERIES:"
echo "   • Upload: spotify_semantic_model.yml to Snowflake"
echo "   • Use: SNOWFLAKE.CORTEX.ANALYST() function"
echo ""
echo "7. 🔄 AUTOMATE WITH GITHUB ACTIONS:"
echo "   • Fork this repo on GitHub"
echo "   • Add your credentials as secrets"
echo "   • Daily collection runs automatically!"
echo ""
echo "==============================================="
echo "📚 HELPFUL RESOURCES:"
echo "==============================================="
echo ""
echo "📖 Full Documentation:     QUICK_START.md"
echo "🎯 Example Queries:        See QUICK_START.md"
echo "🐛 Troubleshooting:        Check logs in logs/"
echo "💬 Get Help:               GitHub Issues"
echo ""
echo "==============================================="
echo "🎵 SAMPLE COMMANDS TO TRY:"
echo "==============================================="
echo ""
echo "# Process specific artists for missing genres:"
echo "python3 main.py process-artists --artists \"artist_id_1,artist_id_2\""
echo ""
echo "# View pipeline statistics:"
echo "python3 main.py stats --enable-artist-genre-processing"
echo ""
echo "# Test individual components:"
echo "python3 main.py test"
echo ""
echo "==============================================="

# Check if user wants to run first collection
echo ""
read -p "🎵 Run your first data collection now? [Y/n]: " run_first
if [ "$run_first" != "n" ] && [ "$run_first" != "N" ]; then
    echo ""
    echo "🚀 Running first data collection..."
    echo "   This will:"
    echo "   • Authenticate with Spotify (browser will open)"
    echo "   • Collect your recent listening history"
    echo "   • Enhance artist genre data"
    echo "   • Upload to S3"
    echo ""
    
    if python3 main.py run-once --enable-artist-genre-processing; then
        echo ""
        echo "✅ First collection successful!"
        echo ""
        echo "📊 View your stats:"
        python3 main.py stats --enable-artist-genre-processing
    else
        echo ""
        echo "❌ First collection failed. Check the logs for details."
        echo "   You can try again later with:"
        echo "   python3 main.py run-once --enable-artist-genre-processing"
    fi
fi

echo ""
echo "🎉 Welcome to your personal Spotify analytics pipeline!"
echo "   Your musical journey is now being tracked and analyzed."
echo ""
echo "Happy listening! 🎵📊"
echo ""

# Final reminders
echo "💡 REMINDERS:"
echo "   • Keep your .env file secure (never commit to git)"
echo "   • Check GitHub for updates and new features"
echo "   • Set up Snowflake for advanced analytics"
echo "   • Enable GitHub Actions for automated collection"
echo ""