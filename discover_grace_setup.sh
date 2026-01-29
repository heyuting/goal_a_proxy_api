#!/bin/bash
# Helper script to discover correct Python environment and modules on Grace
# Run this on Grace to find the correct paths

echo "=== Discovering Grace Setup ==="
echo ""

echo "1. Available Python modules:"
module avail Python 2>&1 | grep -i python || echo "No Python modules found"

echo ""
echo "2. Available GDAL modules:"
module avail GDAL 2>&1 | grep -i gdal || echo "No GDAL modules found"

echo ""
echo "3. Searching for Python virtual environments:"
DRN_PATH="/home/yhs5/project/DRN/R_code/python_version"
if [ -d "$DRN_PATH" ]; then
    echo "Checking $DRN_PATH:"
    cd "$DRN_PATH"
    
    # Check for venv locations
    for venv_path in ".venv" "venv" "../venv" "../../venv"; do
        if [ -f "$venv_path/bin/activate" ]; then
            echo "  FOUND: $venv_path/bin/activate"
            source "$venv_path/bin/activate"
            echo "  Python: $(which python)"
            echo "  Python version: $(python --version)"
            if python -c "import geopandas" 2>/dev/null; then
                echo "  ✓ geopandas available"
            else
                echo "  ✗ geopandas NOT available"
            fi
            deactivate 2>/dev/null || true
        fi
    done
else
    echo "  ERROR: Directory $DRN_PATH not found"
fi

echo ""
echo "4. Checking system Python:"
if command -v python3 &> /dev/null; then
    echo "  Python3 found: $(which python3)"
    echo "  Python3 version: $(python3 --version)"
    if python3 -c "import geopandas" 2>/dev/null; then
        echo "  ✓ geopandas available in system Python"
    else
        echo "  ✗ geopandas NOT available in system Python"
    fi
else
    echo "  python3 not found"
fi

echo ""
echo "5. Checking if ogr2ogr (GDAL) is available:"
if command -v ogr2ogr &> /dev/null; then
    echo "  ✓ ogr2ogr found: $(which ogr2ogr)"
else
    echo "  ✗ ogr2ogr not found (try loading GDAL module)"
fi

echo ""
echo "=== Discovery Complete ==="

