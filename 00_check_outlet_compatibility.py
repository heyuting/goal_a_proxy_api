#!/usr/bin/env python3
"""
Check if multiple locations share the same outlet COMID.

This script takes coordinates and determines if they all drain to the same outlet.
It's a lightweight version that only does the intersection and lookup without
running the full site selection.

Usage:
    python check_outlet_compatibility.py --coords-file coords.json
    python check_outlet_compatibility.py --lat 37 --lon -78 --lat 36.5 --lon -77.5
"""

import argparse
import gc
import json
import sys
import warnings
from pathlib import Path

import geopandas as gpd
import pandas as pd

# Suppress warnings
warnings.filterwarnings("ignore")


def parse_coordinates(args):
    """Parse coordinates from command-line arguments or JSON file."""
    if args.coords_file:
        with open(args.coords_file, "r") as f:
            data = json.load(f)
            if isinstance(data, list):
                # Format: [{"lat": 37, "lon": -78}, ...]
                coords = [(item["lon"], item["lat"]) for item in data]
            elif isinstance(data, dict) and "coordinates" in data:
                # Format: {"coordinates": [[37, -78], [36.5, -77.5]]}
                coords = [
                    (c[1], c[0]) for c in data["coordinates"]
                ]  # [lat, lon] -> (lon, lat)
            else:
                # Format: {"lats": [37, 36.5], "lons": [-78, -77.5]}
                coords = list(zip(data.get("lons", []), data.get("lats", [])))
    else:
        # Parse from --lat/--lon pairs
        if not args.lat or not args.lon:
            raise ValueError(
                "Either --coords-file or --lat/--lon pairs must be provided"
            )
        if len(args.lat) != len(args.lon):
            raise ValueError("Number of --lat and --lon values must match")
        coords = list(zip(args.lon, args.lat))

    return coords


def check_outlet_compatibility(coordinates, script_dir=None):
    """
    Check if multiple coordinates share the same outlet COMID.

    This function follows the same pattern as 01_site_selection.py:
    - Loads shapefiles once (not per coordinate)
    - Processes all coordinates together in batch
    - Uses the same spatial join approach

    Parameters:
    -----------
    coordinates : list of tuples
        List of (lon, lat) coordinate pairs
    script_dir : Path, optional
        Directory where script is located

    Returns:
    --------
    dict
        Dictionary with 'same_outlet' (bool), 'outlet_comids' (list),
        'unique_outlets' (list), and 'results' (list of individual results)
    """
    if len(coordinates) < 2:
        return {
            "error": "At least 2 coordinates are required for outlet compatibility check"
        }

    if script_dir is None:
        script_dir = Path(__file__).parent
    else:
        script_dir = Path(script_dir)

    # Input directory is one level up from python_version folder
    base_dir = script_dir.parent if script_dir.name == "python_version" else script_dir
    input_shp_dir = base_dir / "input" / "shp"

    try:
        print(f"Processing {len(coordinates)} point(s)...")

        # Create point GeoDataFrame (same as 01_site_selection.py)
        points_df = pd.DataFrame(coordinates, columns=["x", "y"])
        sf_point = gpd.GeoDataFrame(
            points_df,
            geometry=gpd.points_from_xy(points_df["x"], points_df["y"]),
            crs="EPSG:4326",
        )

        # Load watershed shapefile - memory optimized approach
        print("Loading watershed data...")
        sf_ws_region = gpd.read_file(
            input_shp_dir / "sf_basin_us",
            quiet=True,
        )

        # Immediately drop any columns we don't need (keep only COMID and geometry)
        required_cols = ["COMID", "geometry"]
        cols_to_drop = [col for col in sf_ws_region.columns if col not in required_cols]
        if cols_to_drop:
            sf_ws_region = sf_ws_region.drop(columns=cols_to_drop)

        # Ensure watershed region is in same CRS
        if sf_ws_region.crs != sf_point.crs:
            sf_ws_region = sf_ws_region.to_crs(sf_point.crs)

        # Intersect points with watersheds (same approach as 01_site_selection.py)
        print("Finding intersecting watersheds...")
        sf_ws_point = gpd.sjoin(
            sf_ws_region, sf_point, how="inner", predicate="intersects"
        )

        # Get COMIDs before dropping geometry
        s_COMID = sf_ws_point["COMID"].values

        # Drop geometry and free memory from large watershed file
        sf_ws_point = sf_ws_point.drop(columns=["geometry"])
        del sf_ws_region  # Free memory from large watershed shapefile
        gc.collect()  # Force garbage collection

        if len(sf_ws_point) == 0:
            return {
                "error": "No watersheds found for the given coordinates. Check coordinates are within CONUS."
            }

        # Load river region data - only load what we need
        print("Loading river region data...")
        sf_river_region = gpd.read_file(
            input_shp_dir / "sf_river_seg_number",
            quiet=True,
        )

        # Convert to data table - select only COMID and outlet columns
        # This automatically drops geometry when converting GeoDataFrame to DataFrame
        dt_river_region = pd.DataFrame(sf_river_region[["COMID", "outlet"]])
        del sf_river_region  # Free memory
        gc.collect()  # Force garbage collection

        # Get COMIDs for all points
        print(f"Found {len(s_COMID)} matching COMID(s): {s_COMID}")

        # Get outlets for all COMIDs (same approach as 01_site_selection.py)
        # Create dt_ws_selected - get COMID and outlet for selected segments
        dt_ws_selected = dt_river_region[dt_river_region["COMID"].isin(s_COMID)][
            ["COMID", "outlet"]
        ].copy()

        # Create a mapping from COMID to outlet for fast lookup
        comid_to_outlet = dict(zip(dt_ws_selected["COMID"], dt_ws_selected["outlet"]))

        # Match each coordinate to its COMID and outlet
        results = []
        outlet_comids = []

        # Match points to their results using index_right from sjoin
        for idx in range(len(coordinates)):
            lon, lat = coordinates[idx]

            # Find the matching row(s) in sf_ws_point
            # index_right corresponds to the index in sf_point
            point_matches = sf_ws_point[sf_ws_point.index_right == idx]

            if len(point_matches) == 0:
                results.append(
                    {"error": f"No watershed found for coordinate ({lat}, {lon})"}
                )
                continue

            # Take the first match (in case of multiple watersheds)
            comid = point_matches["COMID"].iloc[0]

            if comid not in comid_to_outlet:
                results.append(
                    {
                        "error": f"COMID {comid} not found in river region for coordinate ({lat}, {lon})"
                    }
                )
                continue

            outlet_comid = comid_to_outlet[comid]

            results.append(
                {
                    "outlet_comid": int(outlet_comid),
                    "comid": int(comid),
                    "lat": lat,
                    "lon": lon,
                }
            )
            outlet_comids.append(int(outlet_comid))

        # Check if any errors occurred
        errors = [r for r in results if "error" in r]
        if errors:
            return {
                "error": f"Failed to get outlets for some coordinates: {errors}",
                "results": results,
            }

        # Check if all outlets are the same
        unique_outlets = list(set(outlet_comids))
        same_outlet = len(unique_outlets) == 1

        return {
            "same_outlet": same_outlet,
            "outlet_comids": outlet_comids,
            "unique_outlets": unique_outlets,
            "results": results,
        }

    except Exception as e:
        return {"error": str(e)}


def main():
    parser = argparse.ArgumentParser(
        description="Check if multiple locations share the same outlet COMID",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # From JSON file
  python check_outlet_compatibility.py --coords-file coords.json
  
  # Single point (will return error - need at least 2)
  python check_outlet_compatibility.py --lat 37 --lon -78
  
  # Multiple points
  python check_outlet_compatibility.py --lat 37 --lon -78 --lat 36.5 --lon -77.5
        """,
    )

    parser.add_argument(
        "--coords-file",
        type=str,
        help='JSON file with coordinates: [{"lat": 37, "lon": -78}, ...] or {"coordinates": [[37, -78], ...]}',
    )
    parser.add_argument(
        "--lat", type=float, nargs="+", help="Latitude(s) (can specify multiple)"
    )
    parser.add_argument(
        "--lon", type=float, nargs="+", help="Longitude(s) (can specify multiple)"
    )
    parser.add_argument(
        "--script-dir",
        type=str,
        default=None,
        help="Directory containing script (for relative paths). Default: script location",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output JSON file (optional). If not specified, prints to stdout",
    )

    args = parser.parse_args()

    try:
        coords = parse_coordinates(args)
        script_dir = Path(args.script_dir) if args.script_dir else Path(__file__).parent

        result = check_outlet_compatibility(coords, script_dir=script_dir)

        # Output result
        if args.output:
            with open(args.output, "w") as f:
                json.dump(result, f, indent=2)
            print(f"Result saved to {args.output}")
        else:
            # Print JSON to stdout
            print(json.dumps(result, indent=2))

        # Exit with error code if there was an error
        if "error" in result:
            sys.exit(1)

    except Exception as e:
        error_result = {"error": str(e)}
        if args.output:
            with open(args.output, "w") as f:
                json.dump(error_result, f, indent=2)
        else:
            print(json.dumps(error_result, indent=2))
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
