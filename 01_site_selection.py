#!/usr/bin/env python3
"""
DRN Step 1: Site Selection (Python version)
Converts R script 01-site_selection.R to Python using geopandas and pandas.

Usage:
    python 01_site_selection.py --coords-file coords.json
    python 01_site_selection.py --lat -78 --lon 37
    python 01_site_selection.py --lat -78 --lon 37 --lat -77.5 --lon 36.5
"""

import argparse
import json
import pickle
import sys
import warnings
from pathlib import Path

import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.colors import LinearSegmentedColormap
import numpy as np

# Suppress warnings (similar to R's suppressMessages)
warnings.filterwarnings("ignore")

# Color scheme matching R: c_s = c("#2166ac", "#4393c3", "#abd9e9", "#fee090", "#fdae61", "#d73027")
C_S = ["#2166ac", "#4393c3", "#abd9e9", "#fee090", "#fdae61", "#d73027"]


def load_lookup_data(filepath):
    """
    Load lookup data file. Tries multiple methods:
    1. Load pickle file (.pkl) - preferred format (smaller, faster)
    2. Try pyreadr to read RDS directly (requires R installed)
    3. Fallback: try loading pickle with .pkl extension

    The lookup data should be a dictionary where keys are COMID strings
    and values are lists of COMIDs.
    """
    filepath = Path(filepath)

    # Try pickle first (preferred format - smaller and faster)
    pickle_path = filepath.with_suffix(".pkl")
    if pickle_path.exists():
        try:
            with open(pickle_path, "rb") as f:
                data = pickle.load(f)
                # Normalize data structure: ensure all values are lists
                if isinstance(data, dict):
                    normalized = {}
                    for k, v in data.items():
                        # Ensure all values are lists (handle single values)
                        if isinstance(v, (int, float)):
                            normalized[str(k)] = [v]
                        elif isinstance(v, list):
                            normalized[str(k)] = v
                        else:
                            normalized[str(k)] = [v]
                    return normalized
                return data
        except Exception as e:
            print(f"Warning: Failed to load pickle file {pickle_path}: {e}")
            print("Trying alternative methods...")

    # Try pyreadr to read RDS directly (if R is available)
    try:
        import pyreadr

        result = pyreadr.read_r(str(filepath))
        # pyreadr returns a dict, get the first (and usually only) value
        data = list(result.values())[0] if result else None
        if data is not None:
            # Normalize data structure
            if isinstance(data, dict):
                normalized = {}
                for k, v in data.items():
                    if isinstance(v, (int, float)):
                        normalized[str(k)] = [v]
                    elif isinstance(v, list):
                        normalized[str(k)] = v
                    else:
                        normalized[str(k)] = [v]
                return normalized
            return data
    except ImportError:
        pass  # pyreadr not available, continue to error
    except Exception as e:
        print(f"Warning: Failed to load RDS file with pyreadr: {e}")

    # If we get here, no file was found or loaded successfully
    print(f"Error: Could not load lookup data from {filepath}")
    print(f"Expected file: {pickle_path}")
    print(f"Please convert RDS to pickle first:")
    print(f"  python json_to_pickle.py {filepath.with_suffix('.json')}")
    print(f"  OR")
    print(f"  python convert_rds_to_pickle_simple.py {filepath}")
    sys.exit(1)


def parse_coordinates(args):
    """Parse coordinates from command-line arguments or JSON file."""
    if args.coords_file:
        with open(args.coords_file, "r") as f:
            data = json.load(f)
            if isinstance(data, list):
                # Format: [{"lat": 37, "lon": -78}, ...]
                coords = [(item["lon"], item["lat"]) for item in data]
            else:
                # Format: {"lats": [37, 36.5], "lons": [-78, -77.5]}
                coords = list(zip(data["lons"], data["lats"]))
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


def run_site_selection(coordinates, script_dir=None, output_dir="output"):
    """
    Main function that mirrors the R script logic.

    Parameters:
    -----------
    coordinates : list of tuples
        List of (lon, lat) coordinate pairs
    script_dir : Path, optional
        Directory where script is located (for relative paths)
    output_dir : str, optional
        Output directory path
    """
    if script_dir is None:
        script_dir = Path(__file__).parent
    else:
        script_dir = Path(script_dir)

    # Input directory is one level up from python_version folder
    # If script is in python_version/, go up one level to find input/
    base_dir = script_dir.parent if script_dir.name == "python_version" else script_dir

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "data").mkdir(exist_ok=True)
    (output_dir / "shp").mkdir(exist_ok=True)
    (output_dir / "figure").mkdir(exist_ok=True)

    print("Loading input data...")

    # Read lookup tables (pickle files - converted from RDS)
    input_data_dir = base_dir / "input" / "data"
    l_up_close_COMID_all = load_lookup_data(input_data_dir / "l_up_close_total.rds")
    l_down_COMID_all = load_lookup_data(input_data_dir / "l_down_total.rds")

    # Read spatial data
    input_shp_dir = base_dir / "input" / "shp"
    sf_river_region = gpd.read_file(input_shp_dir / "sf_river_seg_number", quiet=True)
    sf_ws_region = gpd.read_file(input_shp_dir / "sf_basin_us", quiet=True)

    # Convert to data table (drop geometry for attribute table)
    dt_river_region = pd.DataFrame(sf_river_region.drop(columns="geometry"))

    print(f"Processing {len(coordinates)} point(s)...")

    # Create point GeoDataFrame
    points_df = pd.DataFrame(coordinates, columns=["x", "y"])
    sf_point = gpd.GeoDataFrame(
        points_df,
        geometry=gpd.points_from_xy(points_df["x"], points_df["y"]),
        crs="EPSG:4326",
    )

    # Ensure watershed region is in same CRS
    if sf_ws_region.crs != sf_point.crs:
        sf_ws_region = sf_ws_region.to_crs(sf_point.crs)

    # Intersect points with watersheds (equivalent to st_intersection)
    print("Finding intersecting watersheds (this may take time)...")
    sf_ws_point = gpd.sjoin(sf_ws_region, sf_point, how="inner", predicate="intersects")

    if len(sf_ws_point) == 0:
        raise ValueError(
            "No watersheds found for the given coordinates. Check coordinates are within CONUS."
        )

    s_COMID = sf_ws_point["COMID"].values
    print(f"Found {len(s_COMID)} matching COMID(s): {s_COMID}")

    # Select river segments that receive EW
    comid_sel = s_COMID

    # Create dt_ws_rock (equivalent to data.table subset)
    dt_ws_rock = dt_river_region[dt_river_region["COMID"].isin(comid_sel)][
        ["COMID", "outlet", "Length", "ws_area"]
    ].copy()

    # Get total number of segments downstream (including itself)
    # R: num_seg <- as.numeric(sapply(l_down_COMID_all[as.character(dt_ws_rock$COMID)], length))
    num_seg = [
        len(l_down_COMID_all.get(str(comid), [])) for comid in dt_ws_rock["COMID"]
    ]
    dt_ws_rock["num_seg_downstream"] = num_seg

    # Per outlet, how many segments directly receive EW
    # R: dt_ws_rock[, num_seg_per_outlet := .N, by = outlet]
    outlet_counts = (
        dt_ws_rock.groupby("outlet").size().reset_index(name="num_seg_per_outlet")
    )
    dt_ws_rock = dt_ws_rock.merge(outlet_counts, on="outlet", how="left")

    # Order by num_seg_per_outlet from small to big
    dt_ws_rock = dt_ws_rock.sort_values("num_seg_per_outlet").reset_index(drop=True)

    # Get all downstream segments (with current segment included)
    # R: v_COMID_ode <- unname(unlist(l_down_COMID_all[as.character(dt_ws_rock$COMID)]))
    v_COMID_ode = []
    for comid in dt_ws_rock["COMID"]:
        v_COMID_ode.extend(l_down_COMID_all.get(str(comid), []))
    v_COMID_ode_unique = list(set(v_COMID_ode))  # unique()

    # Get closest upstream tributaries
    # R: v_COMID_ode_up <- unname(unlist(l_up_close_COMID_all[as.character(v_COMID_ode_unique)]))
    v_COMID_ode_up = []
    for comid in v_COMID_ode_unique:
        v_COMID_ode_up.extend(l_up_close_COMID_all.get(str(comid), []))
    v_COMID_ode_up_unique = list(set(v_COMID_ode_up))

    # Remove 1 if present
    v_COMID_ode_up_unique = [c for c in v_COMID_ode_up_unique if c != 1]

    # Pure tributaries not in direct downstreams
    v_COMID_ode_up_unique_tributary = [
        c for c in v_COMID_ode_up_unique if c not in v_COMID_ode_unique
    ]

    # Combine direct downstreams and pure tributaries
    v_COMID_all = v_COMID_ode_unique + v_COMID_ode_up_unique_tributary

    print(f"Total COMIDs in network: {len(v_COMID_all)}")
    print(f"  - Direct downstream: {len(v_COMID_ode_unique)}")
    print(f"  - Tributaries: {len(v_COMID_ode_up_unique_tributary)}")

    # Get spatial features for this region
    sf_river_rock = sf_river_region[
        sf_river_region["COMID"].isin(dt_ws_rock["COMID"])
    ].copy()
    sf_river_outlet = sf_river_region[
        sf_river_region["COMID"].isin(dt_ws_rock["outlet"].unique())
    ].copy()
    sf_ws_outlet = sf_ws_region[
        sf_ws_region["COMID"].isin(dt_ws_rock["outlet"].unique())
    ].copy()

    sf_ws_all = sf_ws_region[sf_ws_region["COMID"].isin(v_COMID_all)].copy()
    sf_river_all = sf_river_region[sf_river_region["COMID"].isin(v_COMID_all)].copy()

    sf_river_ode = sf_river_region[
        sf_river_region["COMID"].isin(v_COMID_ode_unique)
    ].copy()
    sf_river_trib = sf_river_region[
        sf_river_region["COMID"].isin(v_COMID_ode_up_unique_tributary)
    ].copy()

    sf_ws_ode = sf_ws_region[sf_ws_region["COMID"].isin(v_COMID_ode_unique)].copy()
    sf_ws_trib = sf_ws_region[
        sf_ws_region["COMID"].isin(v_COMID_ode_up_unique_tributary)
    ].copy()

    # Calculate centroids
    sf_river_middle = sf_river_rock.copy()
    sf_river_middle["geometry"] = sf_river_rock.centroid

    # Create watershed shapefile for selected points (watersheds that contain each point)
    # sf_ws_point contains the intersection result - extract just the watershed polygons
    # Drop point-related columns and keep only watershed data
    sf_ws_selected = sf_ws_point[["COMID", "geometry"]].copy()
    # Remove duplicates (in case multiple points are in the same watershed)
    sf_ws_selected = sf_ws_selected.drop_duplicates(subset=["COMID"]).reset_index(
        drop=True
    )

    # Add point index information if needed (for mapping back to original coordinates)
    # Create a mapping of which watershed contains which point
    point_watershed_map = []
    for idx in range(len(coordinates)):
        point_matches = sf_ws_point[sf_ws_point.index_right == idx]
        if len(point_matches) > 0:
            # Take the first match (or could take largest if multiple)
            comid = point_matches["COMID"].iloc[0]
            point_watershed_map.append(
                {
                    "point_index": idx,
                    "lon": coordinates[idx][0],
                    "lat": coordinates[idx][1],
                    "comid": int(comid),
                }
            )

    # Save outputs
    print("Saving outputs...")

    # Save CSV
    dt_ws_rock.to_csv(output_dir / "data" / "dt_seg.csv", index=False)

    # Save COMID lists as pickle (efficient binary format)
    with open(output_dir / "data" / "v_COMID_all.pkl", "wb") as f:
        pickle.dump(v_COMID_all, f, protocol=pickle.HIGHEST_PROTOCOL)
    with open(output_dir / "data" / "v_COMID_ode_unique.pkl", "wb") as f:
        pickle.dump(v_COMID_ode_unique, f, protocol=pickle.HIGHEST_PROTOCOL)
    with open(output_dir / "data" / "v_COMID_ode_up_unique_tributary.pkl", "wb") as f:
        pickle.dump(
            v_COMID_ode_up_unique_tributary, f, protocol=pickle.HIGHEST_PROTOCOL
        )

    # Save shapefiles
    sf_ws_all.to_file(output_dir / "shp" / "sf_ws_all.shp")
    sf_ws_selected.to_file(
        output_dir / "shp" / "sf_ws_selected.shp"
    )  # Watersheds containing selected points
    sf_river_ode.to_file(output_dir / "shp" / "sf_river_ode.shp")
    sf_river_trib.to_file(output_dir / "shp" / "sf_river_trib.shp")
    sf_river_middle.to_file(output_dir / "shp" / "sf_river_middle.shp")

    # Save point-watershed mapping as JSON for easy lookup
    with open(output_dir / "data" / "point_watershed_map.json", "w") as f:
        json.dump(point_watershed_map, f, indent=2)

    # Create plot
    print("Creating visualization...")
    bbox = sf_ws_all.total_bounds
    asp_ratio = (bbox[3] - bbox[1]) / (bbox[2] - bbox[0])

    fig, ax = plt.subplots(figsize=(6, 6 * asp_ratio), dpi=500)

    # Plot watersheds (background)
    sf_ws_all.plot(
        ax=ax, color="#eeeeee", edgecolor="#aaaaaa", linewidth=0.1, alpha=0.5
    )

    # Plot rivers
    sf_river_ode.plot(ax=ax, color=C_S[5], linewidth=0.2)  # c_s[6] in R (0-indexed)
    sf_river_trib.plot(ax=ax, color=C_S[4], linewidth=0.15)  # c_s[5]
    sf_river_rock.plot(ax=ax, color=C_S[0], linewidth=0.2)  # c_s[1]

    # Plot outlet watershed
    sf_ws_outlet.plot(ax=ax, color=C_S[5], edgecolor=C_S[5], linewidth=0.1, alpha=0.3)

    # Plot centroids
    sf_river_middle.plot(
        ax=ax,
        color=C_S[0],
        edgecolor="#aaaaaa",
        linewidth=0.1,
        markersize=1,
        marker="o",
    )

    ax.set_xlim(bbox[0], bbox[2])
    ax.set_ylim(bbox[1], bbox[3])
    ax.set_aspect("equal")
    ax.grid(True, linestyle="--", linewidth=0.3, alpha=0.5)
    ax.set_xlabel("Longitude", fontsize=10)
    ax.set_ylabel("Latitude", fontsize=10)
    ax.tick_params(labelsize=10)

    plt.tight_layout()
    plt.savefig(
        output_dir / "figure" / "map_river_ws.png", dpi=500, bbox_inches="tight"
    )
    plt.close()

    print(f"✓ Site selection complete! Outputs saved to {output_dir}")
    return {
        "dt_ws_rock": dt_ws_rock,
        "v_COMID_all": v_COMID_all,
        "v_COMID_ode_unique": v_COMID_ode_unique,
        "v_COMID_ode_up_unique_tributary": v_COMID_ode_up_unique_tributary,
    }


def main():
    parser = argparse.ArgumentParser(
        description="DRN Step 1: Site Selection - Python version",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # From JSON file
  python 01_site_selection.py --coords-file coords.json
  
  # Single point
  python 01_site_selection.py --lat 37 --lon -78
  
  # Multiple points
  python 01_site_selection.py --lat 37 --lon -78 --lat 36.5 --lon -77.5
        """,
    )

    parser.add_argument(
        "--coords-file",
        type=str,
        help='JSON file with coordinates: [{"lat": 37, "lon": -78}, ...] or {"lats": [37], "lons": [-78]}',
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
        "--output-dir",
        type=str,
        default="output",
        help="Output directory (default: output)",
    )

    args = parser.parse_args()

    try:
        coords = parse_coordinates(args)
        script_dir = Path(args.script_dir) if args.script_dir else Path(__file__).parent

        results = run_site_selection(
            coords, script_dir=script_dir, output_dir=args.output_dir
        )

        print("\nSummary:")
        print(f"  Selected segments: {len(results['dt_ws_rock'])}")
        print(f"  Total network COMIDs: {len(results['v_COMID_all'])}")

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
