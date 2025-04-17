import json
import requests
import os
from datetime import datetime
from collections import defaultdict

# Get the directory where this script is located
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
COORDINATE_SETS_FILE = "coordinate_sets_count.json"
DUPLICATES_FILE = "location_duplicates.json"

# API Configuration
FORMAT_VERSION = 0.1
API_KEY = "018ffe73-baf6-7140-ab65-8a243c0ee02d"
URL = "https://ropeless.network/api/search_own/"


def search_all_deployed():
    """
    Search for all deployed gear using the search_own endpoint.

    Returns:
        dict: Dictionary containing all deployed sets
    """
    body = {
        "api_key": API_KEY,
        "format_version": FORMAT_VERSION,
        "from_latitude": -90,
        "to_latitude": 90,
        "from_longitude": -180,
        "to_longitude": 180,
        "status": "deployed",
    }

    response = requests.post(URL, json=body)

    if response.status_code != 200:
        print(f"Error: {response.status_code} ({response.text})")
        return None

    data = response.json()

    # Write raw response to file
    raw_response_file = os.path.join(SCRIPT_DIR, "raw_search_own_api_response.json")
    with open(raw_response_file, "w") as file:
        json.dump(data, file, indent=4)
    print(f"Raw API response written to {raw_response_file}")

    sets = data.get("sets", [])

    result = {"total_sets_found": len(sets), "sets": sets}

    return result


def find_location_duplicates(sets):
    """
    Find sets that are deployed at the exact same location.

    Args:
        sets (list): List of sets to analyze

    Returns:
        dict: Dictionary of location duplicates with string keys
    """
    # Group sets by location
    location_groups = defaultdict(list)

    for set_item in sets:
        # Get the first deployed trap's location to represent the set's location
        for trap in set_item.get("traps", []):
            if trap.get("status") == "deployed":
                lat = trap.get("latitude")
                lon = trap.get("longitude")
                if lat is not None and lon is not None:
                    location_key = f"{lat},{lon}"
                    location_groups[location_key].append(
                        {
                            "set_id": set_item.get("set_id"),
                            "trap_id": trap.get("trap_id"),
                            "deploy_datetime_utc": trap.get("deploy_datetime_utc"),
                        }
                    )
                    break  # Only use the first deployed trap's location

    # Filter for locations with multiple sets
    duplicates = {
        location: sets for location, sets in location_groups.items() if len(sets) > 1
    }

    return duplicates


def search_by_coordinates(latitude, longitude):
    """
    Search for sets at a specific location using the search_own endpoint.

    Args:
        latitude (float): Latitude of the search location
        longitude (float): Longitude of the search location

    Returns:
        dict: Dictionary containing the count of sets found and their details
    """
    body = {
        "api_key": API_KEY,
        "format_version": FORMAT_VERSION,
        "from_latitude": latitude,
        "to_latitude": latitude,
        "from_longitude": longitude,
        "to_longitude": longitude,
        "status": "deployed",
    }

    response = requests.post(URL, json=body)

    if response.status_code != 200:
        print(f"Error: {response.status_code} ({response.text})")
        return None

    data = response.json()

    # Write raw response to file
    raw_response_file = os.path.join(SCRIPT_DIR, "raw_api_response.json")
    with open(raw_response_file, "w") as file:
        json.dump(data, file, indent=4)
    print(f"Raw API response written to {raw_response_file}")

    sets = data.get("sets", [])

    result = {
        "search_location": {
            "from_latitude": latitude,
            "to_latitude": latitude,
            "from_longitude": longitude,
            "to_longitude": longitude,
        },
        "total_sets_found": len(sets),
        "sets": sets,
    }

    # Write results to file
    with open(COORDINATE_SETS_FILE, "w") as file:
        json.dump(result, file, indent=4)

    print(f"\nFound {len(sets)} sets at exact location ({latitude}, {longitude})")
    print(f"Results written to {COORDINATE_SETS_FILE}")

    return result


def write_duplicate_details(duplicates, output_file):
    """
    Write detailed information about duplicate locations to a file.

    Args:
        duplicates (dict): Dictionary of location duplicates
        output_file (str): Path to the output file
    """
    detailed_output = {}

    for location, sets in duplicates.items():
        lat, lon = location.split(",")
        detailed_output[location] = {
            "latitude": float(lat),
            "longitude": float(lon),
            "number_of_sets": len(sets),
            "sets": [
                {
                    "set_id": set_info["set_id"],
                    "trap_id": set_info["trap_id"],
                    "deploy_datetime_utc": set_info["deploy_datetime_utc"],
                }
                for set_info in sets
            ],
        }

    with open(output_file, "w") as file:
        json.dump(detailed_output, file, indent=4)


def main():
    """Main function to run the coordinate search or find location duplicates"""
    # Check if coordinates are provided

    # leo's location 1
    # latitude = 20.12232
    # longitude = -98.73646

    # leo's location 2
    # latitude = 19.28666
    # longitude = -99.66275

    # Coordinates from the duplicated trap
    # latitude = 40.5546611
    # longitude = -70.582792

    # if latitude and longitude:
    #    coordinates_provided = True
    # else:
    coordinates_provided = False

    if coordinates_provided:
        print(f"Searching for sets at exact location ({latitude}, {longitude})")
        result = search_by_coordinates(latitude, longitude)

        if result:
            print("\nSearch Results:")
            print(f"Total sets found: {result['total_sets_found']}")
            print(f"Results saved to {COORDINATE_SETS_FILE}")
    else:
        print("No coordinates provided. Searching for all deployed gear...")
        result = search_all_deployed()

        if result:
            print(f"\nFound {result['total_sets_found']} deployed sets")
            duplicates = find_location_duplicates(result["sets"])

            # Calculate unique sets after removing duplicates
            total_duplicate_sets = sum(len(sets) for sets in duplicates.values())
            total_unique_locations = len(duplicates)
            remaining_sets = result["total_sets_found"] - (
                total_duplicate_sets - total_unique_locations
            )

            print(f"\nFound {len(duplicates)} locations with multiple deployed sets:")
            print("\nSummary of duplicate locations:")
            print(
                "Location (Lat, Lon) | Number of Sets | Example Trap ID | First Deployed"
            )
            print("-" * 80)

            for location, sets in duplicates.items():
                lat, lon = location.split(",")
                # Get the first trap ID as an example
                example_trap_id = sets[0]["set_id"]
                # Get the earliest deployment date
                deploy_dates = [
                    set_info["deploy_datetime_utc"]
                    for set_info in sets
                    if set_info["deploy_datetime_utc"]
                ]
                first_deployed = min(deploy_dates) if deploy_dates else "Unknown"
                print(
                    f"({lat}, {lon}) | {len(sets)} | {example_trap_id} | {first_deployed}"
                )

            print("\nSummary of duplicates:")
            print(f"Total sets found: {result['total_sets_found']}")
            print(f"Total duplicate sets: {total_duplicate_sets}")
            print(f"Unique locations with duplicates: {total_unique_locations}")
            print(
                f"Sets that would be removed: {total_duplicate_sets - total_unique_locations}"
            )
            print(f"Sets remaining after removing duplicates: {remaining_sets}")

            # Write detailed information to file
            write_duplicate_details(duplicates, DUPLICATES_FILE)
            print(f"\nDetailed information saved to {DUPLICATES_FILE}")


if __name__ == "__main__":
    main()

# ! Example
#   "vessel_id": "74218a3d-08d5-4fcd-a71e-fe85c81467d3",
#   "set_id": "188bd1f6-972a-4ce2-be3b-129b74b6ee79",
#   "deployment_type": "single",
#   "traps_in_set": 1,
#   "trawl_path": "",
#   "share_with": [
#     "Earth Ranger"
#   ],
#   "traps": [
#     {
#       "trap_id": "e_testestestestestestestestestestestest",
#       "sequence": 0,
#       "latitude": -90,
#       "longitude": -180,
#       "deploy_datetime_utc": "2025-01-07T21:44:57.627000Z",
#       "surface_datetime_utc": "2025-01-07T21:44:57.627000Z",
#       "retrieved_datetime_utc": "2025-03-31T23:54:35.256000Z",
#       "status": "retrieved",
#       "accuracy": "string",
#       "release_type": "none",
#       "is_on_end": true
#     }
#   ],
#   "when_updated_utc": "2025-03-31T23:54:35.256000Z"
# }
