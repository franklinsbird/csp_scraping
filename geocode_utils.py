import os
import pandas as pd
from googlemaps import Client as GoogleMaps

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
if not GOOGLE_API_KEY:
    raise EnvironmentError("GOOGLE_API_KEY environment variable not set")

gmaps = GoogleMaps(key=GOOGLE_API_KEY)

def get_lat_long(place: str):
    """Return (lat, lng, city) for a place using Google Maps geocoding."""
    try:
        geo = gmaps.geocode(place.strip())
        if geo:
            print(f"Geocoding {place}...")
            loc = geo[0]["geometry"]["location"]
            city = ""
            for component in geo[0]["address_components"]:
                if "locality" in component["types"]:
                    city = component["long_name"]
                    break
            return loc["lat"], loc["lng"], city
            print(f"Geocoded {place}: {loc['lat']}, {loc['lng']}, {city}")
    except Exception:
        print(f"Error geocoding {place}.")
    return "", "", ""

def geocode_csv(input_csv: str, output_csv: str):
    """Geocode addresses from an input CSV and write results to an output CSV."""
    # Load input CSV
    df = pd.read_csv(input_csv)
    if "address" not in df.columns:
        raise ValueError("Input CSV must contain an 'address' column")

    # Prepare output columns
    df["latitude"] = ""
    df["longitude"] = ""
    df["city"] = ""

    # Geocode each address
    for index, row in df.iterrows():
        address = row["address"]
        lat, lng, city = get_lat_long(address)
        df.at[index, "latitude"] = lat
        df.at[index, "longitude"] = lng
        df.at[index, "city"] = city

    print (f"Geocoded {len(df)} addresses.")
    
    # Write results to output CSV
    df.to_csv(output_csv, index=False)
    print(f"Geocoding complete. Results saved to {output_csv}.")


if __name__ == "__main__":
    input_csv = "/Users/fbird/Downloads/Addresses_to_Geocode.csv"
    output_csv = "/Users/fbird/Downloads/Addresses_geocoded.csv"
    print(f"Input CSV: {input_csv}")
    print(f"Output CSV: {output_csv}")
    if not os.path.exists(input_csv):
        raise FileNotFoundError(f"Input CSV file {input_csv} does not exist.")
    out_dir = os.path.dirname(output_csv)
    if out_dir and not os.path.exists(out_dir):
        raise FileNotFoundError(f"Output directory {out_dir} does not exist.")
    geocode_csv(input_csv, output_csv)
