import os
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
            loc = geo[0]["geometry"]["location"]
            city = ""
            for component in geo[0]["address_components"]:
                if "locality" in component["types"]:
                    city = component["long_name"]
                    break
            return loc["lat"], loc["lng"], city
    except Exception:
        pass
    return "", "", ""
