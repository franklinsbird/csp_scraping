import requests
from bs4 import BeautifulSoup
import csv
from geopy.geocoders import Nominatim
import time

# Initialize geolocator
geolocator = Nominatim(user_agent="exact_sports_scraper")

# URL of the EXACT Sports Soccer Camps page
url = "https://exactsports.com/soccer/#tve-jump-1896baece7f"

# Send a GET request to the page
response = requests.get(url)
soup = BeautifulSoup(response.content, "html.parser")

# Find the table using the provided XPath
# Note: BeautifulSoup doesn't support XPath directly, so we need to find the table by other means
# Assuming the table has a unique class or id; if not, adjust the selector accordingly
table = soup.find("table")

# Prepare CSV file
csv_file = "exact_sports_soccer_camps.csv"
fieldnames = [
    "Camp Name", "Camp Organizer", "Camp Type", "Image", "URL", "Lat", "Long",
    "Start_date", "End_date", "City", "State", "Grade Level", "Ages", "Division", "Cost", "Gender"
]

with open(csv_file, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.DictWriter(file, fieldnames=fieldnames)
    writer.writeheader()

    # Iterate over table rows
    for row in table.find_all("tr")[1:]:  # Skip header row
        cols = row.find_all("td")
        if len(cols) < 5:
            continue  # Skip rows that don't have enough columns

        gender = cols[0].get_text(strip=True)
        state = cols[1].get_text(strip=True)
        camp_name = cols[2].get_text(strip=True)
        date = cols[3].get_text(strip=True)
        url_tag = cols[4].find("a")
        camp_url = url_tag['href'] if url_tag else ""

        # Since the camps are one-day events, start and end dates are the same
        start_date = end_date = date

        # Fetch additional details from the camp URL
        city = ""
        lat = ""
        lon = ""
        grade_level = ""
        ages = ""
        cost = ""
        if camp_url:
            camp_response = requests.get(camp_url)
            camp_soup = BeautifulSoup(camp_response.content, "html.parser")
            # Example: Extract city from the camp page
            # Adjust the selector based on the actual structure of the camp pages
            location_tag = camp_soup.find("div", class_="location")
            if location_tag:
                city = location_tag.get_text(strip=True)
                # Geocode the address to get lat and lon
                try:
                    location = geolocator.geocode(f"{city}, {state}")
                    if location:
                        lat = location.latitude
                        lon = location.longitude
                except:
                    pass
            # Example: Extract grade level, ages, and cost
            # Adjust the selectors based on the actual structure of the camp pages
            details = camp_soup.find_all("div", class_="camp-detail")
            for detail in details:
                text = detail.get_text(strip=True)
                if "Grade Level" in text:
                    grade_level = text.split(":")[-1].strip()
                elif "Ages" in text:
                    ages = text.split(":")[-1].strip()
                elif "Cost" in text:
                    cost = text.split(":")[-1].strip()

        # Write the row to CSV
        writer.writerow({
            "Camp Name": camp_name,
            "Camp Organizer": "ExactSports",
            "Camp Type": "collaborative",
            "Image": "",
            "URL": camp_url,
            "Lat": lat,
            "Long": lon,
            "Start_date": start_date,
            "End_date": end_date,
            "City": city,
            "State": state,
            "Grade Level": grade_level,
            "Ages": ages,
            "Division": "",
            "Cost": cost,
            "Gender": gender
        })

print(f"Data has been written to {csv_file}")
