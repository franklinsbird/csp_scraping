import requests
from bs4 import BeautifulSoup
import csv
from geopy.geocoders import Nominatim
import time

BASE_URL = "https://www.ussportscamps.com"
START_URL = f"{BASE_URL}/soccer/nike"
geolocator = Nominatim(user_agent="nike-camp-scraper")

def get_lat_long(location):
    try:
        time.sleep(1)  # to avoid hitting rate limits
        loc = geolocator.geocode(location)
        return (loc.latitude, loc.longitude) if loc else ("", "")
    except:
        return ("", "")

def classify_grade_level(ages):
    grade_levels = []
    age_list = [int(s.strip()) for s in ages.replace('+', '').split('–') if s.strip().isdigit()]
    if any(2 <= age <= 12 for age in age_list):
        grade_levels.append("Elementary School")
    if any(13 <= age <= 14 for age in age_list):
        grade_levels.append("Middle School")
    if any(15 <= age <= 18 for age in age_list):
        grade_levels.append("High School")
    return ", ".join(grade_levels)

def scrape_state_camps():
    response = requests.get(START_URL)
    soup = BeautifulSoup(response.text, 'html.parser')

    camps = []

    # All clickable camp links on the main page
    links = soup.select('dl.locations-list a')
    for link in links:
        camp_url = BASE_URL + link.get('href')
        camp_resp = requests.get(camp_url)
        camp_soup = BeautifulSoup(camp_resp.text, 'html.parser')

        title = camp_soup.find('h1')
        title = title.text.strip() if title else "Unknown Camp Name"

        city_state = camp_soup.select_one('.location')
        city, state = "", ""
        if city_state:
            parts = city_state.text.strip().split(',')
            city = parts[0].strip()
            if len(parts) > 1:
                state = parts[1].strip()

        age = "Not listed"
        age_tag = camp_soup.find(text=lambda t: "ages" in t.lower())
        if age_tag:
            age = age_tag.strip().split(':')[-1].strip()

        date = "Not listed"
        date_tag = camp_soup.find(text=lambda t: "date" in t.lower())
        if date_tag:
            date = date_tag.strip().split(':')[-1].strip()
        start_date, end_date = "", ""
        if 'to' in date:
            start_date, end_date = [d.strip() for d in date.split('to')]

        cost = "Not listed"
        cost_tag = camp_soup.find(text=lambda t: "$" in t)
        if cost_tag:
            cost = cost_tag.strip()

        gender = "Coed"
        if "boys" in title.lower():
            gender = "Boys"
        elif "girls" in title.lower():
            gender = "Girls"

        full_location = f"{title}, {city}, {state}"
        lat, lon = get_lat_long(full_location)
        grade_level = classify_grade_level(age)

        camps.append({
            "Camp Name": title,
            "Camp Organizer": "Nike",
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
            "Ages": age,
            "Division": "",
            "Cost": cost,
            "Gender": gender
        })

    return camps

def write_csv(camps, filename="nike_soccer_camps_all_states.csv"):
    fieldnames = [
        "Camp Name", "Camp Organizer", "Camp Type", "Image", "URL", "Lat", "Long",
        "Start_date", "End_date", "City", "State", "Grade Level", "Ages", "Division", "Cost", "Gender"
    ]
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
