import os
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from playwright.sync_api import sync_playwright

URL = "https://exactsports.com/soccer/#tve-jump-1896baece7f"
URLS = [
    "https://exactsports.com/events/2956/soccer/girls/x1-showcase-camp-new-york-city-girls-07-2025",
    "https://exactsports.com/events/2936/soccer/girls/x1-showcase-camp-new-jersey-girls-07-2025",
    "https://exactsports.com/events/3036/soccer/boys/x1-showcase-camp-new-jersey-boys-07-2025",
    "https://exactsports.com/events/2912/soccer/girls/x1-showcase-camp-philadelphia-girls-06-2025",
    "https://exactsports.com/events/3044/soccer/boys/x1-showcase-camp-philadelphia-boys-07-2025",
    "https://exactsports.com/events/3040/soccer/boys/x1-showcase-camp-connecticut-boys-07-2025",
    "https://exactsports.com/events/2979/soccer/boys/x1-showcase-camp-baltimore-boys-06-2025",
    "https://exactsports.com/events/2920/soccer/girls/x1-showcase-camp-baltimore-girls-07-2025",
    "https://exactsports.com/events/2967/soccer/girls/x1-showcase-camp-boston-girls-07-2025",
    "https://exactsports.com/events/3048/soccer/boys/x1-showcase-camp-boston-boys-07-2025",
    "https://exactsports.com/events/2982/soccer/girls/x1-showcase-camp-washington-dc-girls-06-2025",
    "https://exactsports.com/events/3023/soccer/boys/x1-showcase-camp-dc-boys-07-2025",
    "https://exactsports.com/events/2919/soccer/girls/x1-showcase-camp-buffalo-girls-07-2025",
    "https://exactsports.com/events/3009/soccer/boys/x1-showcase-camp-buffalo-boys-07-2025",
    "https://exactsports.com/events/3003/soccer/boys/x1-showcase-camp-richmond-boys-07-2025",
    "https://exactsports.com/events/3012/soccer/boys/x1-showcase-camp-virginia-beach-boys-07-2025",
    "https://exactsports.com/events/2981/soccer/boys/x1-showcase-camp-pittsburgh-boys-06-2025",
    "https://exactsports.com/events/2924/soccer/girls/x1-showcase-camp-cleveland-girls-07-2025",
    "https://exactsports.com/events/3027/soccer/boys/x1-showcase-camp-cleveland-boys-07-2025",
    "https://exactsports.com/events/2995/soccer/boys/x1-showcase-camp-raleigh-boys-06-2025",
    "https://exactsports.com/events/2918/soccer/girls/x1-showcase-camp-raleigh-girls-07-2025",
    "https://exactsports.com/events/3043/soccer/boys/x1-showcase-camp-columbus-boys-07-2025",
    "https://exactsports.com/events/2902/soccer/girls/x1-showcase-camp-detroit-girls-06-2025",
    "https://exactsports.com/events/2991/soccer/boys/x1-showcase-camp-detroit-boys-06-2025",
    "https://exactsports.com/events/2913/soccer/girls/x1-showcase-camp-charlotte-girls-06-2025",
    "https://exactsports.com/events/3037/soccer/boys/x1-showcase-camp-charlotte-boys-07-2025",
    "https://exactsports.com/events/3020/soccer/boys/x1-showcase-camp-indianapolis-boys-07-2025",
    "https://exactsports.com/events/2976/soccer/boys/x1-showcase-camp-chicago-boys-1-06-2025",
    "https://exactsports.com/events/2964/soccer/girls/x1-showcase-camp-chicago-girls-07-2025",
    "https://exactsports.com/events/3047/soccer/boys/x1-showcase-camp-chicago-boys-2-07-2025",
    "https://exactsports.com/events/2968/soccer/boys/x1-showcase-camp-milwaukee-boys-06-2025",
    "https://exactsports.com/events/2922/soccer/girls/x1-showcase-camp-milwaukee-girls-07-2025",
    "https://exactsports.com/events/2921/soccer/girls/x1-showcase-camp-nashville-girls-07-2025",
    "https://exactsports.com/events/3042/soccer/boys/x1-showcase-camp-nashville-boys-07-2025",
    "https://exactsports.com/events/3025/soccer/boys/x1-showcase-camp-atlanta-boys-07-2025",
    "https://exactsports.com/events/2938/soccer/girls/x1-showcase-camp-atlanta-girls-07-2025",
    "https://exactsports.com/events/2987/soccer/boys/x1-showcase-camp-birmingham-boys-06-2025",
    "https://exactsports.com/events/2914/soccer/girls/x1-showcase-camp-st-louis-girls-06-2025",
    "https://exactsports.com/events/3049/soccer/boys/x1-showcase-camp-st-louis-boys-07-2025",
    "https://exactsports.com/events/3016/soccer/boys/x1-showcase-camp-orlando-boys-07-2025",
    "https://exactsports.com/events/2901/soccer/girls/x1-showcase-camp-tampa-girls-06-2025",
    "https://exactsports.com/events/2958/soccer/boys/x1-showcase-camp-tampa-boys-06-2025",
    "https://exactsports.com/events/2917/soccer/girls/x1-showcase-camp-minneapolis-girls-06-2025",
    "https://exactsports.com/events/3038/soccer/boys/x1-showcase-camp-minneapolis-boys-07-2025",
    "https://exactsports.com/events/2940/soccer/girls/x1-showcase-camp-kansas-city-girls-07-2025",
    "https://exactsports.com/events/2947/soccer/girls/x1-showcase-camp-miami-girls-07-2025",
    "https://exactsports.com/events/3041/soccer/boys/x1-showcase-camp-miami-boys-07-2025",
    "https://exactsports.com/events/2960/soccer/boys/x1-showcase-camp-omaha-boys-06-2025",
    "https://exactsports.com/events/2999/soccer/boys/x1-showcase-camp-oklahoma-boys-06-2025",
    "https://exactsports.com/events/2903/soccer/girls/x1-showcase-camp-dallas-girls-06-2025",
    "https://exactsports.com/events/2963/soccer/boys/x1-showcase-camp-dallas-boys-1-06-2025",
    "https://exactsports.com/events/3039/soccer/boys/x1-showcase-camp-dallas-boys-2-07-2025",
    "https://exactsports.com/events/3167/soccer/girls/x1-showcase-camp-dallas-girls-2-07-2025",
    "https://exactsports.com/events/3031/soccer/boys/x1-showcase-camp-houston-boys-07-2025",
    "https://exactsports.com/events/2950/soccer/girls/x1-showcase-camp-houston-girls-07-2025",
    "https://exactsports.com/events/3008/soccer/boys/x1-showcase-camp-san-antonio-boys-07-2025",
    "https://exactsports.com/events/3029/soccer/boys/x1-showcase-camp-denver-boys-07-2025",
    "https://exactsports.com/events/2944/soccer/girls/x1-showcase-camp-denver-girls-07-2025",
    "https://exactsports.com/events/3045/soccer/boys/x1-showcase-camp-albuquerque-boys-07-2025",
    "https://exactsports.com/events/2910/soccer/girls/x1-showcase-camp-phoenix-girls-06-2025",
    "https://exactsports.com/events/2966/soccer/boys/x1-showcase-camp-phoenix-boys-06-2025",
    "https://exactsports.com/events/3022/soccer/boys/x1-showcase-camp-boise-boys-07-2025",
    "https://exactsports.com/events/3051/soccer/boys/x1-showcase-camp-los-angeles-boys-07-2025",
    "https://exactsports.com/events/2932/soccer/girls/x1-showcase-camp-seattle-girls-07-2025",
    "https://exactsports.com/events/3050/soccer/boys/x1-showcase-camp-seattle-boys-07-2025",
    "https://exactsports.com/events/3019/soccer/boys/x1-showcase-camp-riverside-boys-07-2025",
    "https://exactsports.com/events/2974/soccer/boys/x1-showcase-camp-san-diego-boys-06-2025",
    "https://exactsports.com/events/2915/soccer/girls/x1-showcase-camp-san-diego-girls-06-2025",
    "https://exactsports.com/events/2952/soccer/girls/x1-showcase-camp-portland-girls-07-2025",
    "https://exactsports.com/events/2923/soccer/girls/x1-showcase-camp-riverside-girls-07-2025",
    "https://exactsports.com/events/2971/soccer/girls/x1-showcase-camp-los-angeles-girls-07-2025",
    "https://exactsports.com/events/2911/soccer/girls/x1-showcase-camp-sacramento-girls-06-2025",
    "https://exactsports.com/events/3046/soccer/boys/x1-showcase-camp-sacramento-boys-07-2025",
    "https://exactsports.com/events/2916/soccer/girls/x1-showcase-camp-san-francisco-girls-06-2025",
    "https://exactsports.com/events/3033/soccer/boys/x1-showcase-camp-san-francisco-boys-07-2025",
    "https://exactsports.com/events/3033/soccer/boys/x1-showcase-camp-san-francisco-boys-07-2025"
]
GRADES_XPATH = "/html/body/section[1]/div[2]/div/div[2]/div[1]/div[2]/div[1]/div[2]/p"
COST_XPATH = "/html/body/section[1]/div[2]/div/div[2]/div[1]/div[2]/div[2]/div[2]/h1/span"
ADDRESS_XPATH = "/html/body/section[1]/div[2]/div/div[2]/div[1]/div[2]/div[1]/div[3]/a"

SHEET_ID = os.getenv("SHEET_ID")
if not SHEET_ID:
    raise EnvironmentError("SHEET_ID environment variable not set")
TAB_NAME = "Camps"
CREDS_FILE = "cspscraping.json"

scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name(CREDS_FILE, scope) # type: ignore
client = gspread.authorize(creds) # type: ignore

try:
    sheet = client.open_by_key(SHEET_ID).worksheet(TAB_NAME)
    print(f"Successfully accessed sheet: {sheet.title}")
except gspread.exceptions.SpreadsheetNotFound:
    print("Error: The SHEET_ID is invalid or the service account does not have access.")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

def get_camp_data(url):
    """
    Extracts camp data from a webpage using Playwright and converts it into a pandas DataFrame.

    Args:
        url (str): The URL of the webpage.

    Returns:
        pd.DataFrame: A DataFrame containing the camp data.
    """
    camp_data = {}
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(url)

        # Wait for the camp data to load
        page.wait_for_selector(f"xpath={ADDRESS_XPATH}")

        # Extract camp data
        camp_data["url"] = url
        camp_data["address"] = page.locator(f"xpath={ADDRESS_XPATH}").inner_text()
        camp_data["grades"] = page.locator(f"xpath={GRADES_XPATH}").inner_text()
        camp_data["cost"] = page.locator(f"xpath={COST_XPATH}").inner_text()

        browser.close()

    return camp_data

def create_dataframe_and_write_to_csv(data, output_file):
    """
    Creates a DataFrame with columns URL, address, grades, and cost, and writes it to a CSV file.

    Args:
        data (list of dict): List of dictionaries containing URL, address, grades, and cost.
        output_file (str): Path to the output CSV file.
    """
    # Create a DataFrame
    df = pd.DataFrame(data, columns=["url", "address", "grades", "cost"])

    # Write the DataFrame to a CSV file
    df.to_csv(output_file, index=False)
    print(f"DataFrame written to {output_file}")
    return df

    return df

def main():
    """
    Main function to create a DataFrame with one row per URL in URLS.
    Extracts address, grades, and cost for each URL and writes the DataFrame to a CSV file.
    """
    data = []
    for url in URLS:
        
        # Print the URL being processed
        print("Trying to extract data from:", url)
        data.append(get_camp_data(url))

    output_file = "exact_sports_camps.csv"
    df = create_dataframe_and_write_to_csv(data, output_file)
    print(df)
if __name__ == "__main__":
    main()
