from bs4 import BeautifulSoup
import requests
import json
import copy
import os

def get_llm_data(res, html_tag):
    """
    Extracts structured data from the specified HTML tag in the response.

    Args:
        res: The HTTP response object.
        html_tag: The HTML tag or tag with attributes to search for (e.g., "div", "div class='dt-box'").

    Returns:
        list: A list of additional camps extracted from the response.
    """
    soup = BeautifulSoup(res.text, "html.parser")
    text_blocks = soup.select(html_tag)  # Use CSS selectors for flexibility
    for i, tag in enumerate(text_blocks[:2]):
        print(f"Block {i + 1}: {tag.text.strip()}")
    relevant_lines = []

    for tag in text_blocks:
        if tag.text:
            text = tag.text.strip()
            text_lower = text.lower()
            # if any(keyword in text_lower for keyword in ["camp", "date", "session", "ages", "$", "–", "to", "through"]) or any(char.isdigit() for char in text_lower):
            relevant_lines.append(text)

    snippet = "\n".join(relevant_lines)[:5000]
    OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
    prompt = f"""
    You are a structured data extractor. From the following text, extract ONLY the values below and return them in strict JSON format. You are looking for
    information about soccer camps, including the event name, start and end dates, ages, and cost. Only extract data if you
    are confident there is a soccer camp occurring in the near future. Do not return data just because you see the word soccer.
    There should be at least the word camp and probably a start date of some kind to represent a valid camp.
    If you are not confident, return an empty string for all fields. The text may contain various formats of dates.
    You may encounter data on multiple camps. If this is the case, return several JSON objects in an array. They may often be contained in an HTML table format.
    If you find a start_date but no end_date, assume the end_date is the same as the start_date.

    Fields:
    - event_name
    - start_date
    - end_date
    - ages
    - cost

    Text:
    """
    {snippet}
    """

    Return only this format:
    {{"event_name":"", "start_date": "", "end_date": "", "ages": "", "cost": ""}}

    Even if the text does not contain all fields, return an empty string for those fields. Do not return any other text or explanation, just the JSON.
    """

    headers_llm = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json"
    }
    payload = {
        "model": "google/gemma-3n-e4b-it:free",
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.2
    }

    addl_camps = []
    camp = {
        "Event Details": "",
        "start_date": "",
        "end_date": "",
        "Ages / Grade Level": "",
        "Cost": "",
        "Camp Found?": "No"
    }

    try:
        llm_resp = requests.post("https://openrouter.ai/api/v1/chat/completions", headers=headers_llm, data=json.dumps(payload))
        llm_json = llm_resp.json()
        print("LLM Response:", llm_json)
        if "choices" in llm_json and llm_json["choices"]:
            llm_output = llm_json["choices"][0]["message"]["content"]
        else:
            raise ValueError("No 'choices' in LLM response")
        if llm_output.startswith("```"):
            llm_output = llm_output.strip("`").strip()
        json_start = llm_output.find('[')
        if json_start == -1:
            raise ValueError("No '[' found in LLM output")
        json_snippet = llm_output[json_start:]
        try:
            parsed = json.loads(json_snippet)
        except json.JSONDecodeError:
            raise
        try:
            if isinstance(parsed, list) and len(parsed) > 0:
                camp["Camp Found?"] = "Yes"
                camp.update({
                    "Event Details": parsed[0].get("event_name", camp.get("Event Details")),
                    "start_date": parsed[0].get("start_date", ""),
                    "end_date": parsed[0].get("end_date", ""),
                    "Ages / Grade Level": parsed[0].get("ages", ""),
                    "Cost": parsed[0].get("cost", "")
                })
                addl_camps.append(camp)
                print("Adding camp:", camp["Event Details"], "to the list.")
                for camp_obj in parsed[1:]:
                    new_camp = copy.deepcopy(camp)
                    new_camp.update({
                        "Event Details": camp_obj.get("event_name", camp.get("Event Details")),
                        "start_date": camp_obj.get("start_date", ""),
                        "end_date": camp_obj.get("end_date", ""),
                        "Ages / Grade Level": camp_obj.get("ages", ""),
                        "Cost": camp_obj.get("cost", "")
                    })
                    addl_camps.append(new_camp)
                    print("Adding additional camp:", new_camp["Event Details"], "to the list.")
            else:
                camp["Camp Found?"] = "No"
        except json.JSONDecodeError:
            pass
    except Exception:
        camp["start_date"] = camp["end_date"] = camp["Ages / Grade Level"] = camp["Cost"] = "LLM Error"
    return addl_camps

# Example usage:
# res = requests.get("https://collegesoccerprospects.com")
# camps = get_llm_data(res, "div.dt-box")
# print(camps)
