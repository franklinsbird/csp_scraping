from bs4 import BeautifulSoup
import requests
import json
import copy
import os

def get_llm_data_from_markdown(markdown, prompt=None):
    """
    Extracts structured data from markdown.

    Args:
        markdown: The markdown text to extract data from.
        prompt: (Optional) The prompt to use for the LLM. If not provided, a default prompt will be used.
    Returns:
        list: A list of additional camps extracted from the markdown.

    """
    snippet = markdown
    OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
    if not prompt:
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
        - address
        - start_date
        - end_date
        - ages
        - cost

        Text:
        {snippet}

        Return only this format:
        {{"event_name":"", "address": "", "start_date": "", "end_date": "", "ages": "", "cost": ""}}

        Even if the text does not contain all fields, return an empty string for those fields. Do not return any other text or explanation, just the JSON.
        """

    headers_llm = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json"
    }
    payload = {
        "model": "deepseek/deepseek-r1-0528-qwen3-8b:free",
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.2
    }

    addl_camps = []
    camp = {
        "Event Details": "",
        "address": "",
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
