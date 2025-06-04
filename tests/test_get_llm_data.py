import ast
import json
import types
from pathlib import Path


def load_get_llm_data(openrouter_response):
    """Load get_llm_data from camp_scraper.py with stub dependencies."""
    source = Path("camp_scraper.py").read_text()
    tree = ast.parse(source)
    node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "get_llm_data")
    mod = ast.Module(body=[node], type_ignores=[])
    code = compile(mod, filename="camp_scraper.py", mode="exec")

    class DummyTag:
        def __init__(self, text):
            self.text = text

    class DummySoup:
        def __init__(self, text):
            self.text = text

        def find_all(self, names):
            import re
            pattern = re.compile(r"<(?:p|li|div)[^>]*>(.*?)</(?:p|li|div)>", re.S | re.I)
            return [DummyTag(m.group(1)) for m in pattern.finditer(self.text)]

    def bs(text, parser):
        return DummySoup(text)

    class DummyRequests:
        def __init__(self, resp):
            self.resp = resp

        def post(self, *args, **kwargs):
            return types.SimpleNamespace(json=lambda: self.resp)

    namespace = {
        "BeautifulSoup": bs,
        "requests": DummyRequests(openrouter_response),
        "json": json,
        "copy": __import__("copy"),
        "os": __import__("os"),
    }
    exec(code, namespace)
    return namespace["get_llm_data"]


def test_valid_json_parses_fields():
    valid = "[{\"event_name\": \"Test Camp\", \"start_date\": \"2024-06-01\", \"end_date\": \"2024-06-03\", \"ages\": \"10-18\", \"cost\": \"$100\"}]"
    func = load_get_llm_data({"choices": [{"message": {"content": valid}}]})

    response = types.SimpleNamespace(text="<p>camp info</p>")
    camp = {
        "Camp Info URL": "http://example.com",
        "Camp Found?": "",
        "Event Details": "",
        "start_date": "",
        "end_date": "",
        "Ages / Grade Level": "",
        "Cost": "",
    }
    addl = func(response, camp)
    assert addl == []
    assert camp["Camp Found?"] == "Yes"
    assert camp["Event Details"] == "Test Camp"
    assert camp["start_date"] == "2024-06-01"
    assert camp["end_date"] == "2024-06-03"
    assert camp["Ages / Grade Level"] == "10-18"
    assert camp["Cost"] == "$100"


def test_malformed_json_returns_empty():
    malformed = "[{'event_name':'Camp'}]"
    func = load_get_llm_data({"choices": [{"message": {"content": malformed}}]})

    response = types.SimpleNamespace(text="<p>camp info</p>")
    camp = {
        "Camp Info URL": "http://example.com",
        "Camp Found?": "",
        "Event Details": "",
        "start_date": "",
        "end_date": "",
        "Ages / Grade Level": "",
        "Cost": "",
    }
    addl = func(response, camp)
    assert addl == []
    assert camp["start_date"] == "LLM Error"
    assert camp["end_date"] == "LLM Error"
    assert camp["Ages / Grade Level"] == "LLM Error"
    assert camp["Cost"] == "LLM Error"

