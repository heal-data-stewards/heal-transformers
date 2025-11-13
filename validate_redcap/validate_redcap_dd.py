import requests
import os
import json
from dotenv import load_dotenv
import sys

load_dotenv()

api_url = os.getenv("API_URL")
api_token = os.getenv("API_TOKEN")

if len(sys.argv) < 2:
    print("Usage: python script.py <path_to_redcap_csv>")
    sys.exit(1)

path_to_redcap_csv = sys.argv[1]

try:
    with open(path_to_redcap_csv, "r") as file:
        metadata_content = file.read()
except FileNotFoundError:
    print(f"Error: File not found at {path_to_redcap_csv}")
    sys.exit(1)
except Exception as e:
    print(f"Error reading file: {e}")
    sys.exit(1)

payload = {
    "token": api_token,
    "content": "metadata",
    "format": "csv",
    "data": metadata_content,
    "returnFormat": "json"
}

response = requests.post(api_url, data=payload)

if response.status_code == 200:
    response_data = response.json()
    print(f"DD is REDCap compliant (status: {str(response.status_code)})\nNumber of variables imported: {response_data}")
else:
    try:
        error_message = response.json().get("error", "")
        if error_message:
            errors = error_message.split("\n")
            print("Errors:")
            for error in errors:
                print(f"- {error.strip()}")
        else:
            print(f"Error: HTTP {response.status_code} - {response.text}")
    except json.JSONDecodeError:
        print(f"Error: HTTP {response.status_code} - Unable to parse response")
