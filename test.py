import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import random

# Desired column order
DESIRED_COLUMNS = [
    "licenceId", "licenceNumber", "licenceType", "licenceTypeFriendly", "licenceGroup", "licenceName", "status", 
    "granted", "expires", "term", "licensee", "licenseeType", "suburb", "state", "postcode", "addressType", 
    "ABN", "ACN", "formattedABN", "formattedACN", "classes", "associatedParties", "complianceSummary", 
    "regionsOfOperation", "history", "classCodes", "hasVariousClassExpiries"
]

# Function to get component data for a specific license
def getComonentData(licenseId, licenceType, proxy):
    # Define the URL
    url = f"https://verify.licence.nsw.gov.au/publicregisterapi/api/v1/licence/search/details/{licenceType}/{licenseId}"
    headers = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "en-US,en;q=0.9",
        "referer": f"https://verify.licence.nsw.gov.au/details/{licenceType}/{licenseId}",
        "sec-ch-ua": "\"Chromium\";v=\"130\", \"Google Chrome\";v=\"130\", \"Not?A_Brand\";v=\"99\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\"",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
        "accept-encoding": "gzip, deflate, br, zstd",
    }
    
    for attempt in range(5):  # Retry up to 5 times
        response = requests.get(url, headers=headers, proxies=proxy)

        if response.status_code == 200:
            # Parse JSON response if request was successful
            data = response.json()
            combined_data = pd.json_normalize(data["componentData"])
            return combined_data
        elif response.status_code == 429:
            wait_time = random.randint(1, 5)
            print(f"Rate limited. Waiting for {wait_time} seconds before retrying...")
            time.sleep(wait_time)
        else:
            print(f"Failed to retrieve data for {licenseId}. Status code: {response.status_code}")
            return None

    print(f"Max retries exceeded for {licenseId}.")
    return None

# Function to get license IDs
def getLicenseID(licenceNumbers):
    url = "https://verify.licence.nsw.gov.au/publicregisterapi/api/v1/licence/search/bulk"
    headers = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "en-US,en;q=0.9",
        "content-type": "application/json",
        "origin": "https://verify.licence.nsw.gov.au",
        "referer": "https://verify.licence.nsw.gov.au/home/ADL/results?searchTerm=&licenceGroupCode=ADL&searchMultipleTerm=AD212846,AD211846&status=all&isMulti=true",
        "sec-ch-ua": "\"Chromium\";v=\"130\", \"Google Chrome\";v=\"130\", \"Not?A_Brand\";v=\"99\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\"",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    }

    payload = {
        "licenceGroup": "ADL",
        "licenceNumbers": licenceNumbers,
        "licenceStatuses": [],
        "pageNumber": 0,
        "pageSize": 200
    }

    response = requests.post(url, headers=headers, json=payload)

    if response.status_code == 200:
        data = response.json()
        return data["results"]
    else:
        print(f"Request failed with status code {response.status_code}")
        return []

# Function to save data to CSV file with reordered columns
def save_data_to_csv(data):
    data = data.reindex(columns=DESIRED_COLUMNS, fill_value="")  # Reorder and add missing columns
    data.to_csv("combined_data_all.csv", mode='a', header=not pd.io.common.file_exists("combined_data_all.csv"), index=False)

# Main function to fetch and save data
def fetchAndSaveData(start, end, step):
    license_ranges = [f"AD{200000 + no}" for no in range(start, end + 1)]
    proxies = [
        
    ]
    count = 0

    for i in range(0, len(license_ranges), 100):
        chunk = license_ranges[i:i + 100]
        print(chunk)
        try:
            results = getLicenseID(chunk)
            if results:
                for result in results:
                    licenceId = result['licenceId']
                    licenceType = result['licenceType']
                    proxy = {
                        'http': proxies[count%len(proxies)],
                        'https': proxies[count%len(proxies)]
                    }
                    combined_data = getComonentData(licenceId, licenceType, proxy)
                    if combined_data is not None:
                        count += 1
                        save_data_to_csv(combined_data)
                        print(f"Data retrieved and saved for {licenceId}, count={count}")
            else:
                print(f"No results for chunk starting at index {i}")
        except Exception as e:
            print(f"Error processing chunk: {e}", chunk)

# Fetch data for license IDs in the specified range with the given step
fetchAndSaveData(10000, 30000, 100)
