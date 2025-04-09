import requests
import json
import time
import os

def get_waze_data():
    # URL with the parameters for the Waze API
    url = "https://www.waze.com/live-map/api/georss"
    
    # Parameters for the request
    params = {
        'top': -33.420554365669666,
        'bottom': -33.47727360466537,
        'left': -70.69968223571779,
        'right': -70.63891410827638,
        'env': 'row',
        'types': 'alerts,traffic,users'
    }
    
    # Adding headers to mimic a browser request
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        # Make the GET request
        response = requests.get(url, params=params, headers=headers)
        
        # Check if the request was successful
        if response.status_code == 200:
            # Parse the JSON response
            data = response.json()
            
            # Pretty print the JSON data
            print(json.dumps(data, indent=4))
            
            # Alternatively, you can process specific parts of the data
            if 'alerts' in data:
                print(f"\nNumber of alerts: {len(data['alerts'])}")
            if 'jams' in data:
                print(f"Number of traffic jams: {len(data['jams'])}")
            if 'users' in data:
                print(f"Number of users: {len(data['users'])}")
                
            return data
        else:
            print(f"Request failed with status code: {response.status_code}")
            print(f"Response: {response.text}")
            return None
            
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

if __name__ == "__main__":
    print("Fetching Waze data...")
    data = get_waze_data()
    
    # Create 'jsons' directory if it doesn't exist
    jsons_dir = 'jsons'
    if not os.path.exists(jsons_dir):
        os.makedirs(jsons_dir)
        print(f"Created directory: {jsons_dir}")
    
    # Save the data to a file in the 'jsons' folder
    if data:
        # Get current timestamp for unique filename
        timestamp = time.strftime("%Y%m%d-%H%M%S")
        filename = f"{jsons_dir}/waze_data_{timestamp}.json"
        
        with open(filename, 'w') as f:
            json.dump(data, f, indent=4)
        print(f"\nData has been saved to {filename}")