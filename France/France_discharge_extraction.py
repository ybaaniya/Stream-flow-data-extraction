#import all packages
import requests
import pandas as pd
import json
import os
#Read the csv file that contain the station code
df = pd.read_csv('path/of/station/csv/provided/in/content/of/hydroshare')
# Base URL for daily mean discharge
base_url = "https://hubeau.eaufrance.fr/api/v1/hydrometrie/obs_elab?grandeur_hydro_elab=QmJ&fields=date_obs_elab,resultat_obs_elab&cursor=&size=20000&code_entite="

# Directory to save CSV files
output_dir = 'path/to/save/csv'
os.makedirs(output_dir, exist_ok=True)

for index, row in df.iterrows():
    # Strip whitespace from code_station if it exists, otherwise set it to None
    code_station = row['code_station'].strip() if row['code_station'] else None

    # Skip the row if code_station is not valid
    if not code_station:
        continue

    # Replace the code_entite in the URL
    url = f"{base_url}{code_station}"

    # Make the API request
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        # Check if the JSON response is not empty
        if data.get('data'):
            print(f"The station '{row['code_site']}' has a non-empty JSON response.")

            # Extract the data into a DataFrame
            obs_df = pd.DataFrame(data['data'])

            # Define the CSV file path
            csv_file_path = os.path.join(output_dir, f"{code_station}.csv")

            # Save the DataFrame to a CSV file
            obs_df.to_csv(csv_file_path, index=False)

            print(f"Data saved for station '{row['libelle_station']}' to '{csv_file_path}'")
        else:
            print(f"The station '{row['libelle_station']}' has an empty JSON response.")
    else:
        print(f"Failed to retrieve data for station '{row['libelle_station']}'.")

print(f"\nTotal stations processed: {len(df)}")
