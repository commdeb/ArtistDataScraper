import re
import time
import pandas as pd
import hashlib
import requests
from bs4 import BeautifulSoup
import pyodbc

artist_names_skipped = []


# Function to get artist birth date from Wikipedia
def get_artist_info(artist_name):
    # Initialize variables to store scraped data
    birth_date = None
    start_of_career = None
    print(f"Started for: {artist_name}")
    # Search for artist on Wikipedia
    url = f"https://en.wikipedia.org/wiki/{artist_name}"
    try:
        response = requests.get(url, timeout=10)  # Set timeout to 10 seconds
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')

            # Extract birth date if available
            birth_date_element = soup.find('span', {'class': 'bday'})
            if birth_date_element:
                birth_date = birth_date_element.text

            print(f"Obtained birthdate: {birth_date}")

            # Search for career-related keywords
            career_keywords = ['begin', 'start', 'debut']
            for keyword in career_keywords:
                career_section = soup.find('span', string=re.compile(keyword, flags=re.IGNORECASE))
                if career_section:
                    start_of_career_element = career_section.find_next('span', {'class': 'bday'})
                    if start_of_career_element:
                        start_of_career = start_of_career_element.text
                        break

            print(f"Obtained start_of_career: {start_of_career}")
            print("------------------------------------------------------------")
    except requests.exceptions.Timeout:
        print(f"Timeout occurred for {artist_name}. Skipping...")
        artist_names_skipped.append({'ArtistName': artist_name})
        time.sleep(1)  # Add a delay before continuing
    except Exception as e:
        print(f"Error occurred for {artist_name}: {str(e)}")

    return birth_date, start_of_career


# Function to retrieve artist names from MSSQL database
def get_artist_names_from_mssql():
    # Connect to MSSQL database
    conn = pyodbc.connect(
        'DRIVER={SQL Server};SERVER=localhost\HURTDANYCH2;DATABASE=TEMP_SpotifyCharts;UID=PyDeamon;PWD=P@ssw0rdPython')
    cursor = conn.cursor()
    # Execute query to get artist names
    cursor.execute("SELECT ArtistName FROM Temp_Artist")
    artist_names = [row[0] for row in cursor.fetchall()]
    print("Successfully obtain data from connection")
    conn.close()
    return artist_names


def main():
    # Get artist names from MSSQL
    artist_names = get_artist_names_from_mssql()

    # Get artists data
    artists_data = []
    indexer = 1
    print("Started data scrapping...")
    for artist_name in artist_names:
        # Get birth date from Wikipedia
        birth_date, carrier_start_date = get_artist_info(artist_name)
        if birth_date or carrier_start_date:
            # Hash artist name
            artist_hash = hashlib.sha256(artist_name.encode()).hexdigest()
            artists_data.append({
                'Index': indexer,
                'Hashed_Artist_Name': artist_hash,
                'Artist_Name': artist_name,
                'Birth_Date': birth_date,
                'Carrier_Start_Date': carrier_start_date
            })
            indexer += 1

    print("Finished data scrapping")
    # Create DataFrame
    df = pd.DataFrame(artists_data)

    print("Saving file...")
    # Save DataFrame to CSV
    df.set_index('Hashed_Artist_Name', inplace=True)
    path = r'..\Source\Artists\artists_data.csv'
    df.to_csv(path)
    print(f"file saved at: {path}")

    if len(artist_names_skipped) > 0:
        print("Saving file with skipped artist name due to conn errors..")
        df = pd.DataFrame(artist_names_skipped)
        path = r'..\Source\Artists\artists_data_skipped.csv'
        df.set_index('Artist_Name', inplace=True)
        df.to_csv(path)
        print(f"file saved at: {path}")


if __name__ == "__main__":
    main()