import hashlib

import pandas as pd
import main as func

artist_names_skipped = []


def main():
    # Load DataFrame from CSV
    path = r'..\Source\Artists\artists_data.csv'
    df = pd.read_csv(r'..\Source\Artists\artists_data_skipped.csv')
    artists_data = []
    indexer = 1
    # Iterate through each row
    for index, row in df.iterrows():
        artist_name = row['Artist_Name']  # Convert artist name to lowercase for URL
        artist_name_lower = artist_name.lower()
        # Check if birth date or start of career is missing
        # if pd.isnull(row['Birth_Date']) or pd.isnull(row['Carrier_Start_Date']):
        # Retrieve artist info
        birth_date, carrier_start_date = func.get_artist_info(artist_name_lower)

        # Update DataFrame with obtained values
        if birth_date is not None:
            df.at[index, 'Birth_Date'] = birth_date
        if carrier_start_date is not None:
            df.at[index, 'Carrier_Start_Date'] = carrier_start_date

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

    df = pd.DataFrame(artists_data)

    print("Saving file...")
    # Save DataFrame to CSV
    df.set_index('Hashed_Artist_Name', inplace=True)
    path = r'..\Source\Artists\artists_data.csv'
    df.to_csv(path, mode="a", index=True)
    print(f"File updated and saved at: {path}")

    if len(artist_names_skipped) > 0:
        print("Saving file with skipped artist names due to connection errors..")
        df_skipped = pd.DataFrame(artist_names_skipped)
        path_skipped = r'..\Source\Artists\artists_data_skipped.csv'
        df_skipped.set_index('ArtistName', inplace=True)
        df_skipped.to_csv(path_skipped, mode='a', header=False)
        print(f"File saved at: {path_skipped}")


if __name__ == "__main__":
    main()
