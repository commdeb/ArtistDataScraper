import pandas as pd
import main as func

artist_names_skipped = []
def main():
    # Load DataFrame from CSV
    path = r'..\Source\Artists\artists_data.csv'
    df = pd.read_csv(path, index_col='Hashed_Artist_Name')

    # Iterate through each row
    for index, row in df.iterrows():
        artist_name = row['Artist_Name'].lower()  # Convert artist name to lowercase for URL

        # Check if birth date or start of career is missing
        if pd.isnull(row['Birth_Date']) or pd.isnull(row['Carrier_Start_Date']):
            # Retrieve artist info
            birth_date, carrier_start_date = func.get_artist_info(artist_name)

            # Update DataFrame with obtained values
            if birth_date is not None:
                df.at[index, 'Birth_Date'] = birth_date
            if carrier_start_date is not None:
                df.at[index, 'Carrier_Start_Date'] = carrier_start_date

    # Save changes to the CSV file
    df.to_csv(path)
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
