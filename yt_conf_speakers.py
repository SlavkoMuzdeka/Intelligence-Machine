import os
import pandas as pd

from pytube import Playlist
from dotenv import load_dotenv
from utils.google_sheets_utils import get_conferences_urls
from utils.openai_utils import parse_speakers_from_yt_titles

load_dotenv()


def get_speakers_from_yt_playlists() -> pd.DataFrame:
    """
    Fetches speaker information from YouTube playlists of conferences and returns a DataFrame.

    The function processes video titles from YouTube playlists found in a Google Sheet,
    filters out conferences already processed, and then extracts the speaker names and 
    talk titles from the video titles. It concatenates new data with existing data if present.

    Returns:
        pandas.DataFrame: A DataFrame containing speaker names, talk titles, and conference names.
    """
    # Fetch Google Sheets data containing conference URLs
    google_sheets_df = get_conferences_urls()

    google_sheets_df = google_sheets_df[
        google_sheets_df["YouTube Playlist URL"] != ""
    ].reset_index(drop=True)

    try:
        # Filter out conferences already processed (i.e., present in the CSV)
        yt_df_path = os.path.join(os.getcwd(), "Data", "all_speakers_yt.csv")
        yt_df = pd.read_csv(yt_df_path)

        conf_already_processed = list(yt_df["conf_name"].unique())
        google_sheets_df = google_sheets_df[
            ~google_sheets_df["Name"].isin(conf_already_processed)
        ].reset_index(drop=True)

        # If no new conferences, return the existing dataframe
        if len(google_sheets_df) == 0:
            return yt_df
    except FileNotFoundError:
        pass    # Proceed if no prior data exists

    speakers_dfs = []

    # Process each conference to extract video titles and speakers
    for _, row in google_sheets_df.iterrows():
        print(f"Finding videos for {row["Name"]}")

        # Fetch the YouTube playlist for the conference
        playlist = Playlist(row["YouTube Playlist URL"])

        # Extract the titles of the videos in the playlist
        video_titles = [v.title for v in playlist.videos]

        print(f"There are {len(video_titles)} videos in playlist for {row['Name']} conference")

        # Process video titles in chunks and extract speakers
        speakers_df = _process_titles_in_chunks(video_titles, 100)
        speakers_df["conf_name"] = row["Name"]
        speakers_dfs.append(speakers_df)

    # Concatenate all new speakers data into one dataframe
    new_speakers_df = pd.concat(speakers_dfs, ignore_index=True)

    try:
        # Load existing data and concatenate it with new data
        yt_df = pd.read_csv(yt_df_path)
        full_df = pd.concat([yt_df, new_speakers_df], ignore_index=True)
    except FileNotFoundError:
        # If no existing data, just use the new dataframe
        full_df = new_speakers_df 

    return full_df


def _process_titles_in_chunks(titles_list: list[str], chunk_size: int) -> pd.DataFrame:
    """
    Processes video titles in chunks to extract speaker information.

    Args:
        titles_list (list[str]): A list of YouTube video titles.
        chunk_size (int): The number of titles to process in each chunk.

    Returns:
        pandas.DataFrame: A DataFrame containing the speakers' names and talk titles.
    """
    speakers_dfs = []

    for i in range(0, len(titles_list), chunk_size):
        chunk = titles_list[i : i + chunk_size]
        print(f"Chunk is {len(chunk)}")

        video_titles = "\n".join([f"{i+1}. {title}" for i, title in enumerate(chunk)])

        speakers_df = parse_speakers_from_yt_titles(video_titles)
        speakers_dfs.append(speakers_df)

    all_speakers_df = pd.concat(speakers_dfs, ignore_index=True)
    return all_speakers_df
