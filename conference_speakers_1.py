import os
import logging
import pandas as pd

from dotenv import load_dotenv

load_dotenv(override=True)

from pytube import Playlist
from openai import RateLimitError
from utils.google_sheets_utils import get_gs_conferences
from utils.database_utils import (
    insert_conf_speakers,
    create_database_if_not_exists,
)
from utils.conference_speakers_utils import (
    get_db_conferences,
    process_titles_in_chunks,
    interpret_agenda_screenshots,
    scrape_page_with_scrapegraph_ai,
    merge_website_df_with_youtube_df,
)

logger = logging.getLogger(__name__)


def setup_logging():
    """
    Configures logging for the script.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(message)s",
        datefmt="%d-%m-%Y %H:%M:%S",
    )


def get_unscraped_conferences():
    """
    Fetches conferences that haven't been scraped yet by comparing the
    Google Sheets data with the database records.

    Returns:
        pd.DataFrame: DataFrame containing unscraped conferences.
    """
    try:
        # Get list of conferences from Google Sheets
        gs_confs_df = get_gs_conferences()

        # Get list of scraped conferences from database
        db_confs_df = get_db_conferences()
        db_confs_set = set(zip(db_confs_df["conf_name"], db_confs_df["conf_year"]))

        # Filter conferences that haven't been scraped
        filtered_df = gs_confs_df[
            ~gs_confs_df.apply(
                lambda row: (row["Name"], row["Year"]) in db_confs_set, axis=1
            )
        ]
        return filtered_df
    except Exception as e:
        logger.error(f"An error while fetching unscraped conferences: {e}")
        raise


def get_speakers_from_conf_website(row) -> pd.DataFrame:
    """
    Scrapes speakers' data from both the 'speakers page' and 'agenda page' of a conference.

    Args:
        row (pd.Series): Row containing conference information.

    Returns:
        pd.DataFrame: Merged DataFrame with scraped speaker data.
    """
    logger.info(f"Scraping speakers from website for {row['Name']} ({row['Year']})...")

    # Scrape speakers from the speakers page and agenda page
    speakers_page_df = scrape_page_with_scrapegraph_ai(source=row["Speakers URL"])
    agenda_page_df = interpret_agenda_screenshots(
        conf_name=row["Name"], conf_year=row["Year"]
    )

    # Merge the two dataframes
    if speakers_page_df.empty and agenda_page_df.empty:
        logger.info(f"No speaker data found for {row['Name']} ({row['Year']})")
        return pd.DataFrame()

    if speakers_page_df.empty:
        merged_df = agenda_page_df.copy()
    elif agenda_page_df.empty:
        merged_df = speakers_page_df.copy()
    else:
        # Data that exists on agenda page, but do not exist on 'main' page
        pom_df = agenda_page_df[
            ~agenda_page_df["norm_name"].isin(speakers_page_df["norm_name"].tolist())
        ]

        merged_df = pd.merge(
            left=speakers_page_df,
            right=agenda_page_df.drop(columns=["speaker_name", "company"]),
            how="left",
            on="norm_name",
        )

        merged_df = pd.concat([merged_df, pom_df], ignore_index=True)
        merged_df.drop(columns=["norm_name"], inplace=True)

    merged_df["conf_name"] = row["Name"]
    merged_df["conf_year"] = int(row["Year"])

    if "website_url" not in list(merged_df.columns):
        merged_df["website_url"] = None

    def extract_linkedin_url(row):
        if pd.notna(row["website_url"]) and "linkedin" in row["website_url"]:
            return row["website_url"]
        return None

    linkedIn_urls = merged_df.apply(extract_linkedin_url, axis=1)
    merged_df.insert(2, "linkedIn_url", linkedIn_urls)

    merged_df["website_url"] = merged_df["website_url"].apply(
        lambda x: None if pd.notna(x) and "linkedin" in x else x
    )

    # Save the scraped data to CSV
    data_path = os.path.join(
        os.getcwd(), "Data", "website", f"{row['Name']}_{row['Year']}.csv"
    )
    merged_df.to_csv(data_path, index=False)
    return merged_df


def get_speakers_from_yt_playlist(
    conf_name, conf_year, yt_playlist_url
) -> pd.DataFrame:
    """
    Fetches speaker information from YouTube playlist of conference and returns a DataFrame.

    The function processes video titles from YouTube playlist and extracts the speaker names
    and talk titles from the video titles.

    Returns:
        pandas.DataFrame: A DataFrame containing speaker names, talk titles, and conference name.
    """
    logger.info(f"Scraping YouTube Playlist for {conf_name} ({conf_year})...")

    if yt_playlist_url == "" or yt_playlist_url == "-":
        logger.info(f"No YouTube Playlist URL provided for {conf_name} ({conf_year}).")
        return pd.DataFrame()

    # Fetch the playlist
    playlist = Playlist(yt_playlist_url)
    try:
        video_titles = [v.title for v in playlist.videos]
        logger.info(f"Found {len(video_titles)} videos in the playlist.")
    except Exception as e:
        logger.error(
            f"Failed to scrape data from YouTube for {conf_name} ({conf_year}): {e}"
        )
        return pd.DataFrame()

    # Process video titles to extract speaker data
    speakers_df = process_titles_in_chunks(video_titles, 100)
    speakers_df["conf_name"] = conf_name
    speakers_df["conf_year"] = conf_year
    speakers_df["website_url"] = None
    speakers_df["linkedIn_url"] = None

    # Remove rows with missing speaker names
    speakers_df = speakers_df[speakers_df["speaker_name"].notna()].reset_index(
        drop=True
    )
    speakers_df = speakers_df[
        [
            "speaker_name",
            "website_url",
            "linkedIn_url",
            "talk_title",
            "conf_name",
            "conf_year",
        ]
    ]
    # Save the scraped data to CSV
    data_path = os.path.join(
        os.getcwd(), "Data", "yt", f"{conf_name}_{conf_year}_yt.csv"
    )
    speakers_df.to_csv(data_path, index=False)
    return speakers_df


def main():
    """
    Main function to scrape conference data from both websites and YouTube playlists,
    merge the data, and insert into the database.
    """
    setup_logging()

    create_database_if_not_exists()

    df = get_unscraped_conferences()

    if df.empty:
        logger.info("All conferences have been scraped. Nothing to process.")
        return

    for _, row in df.iterrows():
        try:
            conf_name = row["Name"]
            conf_year = row["Year"]

            if row["Name"] == "" and row["Year"] == "":
                logger.info(
                    f"You must provide name and year of conference (these fields in Google Sheet are required)..."
                )
                break

            logger.info(f"Processing {conf_name} ({conf_year})...")

            # Scrape speakers from the conference website
            website_df = get_speakers_from_conf_website(row)

            # Scrape speakers from the YouTube playlist
            youtube_df = get_speakers_from_yt_playlist(
                conf_name, conf_year, row["YouTube Playlist URL"]
            )

            # Merge the data from website and YouTube
            merged_df = merge_website_df_with_youtube_df(website_df, youtube_df)

            if not merged_df.empty:
                insert_conf_speakers(merged_df)
                logger.info(f"Inserted data for {conf_name} ({conf_year}).")
                logger.info(
                    f"Successfully scraped speakers from {conf_name} conference"
                )
            else:
                logger.info(f"No data found for {conf_name} ({conf_year}).")
        except RateLimitError as rle:
            logger.error(
                f"Rate limit error while processing {conf_name} ({conf_year}): {rle}"
            )
        except Exception as e:
            logger.error(f"Error while processing {conf_name} ({conf_year}): {e}")


if __name__ == "__main__":
    main()
