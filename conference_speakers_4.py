import logging
import pandas as pd

from dotenv import load_dotenv
from utils.google_sheets_utils import upload_data_to_gs
from utils.database_utils import (
    get_talks,
    get_speakers,
)

load_dotenv(override=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(message)s",
    datefmt="%d-%m-%Y %H:%M:%S",
)

logger = logging.getLogger(__name__)
SHEET_ID_KEY = "CONF_LIST_SHEET_ID"
SHEET_NAME_KEY = "CONF_LIST_OF_SPEAKERS_AND_TOPICS_SHEET_NAME"


def find_speaker_talks(speakers_df, talks_df):
    """
    Merges speakers and talks data on normalized names, aggregates talks by speaker,
    and creates a new DataFrame with unique talk and conference information.

    Args:
        speakers_df (pd.DataFrame): DataFrame containing speaker data.
        talks_df (pd.DataFrame): DataFrame containing conference talk data.

    Returns:
        pd.DataFrame: DataFrame containing aggregated speaker talks and conference details.
    """
    try:
        logging.info("Merging speaker and conference talks data...")
        merged_df = pd.merge(speakers_df, talks_df, on="norm_name", how="left")

        grouped = (
            merged_df.groupby("norm_name")
            .agg(
                {
                    "name": "first",
                    "website_url": "first",
                    "linkedin_url": "first",
                    "talk_title": lambda x: list(x),  # List of talks
                    "conference_name": lambda x: list(x),  # List of conferences
                    "conference_year": lambda x: list(x),  # List of years
                }
            )
            .reset_index()
        )

        logging.info("Aggregating speaker talks and conferences...")
        final_df = _build_speaker_talks_df(grouped)
        return final_df
    except Exception as e:
        logging.error(f"An error occurred while finding speaker talks: {e}")
        raise


def _build_speaker_talks_df(grouped):
    """
    Builds a DataFrame of speaker talks by iterating through grouped data and ensuring uniqueness of talks.

    Args:
        grouped (pd.DataFrame): Grouped DataFrame containing speaker and talk information.

    Returns:
        pd.DataFrame: DataFrame containing the final format of speaker and talk details.
    """
    logging.info("Building the final speaker-talker DataFrame...")
    final_df = pd.DataFrame()

    for _, row in grouped.iterrows():
        row_data = {
            "Name": row["name"],
            "Website URL": row["website_url"],
            "LinkedIn URL": row["linkedin_url"],
        }

        unique_talks = set()

        idx = 1
        for talk, conf, year in zip(
            row["talk_title"], row["conference_name"], row["conference_year"]
        ):
            if (talk, conf, year) not in unique_talks:
                unique_talks.add((talk, conf, year))
                row_data[f"conference_{idx}"] = f"{conf}_{year}"
                row_data[f"talk_{idx}"] = talk
                idx += 1

        final_df = final_df._append(row_data, ignore_index=True)

    logging.info("Completed building the final DataFrame.")
    return final_df


def main():
    """
    Main function to fetch speaker and talk data, find talks for each speaker,
    and output the result to a CSV file.
    """
    logger.info("Script is running...")
    try:
        speakers_df = get_speakers()
        conf_talks_df = get_talks()

        if speakers_df.empty or conf_talks_df.empty:
            logging.warning("No data available to process.")
            return

        speaker_talks_df = find_speaker_talks(speakers_df, conf_talks_df)
        upload_data_to_gs(speaker_talks_df, SHEET_ID_KEY, SHEET_NAME_KEY)
    except Exception as e:
        logging.error(f"An error occurred in the main function: {e}")


if __name__ == "__main__":
    main()
