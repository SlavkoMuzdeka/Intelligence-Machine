import logging
import pandas as pd

from dotenv import load_dotenv

load_dotenv(override=True)

from models.database.models import Speaker
from utils.google_sheets_utils import upload_data_to_gs
from utils.database_utils import (
    get_speakers,
    update_speakers,
    get_linkedIn_users,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(message)s",
    datefmt="%d-%m-%Y %H:%M:%S",
)

logger = logging.getLogger(__name__)
SHEET_ID_KEY = "CONF_LIST_SHEET_ID"
SHEET_NAME_KEY = "CONF_SPEAKERS_WITH_MISSING_LINKEDIN_URL_SHEET_NAME"


def match_conf_speakers_with_linkedIn_users(conf_speakers_df, linkedIn_users_df):
    """
    Match conference speakers with LinkedIn users by normalized name to retrieve LinkedIn URLs.

    Args:
    - conf_speakers_df (pd.DataFrame): DataFrame containing conference speakers data.
    - linkedin_users_df (pd.DataFrame): DataFrame containing LinkedIn users data.

    Returns:
    - pd.DataFrame: Merged DataFrame with matched LinkedIn URLs.
    """
    if linkedIn_users_df.empty:
        logging.info("There are no LinkedIn users in database...")
        return pd.DataFrame()

    logger.info("Matching conference speakers with LinkedIn users...")

    # Merge on normalized names
    merged_df = pd.merge(
        conf_speakers_df.drop(columns=["linkedin_url"]),
        linkedIn_users_df.drop(columns=["name"]),
        how="left",
        on="norm_name",
    )
    # Return only speakers with matched LinkedIn URLs
    return merged_df[merged_df["linkedin_url"].notna()].reset_index(drop=True)


def main():
    """
    Main function that loads conference speakers without LinkedIn URLs,
    attempts to find their LinkedIn URLs from a database of LinkedIn users,
    and updates the speakers' information in the database.
    """
    logger.info("Script is running...")
    try:
        # Step 1: Load conference speakers without LinkedIn URLs
        conf_speakers_df = get_speakers(
            filter_condition=(Speaker.linkedIn_url.is_(None))
        )
        logger.info(
            f"Conference speakers without linkedin url - total {len(conf_speakers_df)}"
        )

        # Step 2: Load LinkedIn users (company employees)
        linkedIn_users_df = get_linkedIn_users()

        # Step 3: Match conference speakers with LinkedIn users
        matched_df = match_conf_speakers_with_linkedIn_users(
            conf_speakers_df.copy(), linkedIn_users_df.copy()
        )

        # Step 4: Update speakers' LinkedIn URLs if matches found
        if not matched_df.empty:
            logger.info(f"Find {len(matched_df)} speakers LinkedIn URLs...")
            update_speakers(matched_df)

            conf_speakers_df = get_speakers(
                filter_condition=(Speaker.linkedIn_url.is_(None))
            )
        else:
            logger.info("There is no matched data")

        conf_speakers_df = conf_speakers_df[["name"]]
        upload_data_to_gs(conf_speakers_df, SHEET_ID_KEY, SHEET_NAME_KEY)

    except Exception as e:
        logger.error(f"An error occurred in the main process: {str(e)}")


if __name__ == "__main__":
    main()
