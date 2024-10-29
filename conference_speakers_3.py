import logging
import pandas as pd

from dotenv import load_dotenv

load_dotenv(override=True)

from utils.openai_utils import get_openai_filtered_profiles
from models.phantom.SearchExportScraper import SearchExportScraper
from utils.database_utils import (
    update_conf_speakers,
    create_database_session_and_engine,
)

logger = logging.getLogger(__name__)


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(message)s",
        datefmt="%d-%m-%Y %H:%M:%S",
    )


def get_data():
    """
    Fetch data from the Phantom Buster Search Export.

    Returns:
        pd.DataFrame: DataFrame containing the scraped data of users and their LinkedIn URLs.
    """
    se = SearchExportScraper()
    df = se.get_scraped_data()

    if df.empty:
        logger.warning("No data fetched from Search Export.")

    return df


def _find_one_to_one_matches(df):
    """
    Identify 1:1 matches where one person has one LinkedIn URL.

    Args:
        df (pd.DataFrame): DataFrame containing user data.

    Returns:
        pd.DataFrame: DataFrame with matched records for 1:1 connections.
    """
    logger.info("Searching for 1:1 matches...")
    grouped_df = df.groupby("query")["linkedin_url"].nunique().reset_index()
    one_to_one_matches_df = grouped_df[grouped_df["linkedin_url"] == 1].reset_index(
        drop=True
    )

    logger.info(f"Found {len(one_to_one_matches_df)} 1:1 matches.")

    result_df = pd.merge(
        df,
        one_to_one_matches_df.drop(columns=["linkedin_url"]),
        on="query",
        how="inner",
    )

    result_df = result_df[["query", "linkedin_url"]]
    return result_df


def _find_one_to_many_matches(df):
    """
    Identify 1:n matches where multiple LinkedIn URLs exist for the same name.

    Args:
        df (pd.DataFrame): DataFrame containing user data.

    Returns:
        pd.DataFrame: DataFrame with matched records for 1:n connections.
    """
    logger.info("Searching for 1:n matches...")
    grouped = df.groupby("query")
    result_rows = []

    for _, group in grouped:
        if len(group) > 1:  # If there are multiple speakers with the same name
            priority_order = ["1st", "2nd", "3rd"]

            for degree in priority_order:
                filtered_group = group[group["connectionDegree"] == degree]

                if len(filtered_group) == 1:
                    query = filtered_group.iloc[0]["query"]
                    linkedin_url = filtered_group.iloc[0]["linkedin_url"]
                    result_rows.append(
                        {
                            "query": query,
                            "linkedin_url": linkedin_url,
                        }
                    )
                    logger.info(
                        f"Single match found for query '{query}' with linkedin url '{linkedin_url}'."
                    )
                    break
                elif len(filtered_group) > 1:
                    query = filtered_group.iloc[0]["query"]
                    csv_data_json = filtered_group.to_dict(orient="records")

                    linkedin_url = get_openai_filtered_profiles(csv_data_json)
                    result_rows.append({"query": query, "linkedin_url": linkedin_url})
                    logger.info(
                        f"Multiple matches for query '{query}' - selected LinkedIn URL from OpenAI. - {linkedin_url}"
                    )
                    break
    result_df = pd.DataFrame(result_rows).reset_index(drop=True)
    logger.info(f"Total matches found: {len(result_df)}.")
    return result_df


def main():
    """Main function to execute the script logic."""
    setup_logging()

    session, _ = create_database_session_and_engine()

    try:
        df = get_data()

        if df.empty:
            logger.info("Everything is up-to-date. No records to process.")
            return

        logger.info(f"Finding LinkedIn URLs for {len(df['query'].unique())} speakers.")
        one_to_one_matches_df = _find_one_to_one_matches(df.copy())
        one_to_many_matches_df = _find_one_to_many_matches(df.copy())

        # Concatenate results from both match types
        df = pd.concat(
            [one_to_one_matches_df, one_to_many_matches_df], ignore_index=True
        )

        if not df.empty:
            update_conf_speakers(df=df, session=session, column_name="query")
            logger.info("Conference speakers updated successfully.")
        else:
            logger.info("No valid matches found to update.")
    except Exception as e:
        logger.error(f"An error occurred in the main process: {str(e)}")


if __name__ == "__main__":
    main()
