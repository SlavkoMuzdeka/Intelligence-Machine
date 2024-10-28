import os
import time
import base64
import logging
import pandas as pd

from models.database.models import Conference
from utils.database_utils import normalize_name
from utils.database_utils import create_database_session_and_engine
from utils.openai_utils import (
    image_to_text,
    scrape_data_scrapegraphai,
    parse_speakers_from_yt_titles,
)

logger = logging.getLogger(__name__)


def get_db_conferences() -> pd.DataFrame:
    """
    Retrieves all conferences (name and year) from the database.

    Returns:
        pd.DataFrame: DataFrame containing conference names and years.
    """
    try:
        session, _ = create_database_session_and_engine()
        conferences = session.query(Conference).all()
        data = [(conf.name, conf.year) for conf in conferences]
        df = pd.DataFrame(data, columns=["conf_name", "conf_year"])
        return df
    except Exception as e:
        logger.error(f"Error fetching conferences from the database: {e}")
        raise


def scrape_page_with_scrapegraph_ai(source: str) -> pd.DataFrame:
    """
    Scrapes the speakers page using ScrapeGraph AI and returns the extracted speaker data.

    Args:
        source (str): URL of the speakers page.

    Returns:
        pd.DataFrame: DataFrame containing speakers' names, URLs, and companies.
    """
    try:
        logger.info(f"Scraping speakers page from {source} using ScrapeGraph AI...")

        if source == "-":
            logger.info("There is no speakers page URL...")
            return pd.DataFrame()

        # Prompt for extracting speakers and their website URLs
        prompt = """
        List speakers with their website URL, and the company they work for.
        Return the result strictly as a valid JSON object with the following structure:
        {
            "speakers": [
                {"speaker_name": "speaker_name", "website_url": "website_url", "company": "company_name"}
            ]
        }.
        If the website URL does not start with http, leave the website_url field empty.
        Remove any brackets or special characters from the speaker_name and ensure it contains only the speaker's name.
        Ensure the company name represents a real organization; if unsure, leave the company field as None.
        Make sure the JSON object is complete and valid.
        """
        result = scrape_data_scrapegraphai(prompt=prompt, source=source)

        if "speakers" in result:
            if len(result.get("speakers")) > 0:
                df = pd.DataFrame(result.get("speakers"))
                df["norm_name"] = df["speaker_name"].apply(normalize_name)
                logger.info("Speakers page scraped successfully...")
                return df
            else:
                logger.info("There are no speakers scraped from speakers page URL...")

        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error scraping speakers page from {source}: {e}")
        raise


def interpret_agenda_screenshots(conf_name: str, conf_year: int) -> pd.DataFrame:
    """
    Scrapes speaker data from agenda screenshots and returns extracted speaker info.

    Args:
        conf_name (str): Name of the conference.
        conf_year (int): Year of the conference.

    Returns:
        pd.DataFrame: DataFrame containing speaker names from agenda screenshots.
    """
    try:
        logger.info("Scraping speakers talks from agenda screenshoots...")
        agenda_speakers_dfs = []
        images_path = os.path.join(
            os.getcwd(), "Data", "images", f"{conf_name}_{conf_year}"
        )

        if not os.path.exists(images_path):
            logger.info(f"Directory {images_path} does not exist.")
            return pd.DataFrame()

        for image_name in os.listdir(images_path):
            image_path = os.path.join(images_path, image_name)
            logger.info(f"Scraping data from image {os.path.relpath(image_path)}")
            encoded_image = _encode_image(image_path)

            df = image_to_text(encoded_image)
            agenda_speakers_dfs.append(df)
            time.sleep(1)

        if agenda_speakers_dfs:
            df = pd.concat(agenda_speakers_dfs, ignore_index=True)
            df["norm_name"] = df["speaker_name"].apply(normalize_name)
            logger.info(
                "Speakers talks scraped successfully from agenda screenshoots..."
            )
            new_df = _filter_df(df.copy())
            return new_df
        return pd.DataFrame()
    except Exception as e:
        logger.error(
            f"Error interpreting agenda screenshots for {conf_name} ({conf_year}): {e}"
        )
        raise


def process_titles_in_chunks(titles_list: list[str], chunk_size: int) -> pd.DataFrame:
    """
    Processes video titles in chunks to extract speaker information.

    Args:
        titles_list (list[str]): A list of YouTube video titles.
        chunk_size (int): The number of titles to process in each chunk.

    Returns:
        pandas.DataFrame: A DataFrame containing the speakers' names and talk titles.
    """
    try:
        speakers_dfs = []

        for i in range(0, len(titles_list), chunk_size):
            chunk = titles_list[i : i + chunk_size]
            video_titles = "\n".join(
                [f"{i+1}. {title}" for i, title in enumerate(chunk)]
            )

            speakers_df = parse_speakers_from_yt_titles(video_titles)
            speakers_dfs.append(speakers_df)
            logger.info(f"Processed {len(chunk)} of {len(titles_list)} video titles.")

        all_speakers_df = pd.concat(speakers_dfs, ignore_index=True)
        return all_speakers_df
    except Exception as e:
        logger.error(f"Error processing video titles in chunks: {e}")
        raise


def merge_website_df_with_youtube_df(
    website_df: pd.DataFrame, youtube_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Merges speakers data from the website and YouTube sources.

    Args:
        website_df (pd.DataFrame): DataFrame containing speakers from website.
        youtube_df (pd.DataFrame): DataFrame containing speakers from YouTube.

    Returns:
        pd.DataFrame: Merged DataFrame with speaker names, companies, and talk titles.
    """
    try:
        if website_df.empty and youtube_df.empty:
            return pd.DataFrame()
        elif website_df.empty:
            merged_df = youtube_df
        elif youtube_df.empty:
            merged_df = website_df
        else:
            not_na_df = website_df[website_df["talk_title"].notna()].reset_index(
                drop=True
            )
            na_df = website_df[website_df["talk_title"].isna()].reset_index(drop=True)

            for idx, row in na_df.iterrows():
                match = youtube_df[youtube_df["speaker_name"] == row["speaker_name"]]
                if not match.empty:
                    na_df.at[idx, "talk_title"] = match["talk_title"].values[0]

            merged_df = pd.concat([not_na_df, na_df], ignore_index=True)

        if "company" not in list(merged_df.columns):
            merged_df["company"] = None

        return merged_df
    except Exception as e:
        logger.error(f"Error merging website and YouTube data: {e}")
        raise


def _encode_image(image_path: str) -> str:
    """
    Encodes an image file as a base64 string.

    Args:
        image_path (str): Path to the image file.

    Returns:
        str: Base64-encoded image.
    """
    try:
        with open(image_path, "rb") as image_file:
            return base64.b64encode(image_file.read()).decode("utf-8")
    except Exception as e:
        logger.error(f"Error encoding image at {image_path}: {e}")
        raise


def _filter_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filters out irrelevant company names from the DataFrame.

    Args:
        df (pd.DataFrame): DataFrame with speakers' data.

    Returns:
        pd.DataFrame: Filtered DataFrame with cleaned company names.
    """
    try:
        filters = [
            "Infrastructure and Scalability",
            "Financial Freedom",
            "Mass Adoption",
            "Security and Resilience",
            "dappilon",
            "Public Goods",
            "Main Stage",
            "Stage 2",
            "Stage 3",
            "Security",
            "Miscellaneous",
            "L2s, Bridges & Scalability",
            "Identity & Privacy",
            "DeFi",
            "Account Abstraction",
            "SOLARPUNK VISIONS",
            "SOCIETAL CHALLENGES",
            "CORE & EVM",
            "MISCELLANEOUS",
            "DEVELOPER ECOSYSTEM & TOOLING",
            "COMMUNITY PAIN POINTS",
            "DEFI IRL",
        ]
        df.loc[df["company"].isin(filters), "company"] = None
        return df
    except Exception as e:
        logger.error(f"Error filtering DataFrame: {e}")
        raise
