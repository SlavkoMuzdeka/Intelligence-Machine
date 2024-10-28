import os
import gspread
import logging
import pandas as pd

from gspread.exceptions import WorksheetNotFound

logger = logging.getLogger(__name__)


def get_gs_conferences():
    """
    Fetches data rows from a Google Sheet for conferences, filters out rows without a Speakers URL,
    and returns them as a DataFrame.

    Returns:
        pd.DataFrame: A dataframe containing filtered conference data where "Speakers URL" is not empty.
    Raises:
        Exception: If there's an error while fetching the conference data.
    """
    try:
        wks = _get_wks("CONF_LIST_SHEET_ID", "CONF_LIST_SHEET_NAME")
        records = wks.get_all_records()

        df = pd.DataFrame(records)
        df = df[df["Speakers URL"] != ""].reset_index(drop=True)
        return df
    except Exception as e:
        logger.error(f"Error fetching conference data from Google Sheets: {e}")
        raise


def get_gs_companies():
    """
    Fetches data rows from a Google Sheet for companies and returns a dictionary mapping LinkedIn URLs to companies.

    Returns:
        dict: A dictionary where the keys are LinkedIn URLs and values are company names.
    Raises:
        Exception: If there's an error while fetching company data.
    """
    try:
        wks = _get_wks("COMPANY_LIST_SHEET_ID", "COMPANY_LIST_SHEET_NAME")
        records = wks.get_all_records()

        df = pd.DataFrame(records)
        companies_dict = dict(zip(df["LinkedIn URL"], df["Company"]))
        return companies_dict
    except Exception as e:
        logger.error(f"Error fetching company data from Google Sheets: {e}")
        raise


def upload_conf_speakers_and_topics(df):
    """
    Uploads a DataFrame to a specified Google Sheet after replacing NaN values.

    Args:
        df (pd.DataFrame): DataFrame to upload to Google Sheets.
    Raises:
        Exception: If there's an error during the upload process.
    """
    try:
        logger.info(
            "Uploading list of conference speakers and their topics to Google Sheet..."
        )

        # Replace NaN values to avoid InvalidJSONError
        df = df.fillna("")

        wks = _get_wks(
            sheet_id_key="CONF_LIST_SHEET_ID",
            sheet_name_key="CONF_LIST_OF_SPEAKERS_AND_TOPICS_SHEET_NAME",
        )

        wks.update([df.columns.values.tolist()] + df.values.tolist())
        logger.info(
            "List of conference speakers and their topics uploaded to Google Sheet."
        )
    except Exception as e:
        logger.error(f"Error uploading DataFrame to Google Sheets: {e}")
        raise


def upload_conf_speakrs_with_missing_linkedin_url(df):
    """
    Uploads a DataFrame of conference speakers missing LinkedIn URLs to a Google Sheet.

    Args:
        df (pd.DataFrame): DataFrame containing speaker information, including a 'name' column.

    Raises:
        Exception: If there's an error during the upload process.
    """
    try:
        logger.info(
            "Uploading conference speakers with missing LinkedIn URL to Google Sheet..."
        )

        # Replace NaN values to avoid InvalidJSONError
        df = df.fillna("")

        df = df[["name"]]

        wks = _get_wks(
            sheet_id_key="CONF_LIST_SHEET_ID",
            sheet_name_key="CONF_SPEAKERS_WITH_MISSING_LINKEDIN_URL_SHEET_NAME",
        )
        sheet_df = pd.DataFrame(wks.get_all_records())

        if not sheet_df.empty:
            missing_df = df[~df["name"].isin(sheet_df["name"])]
            wks.append_rows(missing_df.values.tolist())
        else:
            wks.update([df.columns.values.tolist()] + df.values.tolist())

        logger.info(
            "List of conference speakers with their missing LinkedIn URL uploaded to Google Sheet."
        )
    except Exception as e:
        logger.error(f"Error uploading DataFrame to Google Sheets: {e}")
        raise


def _load_credentials():
    """
    Loads Google Service Account credentials from environment variables and returns them as a dictionary.

    Returns:
        dict: A dictionary containing Google Service Account credentials.
    Raises:
        Exception: If there's an error while loading credentials.
    """
    try:
        keys = [
            "type",
            "project_id",
            "private_key_id",
            "client_email",
            "client_id",
            "auth_uri",
            "token_uri",
            "auth_provider_x509_cert_url",
            "client_x509_cert_url",
            "universe_domain",
        ]
        credentials = {key: os.getenv(key.upper()) for key in keys}
        credentials["private_key"] = os.getenv("PRIVATE_KEY").replace("\\n", "\n")
        return credentials
    except Exception as e:
        logger.error(f"Error loading credentials: {e}")
        raise


def _get_wks(sheet_id_key, sheet_name_key):
    """
    Fetches a Google Sheet wks using its ID and sheet name from environment variables.

    Args:
        sheet_id_key (str): Environment variable key for the Google Sheet ID.
        sheet_name_key (str): Environment variable key for the sheet name.

    Returns:
        gspread.Worksheet: A Google Sheet worksheet object.

    Raises:
        Exception: If there's an error accessing the Google Sheet or worksheet.
    """
    try:
        sheet_id = os.getenv(sheet_id_key)
        sheet_name = os.getenv(sheet_name_key)

        credentials = _load_credentials()
        gc = gspread.service_account_from_dict(credentials)
        spreadsheet = gc.open_by_key(sheet_id)

        try:
            wks = spreadsheet.worksheet(sheet_name)
        except WorksheetNotFound:
            # If the worksheet doesn't exist, create a new one with default dimensions
            logger.info(f"Worksheet '{sheet_name}' not found. Creating a new one...")
            wks = spreadsheet.add_worksheet(title=sheet_name, rows="1000", cols="26")
            logger.info(f"Created new worksheet.")
        return wks
    except Exception as e:
        logger.error(f"Error fetching worksheet: {e}")
        raise
