import os
import gspread
import pandas as pd


def load_credentials():
    """
    Loads Google Service Account credentials from Streamlit secrets.
    """

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


def get_conferences_urls():
    """
    Fetches data rows from a Google Sheet.
    """
    credentials = load_credentials()
    gc = gspread.service_account_from_dict(credentials)
    rows = (
        gc.open_by_key(os.getenv("SHEET_ID"))
        .worksheet(os.getenv("SHEET_NAME"))
        .get_all_records()
    )
    return pd.DataFrame(rows)
