import pandas as pd

from unidecode import unidecode
from utils.openai_utils import scrape_data_scrapegraphai
from utils.google_sheets_utils import get_conferences_urls


def get_speakers_from_conf_websites() -> pd.DataFrame:
    """
    Retrieves speaker information from conference websites using URLs from a Google Sheet.

    This function processes a list of conferences, scrapes data from the 'Speakers' and 'Agenda' pages,
    merges the scraped data into a combined DataFrame, and returns it. If either the speakers' page 
    or agenda page is empty, the function handles these cases by ensuring valid output.

    Returns:
        pandas.DataFrame: A DataFrame containing the conference name, speaker names, talk titles, 
                          and website URLs.
    """
    # Fetch Google Sheets data containing conference URLs
    google_sheets_df = get_conferences_urls()

    google_sheets_df = google_sheets_df[
        google_sheets_df["Speakers URL"] != ""
    ].reset_index(drop=True)

    speakers_dfs = []

     # Iterate over each conference and scrape data from its website
    for _, row in google_sheets_df.iterrows():
        print(f"Finding speakers from {row["Name"]} website")

        # Scrape data from the Speakers and Agenda pages
        speakers_page_df = _scrape_page(source=row["Speakers URL"])
        agenda_page_df = _scrape_page(source=row["Agenda URL"])

        if "website_url" in agenda_page_df.columns:
            agenda_page_df['website_url'] = None

        # Handle cases where either page has missing data
        if speakers_page_df.empty:
            print(f"Speakers df is empty for: {row["Name"]}")
            merged_df = agenda_page_df.copy()
            merged_df["website_url"] = None
        elif agenda_page_df.empty:
            print(f"Agenda df is empty for: {row["Name"]}")
            merged_df = speakers_page_df.copy()
            merged_df["talk_title"] = None
        else:
            print(f"Agenda df and speakers df aren't empty for: {row["Name"]}")

            speakers_page_df["normalized_name"] = speakers_page_df["speaker_name"].apply(_normalize_name)
            agenda_page_df["normalized_name"] = agenda_page_df["speaker_name"].apply(_normalize_name)
            
            # Data that exists on agenda page, but do not exist on 'main' page
            pom_df = agenda_page_df[~agenda_page_df["normalized_name"].isin(speakers_page_df["normalized_name"].tolist())]

            merged_df = pd.merge(
                left=speakers_page_df.drop(columns=['talk_title']), 
                right=agenda_page_df.drop(columns=['speaker_name', 'website_url']), 
                how="left", 
                on="normalized_name"
            )

            merged_df = pd.concat([merged_df, pom_df], ignore_index=True)
            merged_df.drop(columns=["normalized_name"], inplace=True)
        
        merged_df["conf_name"] = row['Name']
        speakers_dfs.append(merged_df)

    # Combine all speaker data into a single DataFrame
    all_speakers_df = pd.concat(speakers_dfs, ignore_index=True)

    return all_speakers_df


def _scrape_page(source: str) -> pd.DataFrame:
    """
    Scrapes speaker information from a conference website using the provided URL.

    Args:
        source (str): The URL of the webpage to scrape. If empty, returns an empty DataFrame.

    Returns:
        pandas.DataFrame: A DataFrame containing speaker names and website URLs.
                          If no data is found, an empty DataFrame is returned.
    """
    if source == "":
        return pd.DataFrame()
    
    # Prompt for extracting speakers and their website URLs
    prompt = "List speakers with their talk title and website URL. Return format: {speakers: [{'speaker_name': 'speaker_name', 'talk_title': 'talk_title', 'website_url': 'website_url'}]}. If there is no URL that starts with http then left website_url field empty. Remove any brackets or special characters from the speaker_name and ensure it contains only the speaker's name."
    
    result = scrape_data_scrapegraphai(prompt=prompt, source=source)

    # If scraping result contains "speakers", convert to DataFrame
    if "speakers" in result:
        return pd.DataFrame(result["speakers"])
    
    return pd.DataFrame()

def _normalize_name(name):
    return unidecode(name).strip().lower()
