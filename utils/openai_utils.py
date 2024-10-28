import os
import json
import logging
import pandas as pd

from openai import OpenAI
from scrapegraphai.graphs import SmartScraperGraph

logger = logging.getLogger(__name__)

client = OpenAI()


def parse_speakers_from_yt_titles(video_titles: str) -> pd.DataFrame:
    """
    Extracts speaker names and talk titles from a string of YouTube video titles using an AI assistant.

    Args:
        video_titles (str): A string containing video titles, each prefixed with an index and separated by newlines.

    Returns:
        pandas.DataFrame: A DataFrame with columns 'name' and 'talk_title' for each speaker.
    """
    try:
        messages = [
            {
                "role": "system",
                "content": "You are a helpful assistant. Your response should be in JSON format: speakers: [{'speaker_name': 'speaker_name', 'talk_title': 'talk_title'}]",
            },
            {
                "role": "user",
                "content": f"Extract the talk titles and speaker names from the following video titles:\n{video_titles}\n. Remove any brackets or special characters from the speaker_name and ensure it contains only the speaker's name.",
            },
        ]

        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=messages,
            response_format={"type": "json_object"},
        )

        res = json.loads(response.choices[0].message.content)
        speakers_df = pd.DataFrame(res.get("speakers", {}))
        return speakers_df
    except Exception as e:
        logger.error(f"Error extracting speaker names from video titles: {e}")
        raise


def scrape_data_scrapegraphai(prompt: str, source: str) -> dict:
    """
    Scrapes data from the web using SmartScraperGraph with a given prompt and source.

    Args:
        prompt (str): A text prompt that defines the scraping goal (e.g., what data to extract).
        source (str): The URL or other source from which to scrape the data.

    Returns:
        dict: The result of the scraping process, which typically contains the extracted data in dictionary format.
    """
    try:
        graph_config = {
            "llm": {
                "model": os.getenv("OPENAI_MODEL"),
                "api_key": os.getenv("OPENAI_API_KEY"),
            },
            "headless": True,
        }
        scraper = SmartScraperGraph(prompt=prompt, source=source, config=graph_config)
        result = scraper.run()

        return result
    except Exception as e:
        logger.error(f"Error scraping data from {source} with prompt '{prompt}': {e}")
        raise


def image_to_text(base64_image):
    """
    Extracts talk titles, speaker names, and companies from an image using an AI assistant.

    Args:
        base64_image (str): Base64 encoded image string.

    Returns:
        pandas.DataFrame: A DataFrame containing speaker names, talk titles, and company names.
    """
    try:
        messages = [
            {
                "role": "system",
                "content": "You are a helpful assistant. Your response should be in JSON format: speakers: [{'speaker_name': 'speaker_name', 'talk_title': 'talk_title', 'company': 'company_name'}]",
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": (
                            "Extract the talk titles, speaker names, and the companies they work for from the image. "
                            "If there is a panel with multiple speakers, list each speaker's name along with the talk title and company. "
                            "Ensure the 'company' field contains a valid company name. If the extracted value appears to be a talk category, panel name, or event group, set 'company' to None."
                        ),
                    },
                    {
                        "type": "image_url",
                        "image_url": {"url": f"data:image/jpeg;base64,{base64_image}"},
                    },
                ],
            },
        ]
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=messages,
            response_format={"type": "json_object"},
        )
        res = json.loads(response.choices[0].message.content)
        speakers_df = pd.DataFrame(res.get("speakers", {}))
        return speakers_df
    except Exception as e:
        logger.error(f"Error extracting data from image: {e}")
        raise


def get_openai_filtered_profiles(profiles):
    """
    Filters LinkedIn profiles using an AI assistant based on Web3 and blockchain criteria.

    Args:
        profiles (list): A list of LinkedIn profiles.

    Returns:
        str or None: The URL of the filtered LinkedIn profile or None if no match.
    """
    try:
        messages = [
            {
                "role": "system",
                "content": (
                    "You are a data filtering assistant specialized in LinkedIn profiles."
                    "Your response must be in JSON format: "
                    "{'linkedin_url': 'linkedin_url'}"
                ),
            },
            {
                "role": "user",
                "content": f"""
                    Your task is to process a list of LinkedIn profiles.
                    Each profile includes information like currentJob, job (where company could be mentioned), additionalInfo, and summary.
                    
                    If multiple profiles have the same or similar names, consider them duplicates and return only one profile from duplicates associated with Web3 
                    and blockchain space, based on the following criteria:
                    
                    - Mention of Web3 or blockchain in their job.
                    - Mention of Web3 or blockchain in their current job.
                    - Company they work for (check if the company operates in the Web3 or blockchain space). 
                
                    Here are the profiles:\n {str(profiles)}.
                """,
            },
        ]

        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=messages,
            response_format={"type": "json_object"},
        )
        res = json.loads(response.choices[0].message.content)
        return res.get("linkedin_url", None)
    except Exception as e:
        logger.error(f"Error occurred while processing profiles with OpenAI - {str(e)}")
        raise
