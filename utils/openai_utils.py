import os
import json
import pandas as pd

from openai import OpenAI
from dotenv import load_dotenv
from scrapegraphai.graphs import SmartScraperGraph

load_dotenv()

client = OpenAI()
client.api_key = os.getenv("OPENAI_API_KEY")


def parse_speakers_from_yt_titles(video_titles: str) -> pd.DataFrame:
    """
    Extracts speaker names and talk titles from a string of YouTube video titles using an AI assistant.

    Args:
        video_titles (str): A string containing video titles, each prefixed with an index and separated by newlines.

    Returns:
        pandas.DataFrame: A DataFrame with columns 'name' and 'talk_title' for each speaker.
    """
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
        model="gpt-4o-mini", messages=messages, response_format={"type": "json_object"}
    )

    res = json.loads(response.choices[0].message.content)
    return pd.DataFrame(res["speakers"])


def scrape_data_scrapegraphai(prompt: str, source: str) -> dict:
    """
    Scrapes data from the web using SmartScraperGraph with a given prompt and source.

    Args:
        prompt (str): A text prompt that defines the scraping goal (e.g., what data to extract).
        source (str): The URL or other source from which to scrape the data.

    Returns:
        dict: The result of the scraping process, which typically contains the extracted data in dictionary format.
    """
    graph_config = {
        "llm": {
            "model": os.getenv("OPENAI_MODEL"),
            "api_key": os.getenv("OPENAI_API_KEY"),
        },
        "verbose": True,
        "headless": True,
    }
    scraper = SmartScraperGraph(prompt=prompt, source=source, config=graph_config)
    result = scraper.run()

    return result
