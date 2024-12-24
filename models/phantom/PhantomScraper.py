import os
import logging
import pandas as pd

from utils.phantom_buster_utils import (
    save_ids,
    get_saved_ids,
    get_scraped_data,
    get_all_agent_containers,
)

logger = logging.getLogger(__name__)


class PhantomScraper:
    """
    The `PhantomScraper` class provides functionality for interacting with Phantom Buster agents to scrape data, process it, and manage container IDs for executed tasks.

    Attributes:
        agent_name (str): Name of the Phantom Buster agent.
        base_url (str): Base URL for the Phantom Buster API.
        api_key (str): API key for authenticating Phantom Buster requests.
        agent_id (str): Agent ID for the specified Phantom Buster agent.
        container_ids_path (str): Path to the file where container IDs are stored.
    """

    def __init__(self, agent_name: str):
        """
        Initializes the PhantomScraper instance.

        Args:
            agent_name (str): Name of the Phantom Buster agent.
        """
        self.agent_name = agent_name
        self.base_url = os.getenv("PHANTOM_BUSTER_BASE_URL")
        self.api_key = os.getenv("PHANTOM_BUSTER_API_KEY")
        self.agent_id = os.getenv(f"PHANTOM_BUSTER_{agent_name}_AGENT_ID")
        self.container_ids_path = os.path.join(
            os.getcwd(),
            "Data",
            f"{self.agent_name.lower()}_container_ids_{self.agent_id}.csv",
        )

    def get_scraped_data(self):
        """
        Public method that returns the data scraped from the Phantom Buster agent.

        Returns:
            pandas.DataFrame: The concatenated data from new containers, or an empty DataFrame if no new data is available.
        """
        return self._scrape_data()

    def filter_df(self, df):
        """
        Placeholder method to filter the scraped data.

        Args:
            df (pandas.DataFrame): The scraped data to be filtered.

        Returns:
            pandas.DataFrame: The filtered DataFrame (to be implemented in a subclass or overridden).
        """
        pass

    def _scrape_data(self):
        """
        Private method to scrape data from new containers, filter it, and save container IDs.

        Returns:
            pandas.DataFrame: A DataFrame containing concatenated data from new containers, or an empty DataFrame if no new data is available.
        """
        containers = get_all_agent_containers(
            self.agent_id, self.base_url, self.api_key
        )
        all_ids = [int(container["id"]) for container in containers]
        old_ids = get_saved_ids(self.container_ids_path)
        new_ids = [id for id in all_ids if id not in old_ids]

        logger.info(f"Saved container ids are: {old_ids}")
        logger.info(f"Executed container ids are : {all_ids}")
        logger.info(f"New container ids are : {new_ids}")

        new_dfs = []

        for id in reversed(new_ids):
            df = get_scraped_data(id, self.base_url, self.api_key)
            if not df.empty:
                filtered_df = self.filter_df(df.copy())
                new_dfs.append(filtered_df)
            old_ids.append(id)
        save_ids(old_ids, self.container_ids_path)

        if new_dfs:
            df = pd.concat(new_dfs, ignore_index=True)
            return df
        return pd.DataFrame()
