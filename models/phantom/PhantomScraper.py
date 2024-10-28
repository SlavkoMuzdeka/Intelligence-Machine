import os
import logging
import pandas as pd

from utils.phantom_buster_utils import (
    get_scraped_data,
    read_container_ids,
    update_container_id,
    get_all_agent_containers,
)

logger = logging.getLogger(__name__)


class PhantomScraper:

    def __init__(self, agent_name):
        self.agent_name = agent_name

        self.base_url = os.getenv("PHANTOM_BUSTER_BASE_URL")
        self.api_key = os.getenv("PHANTOM_BUSTER_API_KEY")
        self.agent_id = os.getenv(f"PHANTOM_BUSTER_{agent_name}_AGENT_ID")

        self.container_ids_path = os.path.join(
            os.getcwd(),
            "Data",
            f"{self.agent_name.lower()}_container_ids_{self.agent_id}.txt",
        )

    def scrape_data(self):
        containers = get_all_agent_containers(
            self.agent_id, self.base_url, self.api_key
        )
        executed_container_ids = [container["id"] for container in containers]
        saved_container_ids = read_container_ids(self.container_ids_path)

        container_ids = [
            id for id in executed_container_ids if id not in saved_container_ids
        ]

        logger.info(f"Saved container ids are: {saved_container_ids}")
        logger.info(f"Executed container ids are : {executed_container_ids}")
        logger.info(f"New container ids are : {container_ids}")

        new_dfs = []

        for container_id in reversed(container_ids):
            df = get_scraped_data(container_id, self.base_url, self.api_key)
            if not df.empty:
                filtered_df = self.filter_df(df.copy())
                new_dfs.append(filtered_df)

            update_container_id(container_id, self.container_ids_path)

        if new_dfs:
            df = pd.concat(new_dfs, ignore_index=True)
            return df
        return pd.DataFrame()

    def filter_df(self, df):
        pass
