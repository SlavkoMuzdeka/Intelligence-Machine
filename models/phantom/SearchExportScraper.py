import logging

from models.phantom.PhantomScraper import PhantomScraper

logger = logging.getLogger(__name__)


class SearchExportScraper(PhantomScraper):

    def __init__(self):
        super().__init__(agent_name="SEARCH_EXPORT")

    def get_scraped_data(self):
        scraped_data = self.scrape_data()
        return scraped_data

    def filter_df(self, df):
        columns_to_drop = [
            col
            for col in ["vmid", "sharedConnections", "url", "category", "timestamp"]
            if col in df.columns
        ]
        total_speakers = len(df["query"].unique())
        logger.info(
            f"Fetched {len(df)} records from Search Export (Total: {total_speakers} speakers)."
        )
        df.drop(columns=columns_to_drop, inplace=True)
        df = df[df["error"].isna()].reset_index(drop=True)
        df.rename(columns={"profileUrl": "linkedin_url"}, inplace=True)
        return df
