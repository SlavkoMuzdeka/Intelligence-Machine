import logging

from models.phantom.PhantomScraper import PhantomScraper

logger = logging.getLogger(__name__)


class SearchExportScraper(PhantomScraper):
    """
    This class extends the `PhantomScraper` class and includes functionality to process
    and filter data specific to search export results.
    """

    def __init__(self):
        """
        Initializes the `SearchExportScraper` class.

        Sets the agent name to "SEARCH_EXPORT" by calling the parent class constructor.
        """
        super().__init__(agent_name="SEARCH_EXPORT")

    def filter_df(self, df):
        """
        Filters the scraped DataFrame.

        Args:
            df (pandas.DataFrame): The scraped data to be filtered.

        Returns:
            pandas.DataFrame: A filtered DataFrame with irrelevant columns dropped
            and renamed for standardization.
        """
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
