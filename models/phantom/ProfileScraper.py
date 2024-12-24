from models.phantom.PhantomScraper import PhantomScraper


class ProfileScraper(PhantomScraper):
    """
    This class extends the `PhantomScraper` class and provides minimal functionality
    for profile scraping, where no additional filtering logic is required.
    """

    def __init__(self):
        """
        Initializes the `ProfileScraper` class.

        Sets the agent name to "PROFILE" by calling the parent class constructor.
        """
        super().__init__("PROFILE")

    def filter_df(self, df):
        """
        Returns the DataFrame as is.

        Args:
            df (pandas.DataFrame): The scraped data.

        Returns:
            pandas.DataFrame: The unmodified input DataFrame.
        """
        return df
