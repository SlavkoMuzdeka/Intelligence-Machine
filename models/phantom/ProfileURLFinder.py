from models.phantom.PhantomScraper import PhantomScraper


class ProfileURLFinder(PhantomScraper):

    def __init__(self):
        super().__init__(agent_name="PROFILE_URL_FINDER", owner_api_key="DENIS")

    def get_scraped_data(self):
        scraped_data = self.scrape_data()
        return scraped_data

    def filter_df(self, df):
        df = df[["query", "url", "title", "description"]]
        df.rename(
            columns={"query": "name"},
            inplace=True,
        )
        return df
