from models.phantom.PhantomScraper import PhantomScraper


class ProfileScraper(PhantomScraper):
    def __init__(self):
        super().__init__("PROFILE")

    def get_scraped_data(self):
        scraped_data = self.scrape_data()
        return scraped_data

    def filter_df(self, df):
        return df
