from utils.google_sheets_utils import get_gs_companies
from models.phantom.PhantomScraper import PhantomScraper


class CompanyEmployeesScraper(PhantomScraper):
    """
    Company Employee Scraper which scrapes company employees from LinkedIn
    using PhantomBuster.
    """

    def __init__(self):
        super().__init__(agent_name="COMPANY_EMPLOYEES")
        self.companies_dict = get_gs_companies()

    def get_scraped_data(self):
        scraped_data = self.scrape_data()
        return scraped_data

    def filter_df(self, df):
        if "error" in df.columns:
            df = df[df["error"].isna()].reset_index(drop=True)
        df["company"] = df["query"].map(self.companies_dict)
        columns_to_drop = [
            col for col in ["connectionDegree", "error"] if col in df.columns
        ]
        df.drop(columns=columns_to_drop, inplace=True)
        return df
