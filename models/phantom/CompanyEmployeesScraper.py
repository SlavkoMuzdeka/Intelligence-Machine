from utils.google_sheets_utils import get_gs_companies
from models.phantom.PhantomScraper import PhantomScraper


class CompanyEmployeesScraper(PhantomScraper):
    """
    This class extends the `PhantomScraper` class and provides additional functionality
    specific to scraping employee data from LinkedIn based on a predefined set of companies.

    Attributes:
        companies_dict (dict): A dictionary mapping search queries to company names,
            retrieved from Google Sheets.
    """

    def __init__(self):
        """
        Initializes the `CompanyEmployeesScraper` class.

        Inherits initialization from the `PhantomScraper` class and retrieves a mapping
        of companies from Google Sheets to associate queries with company names.
        """
        super().__init__(agent_name="COMPANY_EMPLOYEES")
        self.companies_dict = get_gs_companies()

    def filter_df(self, df):
        """
        Filters the scraped DataFrame.

        Args:
            df (pandas.DataFrame): The scraped data to be filtered.

        Returns:
            pandas.DataFrame: A filtered DataFrame with unnecessary columns dropped
            and `company` names mapped from the query.
        """
        if "error" in df.columns:
            df = df[df["error"].isna()].reset_index(drop=True)
        df["company"] = df["query"].map(self.companies_dict)
        columns_to_drop = [
            col for col in ["connectionDegree", "error"] if col in df.columns
        ]
        df.drop(columns=columns_to_drop, inplace=True)
        return df
