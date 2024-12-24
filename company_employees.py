import logging
import pandas as pd

from datetime import timezone
from dotenv import load_dotenv
from models.phantom.CompanyEmployeesScraper import CompanyEmployeesScraper

load_dotenv(override=True)

from models.EmploymentStatus import EmploymentStatus
from utils.google_sheets_utils import upload_data_to_gs
from utils.company_employees_utils import get_all_employees
from utils.database_utils import (
    session,
    insert_linkedin_user,
    insert_linkedin_company,
    get_user_company_association,
    create_database_if_not_exists,
    get_user_company_associations,
    insert_user_company_association,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(message)s",
    datefmt="%d-%m-%Y %H:%M:%S",
)
logger = logging.getLogger(__name__)

SHEET_ID_KEY = "COMPANY_LIST_SHEET_ID"
COMPANY_EMPLOYEES = "COMPANY_EMPLOYEES"
COMPANY_FORMER_EMPLOYEES = "COMPANY_EMPLOYEES_FORMER"


def proces_new_scrapped_employees(df):
    """
    Processes newly scraped employee data to update or insert user-company associations.

    Args:
        df (DataFrame): A DataFrame containing scraped employee data, including profile URLs
                        for both users and companies.
    """
    logger.info("Starting to process newly scraped employees...")
    last_updated = df["timestamp"].max()
    for _, row in df.iterrows():
        user_company_association = get_user_company_association(
            user_profile_url=row["profileUrl"], company_profile_url=row["query"]
        )

        if user_company_association:
            local_last_updated = user_company_association.last_updated
            db_last_updated_utc = local_last_updated.astimezone(timezone.utc)
            if db_last_updated_utc != last_updated:
                user_company_association.status_code = EmploymentStatus.EMPLOYED.value
                user_company_association.update_count += 1
                user_company_association.last_updated = last_updated
                user_company_association.update(session=session)
        else:
            # Insert new user, company and user-company associations
            insert_linkedin_user(row)
            insert_linkedin_company(row)
            insert_user_company_association(
                row=row,
                status_code=EmploymentStatus.EMPLOYED.value,
                last_updated=last_updated,
            )
    logger.info("Finished processing newly scraped employees.")


def process_former_employees(df):
    """
    Processes data to update the status of former employees who are no longer with the company.

    Args:
        df (DataFrame): A DataFrame containing the current list of employees for various companies.
    """
    logger.info("Starting to process former employees...")

    # Fetch existing associations for users and companies
    uca_df = get_user_company_associations()
    filtered_uca_df = uca_df[
        uca_df["company_profile_url"].isin(list(df["query"].unique()))
    ].reset_index(drop=True)

    former_employees_df = filtered_uca_df[
        ~filtered_uca_df["user_profile_url"].isin(df["profileUrl"].tolist())
    ].reset_index(drop=True)

    last_updated = df["timestamp"].max()
    for _, row in former_employees_df.iterrows():
        user_company_association = get_user_company_association(
            user_profile_url=row["user_profile_url"],
            company_profile_url=row["company_profile_url"],
        )
        local_last_updated = user_company_association.last_updated
        db_last_updated_utc = local_last_updated.astimezone(timezone.utc)
        if db_last_updated_utc != last_updated:
            user_company_association.status_code = EmploymentStatus.UNEMPLOYED.value
            user_company_association.last_updated = last_updated
            user_company_association.update_count += 1
            user_company_association.update(session=session)
    logger.info("Finished processing former employees.")


def main():
    """
    Main function to initialize the database and process new and former employee data.
    """
    logger.info("Script is running...")

    # Ensure database is created and initialized
    create_database_if_not_exists()

    ces = CompanyEmployeesScraper()
    scraped_employees_df = ces.get_scraped_data()

    if not scraped_employees_df.empty:
        scraped_employees_df["timestamp"] = pd.to_datetime(
            scraped_employees_df["timestamp"]
        )
        proces_new_scrapped_employees(scraped_employees_df)
        process_former_employees(scraped_employees_df)

    all_employees_df = get_all_employees()
    former_employees = all_employees_df[all_employees_df["color"] == "red"].reset_index(
        drop=True
    )

    upload_data_to_gs(all_employees_df, SHEET_ID_KEY, COMPANY_EMPLOYEES)
    upload_data_to_gs(former_employees, SHEET_ID_KEY, COMPANY_FORMER_EMPLOYEES)

    logger.info("Main execution completed.")


if __name__ == "__main__":
    main()
