import pandas as pd

from utils.database_utils import get_linkedin_users, get_user_company_associations


def get_all_employees():
    """
    Retrieves and processes LinkedIn user data and their company associations.

    This function merges linkedin user data with company associations, processes data for
    users employed at a single company and those associated with multiple companies,
    and consolidates the results into a final DataFrame. The resulting DataFrame is
    sorted by user names and includes a formatted list of associated companies.

    Returns:
        pandas.DataFrame: A DataFrame containing processed user data with their associated
        companies and employment details.
    """
    users_df = get_linkedin_users()
    associations_df = get_user_company_associations()

    merged_df = pd.merge(
        users_df, associations_df, left_on="profile_url", right_on="user_profile_url"
    )

    single_company_df = _process_single_company_employees(merged_df.copy())
    multiple_companies_df = _process_mulitple_companies_employees(merged_df.copy())

    df = pd.concat([single_company_df, multiple_companies_df], ignore_index=True)
    df["companies"] = df["companies"].astype(str)
    df = df.sort_values(by="name").reset_index(drop=True)
    return df


def _process_single_company_employees(df):
    """
    Processes data for users associated with a single company.

    Filters users with only one company association, formats their company details,
    and assigns a color based on their employment status and update count.

    Args:
        df (pandas.DataFrame): Merged user and company association data.

    Returns:
        pandas.DataFrame: A DataFrame with processed data for single-company employees.
    """
    single_company_df = df.groupby("profile_url").filter(lambda x: len(x) == 1)
    single_company_df["last_updated"] = single_company_df["last_updated"]
    single_company_df["companies"] = single_company_df.apply(
        lambda row: [
            {
                "company": row["company_profile_url"],
                "employment_status": (
                    "Employed" if row["status_code"] == 1 else "Unemployed"
                ),
                "last_updated": row["last_updated"].strftime("%Y-%m-%d %H:%M:%S"),
            }
        ],
        axis=1,
    )
    single_company_df["color"] = single_company_df.apply(_assign_color, axis=1)
    single_company_df.drop(
        columns=[
            "norm_name",
            "first_name",
            "last_name",
            "association_id",
            "user_profile_url",
            "status_code",
            "update_count",
            "company_profile_url",
            "last_updated",
        ],
        inplace=True,
    )
    return single_company_df


def _process_mulitple_companies_employees(df):
    """
    Processes data for users associated with multiple companies.

    Groups data by user profile, aggregates company details into lists, and formats
    the company details into a standardized structure.

    Args:
        df (pandas.DataFrame): Merged user and company association data.

    Returns:
        pandas.DataFrame: A DataFrame with processed data for multi-company employees.
    """
    filtered_df = df.groupby("profile_url").filter(lambda x: len(x) > 1)
    grouped_df = (
        filtered_df.groupby("profile_url")
        .agg(
            {
                "name": "first",
                "description": "first",
                "location": "first",
                "company_profile_url": lambda x: list(x),
                "status_code": lambda x: list(x),
                "last_updated": lambda x: list(x),
            }
        )
        .reset_index()
    )
    grouped_df["companies"] = grouped_df.apply(_group_companies, axis=1)
    grouped_df["color"] = grouped_df["companies"].apply(
        _assign_color_multiple_companies
    )
    grouped_df = grouped_df.drop(
        columns=["company_profile_url", "status_code", "last_updated"]
    )
    return grouped_df


def _assign_color(row):
    """
    Assigns a color based on the user's employment status and update count.

    Args:
        row (pandas.Series): A row from the DataFrame.

    Returns:
        str: A color indicating the employment status.
    """
    status_code = int(row["status_code"])
    update_count = int(row["update_count"])
    return "red" if status_code == 0 else ("green" if update_count == 0 else "neutral")


def _group_companies(row):
    """
    Groups company details into a standardized list of dictionaries.

    Args:
        row (pandas.Series): A row from the grouped DataFrame.

    Returns:
        list[dict]: A list of dictionaries containing company details.
    """
    companies = []
    for company, status, last_updated in zip(
        row["company_profile_url"], row["status_code"], row["last_updated"]
    ):
        companies.append(
            {
                "company": company,
                "employment_status": "Employed" if status == 1 else "Not Employed",
                "last_updated": last_updated.strftime("%Y-%m-%d %H:%M:%S"),
            }
        )
    return companies


def _assign_color_multiple_companies(companies):
    """
    Assigns a color based on the employment statuses in multiple companies.

    Args:
        companies (list[dict]): A list of dictionaries containing company details.

    Returns:
        str: A color indicating the overall employment status.
    """
    if all(company["employment_status"] == "Not Employed" for company in companies):
        return "red"
    return "multiple"
