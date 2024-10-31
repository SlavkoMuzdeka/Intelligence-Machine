import os
import logging
import psycopg2
import pandas as pd

from unidecode import unidecode
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models.database.models import Base
from psycopg2 import sql, OperationalError
from sqlalchemy.exc import SQLAlchemyError
from models.database.models import (
    Talk,
    Speaker,
    Conference,
    LinkedInUser,
    LinkedInCompany,
    UserCompanyAssociation,
)

logger = logging.getLogger(__name__)

# -------------------------- Database Initialization and Setup -------------------------- #


def create_database_session_and_engine():
    """
    Establishes a new SQLAlchemy session for database interactions.

    Returns:
        session (sessionmaker): SQLAlchemy session object.
    """
    database_uri = f"postgresql://{os.getenv("DATABASE_USER")}:{os.getenv("DATABASE_PASSWORD")}@{os.getenv("DATABASE_HOST")}:{os.getenv("DATABASE_PORT")}/{os.getenv("DATABASE_NAME")}"
    engine = create_engine(database_uri)
    Session = sessionmaker(bind=engine)
    return Session(), engine


def create_database_if_not_exists():
    """
    Creates the PostgreSQL database if it does not exist and creates tables.
    """
    db_name = os.getenv("DATABASE_NAME")

    try:
        # Connect to the default 'postgres' database
        conn = psycopg2.connect(
            database="postgres",  # Connect to the default postgres database
            user=os.getenv("DATABASE_USER"),
            password=os.getenv("DATABASE_PASSWORD"),
            host=os.getenv("DATABASE_HOST"),
            port=os.getenv("DATABASE_PORT"),
        )
        conn.autocommit = True  # Enable autocommit mode

        # Creating a cursor object
        cursor = conn.cursor()

        # Check if the database already exists
        cursor.execute(
            sql.SQL("SELECT 1 FROM pg_database WHERE datname = %s"), [db_name]
        )
        exists = cursor.fetchone()

        if not exists:
            # Create the database if it doesn't exist
            cursor.execute(sql.SQL(f"CREATE DATABASE {db_name}"))
            logger.info(f"Database '{db_name}' has been created successfully!")
            _create_tables()
    except OperationalError as e:
        logger.error(f"Error connecting to the PostgreSQL server: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


# -------------------------- Global session object -------------------------- #

session, engine = create_database_session_and_engine()

# -------------------------- Speaker CRUD Operations -------------------------- #


def get_speakers(filter_condition=None):
    """
    Fetches speaker data from the database as a DataFrame.

    Args:
        filter_condition (Optional): Condition to filter speakers.

    Returns:
        pd.DataFrame: Speaker information including name, website, LinkedIn, and normalized name.
    """
    return _fetch_data(model=Speaker, filter_condition=filter_condition)


def update_speaker(speaker_name, new_linkedin_url):
    """
    Updates LinkedIn URLs of conference speaker in the database.

    Args:
        speaker_name (str): Name of the speaker.
        new_linkedin_url (str): New LinkedIn URL of user.
    """

    speaker = session.query(Speaker).filter(Speaker.name == speaker_name).one_or_none()

    if speaker:
        if speaker.linkedin_url == None:
            speaker.linkedin_url = new_linkedin_url
            speaker.update(session)
            return True
    else:
        logger.info(f"Speaker '{speaker_name}' not found in the database.")
    return False


def insert_speaker(row):
    """
    Inserts a speaker into the database if they do not already exist.

    Args:
        row (pd.Series): A row from the DataFrame containing speaker details.
                         Expected keys include 'speaker_name', 'website_url', and 'linkedin_url'.
    """
    _insert_data(
        model=Speaker,
        filter_condition=(Speaker.name == row["speaker_name"]),
        name=row["speaker_name"],
        website_url=row["website_url"],
        linkedin_url=row["linkedin_url"],
    )


# -------------------------- Conference CRUD Operations -------------------------- #


def insert_conference(name, year):
    """
    Inserts a conference into the database if it does not already exist.

    Args:
        name (str): The name of the conference.
        year (int): The year of the conference.
    """
    _insert_data(
        model=Conference,
        filter_condition=((Conference.name == name) & (Conference.year == year)),
        name=name,
        year=year,
    )


# -------------------------- Talk CRUD Operations -------------------------- #


def get_talks():
    """
    Fetches all conference talk data from the database and converts it to a DataFrame.

    Returns:
        pd.DataFrame: DataFrame containing conference talk information.
    """
    return _fetch_data(model=Talk, norm_by_column="speaker_name")


def insert_talk(row):
    """
    Inserts a talk into the database.

    Args:
        row (pd.Series): A row from the DataFrame containing talk details.
                         Expected keys include 'speaker_name', 'conf_name', 'conf_year',
                         'talk_title', and 'company'.
    """
    _insert_data(
        model=Talk,
        filter_condition=None,
        speaker_name=row["speaker_name"],
        conference_name=row["conf_name"],
        conference_year=row["conf_year"],
        talk_title=row["talk_title"],
        company=row["company"],
    )


# -------------------------- LinkedIn User CRUD Operations -------------------------- #


def get_linkedin_users():
    """
    Load all LinkedIn users from the database.

    Returns:
    - pd.DataFrame: Dataframe containing LinkedIn users data.
    """
    return _fetch_data(model=LinkedInUser)


def insert_linkedin_user(row):
    """
    Inserts a LinkedIn user into the database if not present.

    Args:
        session (Session): Database session.
        row (dict): User data, including "profileUrl", "name", "firstName",
                    "lastName", "job", and "location".
    """
    _insert_data(
        model=LinkedInUser,
        filter_condition=(LinkedInUser.profile_url == row["profileUrl"]),
        profile_url=row["profileUrl"],
        name=row["name"],
        first_name=row["firstName"],
        last_name=row["lastName"],
        description=row["job"],
        location=row["location"],
    )


# -------------------------- LinkedIn Company CRUD Operations -------------------------- #


def insert_linkedin_company(row):
    """
    Inserts a LinkedIn company into the database if not present.

    Args:
        row (dict): Company data, including "query" and "company".
    """
    _insert_data(
        model=LinkedInCompany,
        filter_condition=(LinkedInCompany.profile_url == row["query"]),
        profile_url=row["query"],
        name=row["company"],
    )


# -------------------------- User Company Association CRUD Operations -------------------------- #


def get_user_company_associations():
    """
    Fetches LinkedIn user-company associations from the database.

    Returns:
    - pd.DataFrame: Dataframe containing User-company association data.
    """
    return _fetch_data(model=UserCompanyAssociation)


def insert_user_company(row, status_code):
    """
    Inserts a UserCompanyAssociation instance into the database if not present.

    Args:
        row (dict): Company data, including "profileUrl" and "query".
        status_code (int): Status code of employee.
    """
    _insert_data(
        model=UserCompanyAssociation,
        filter_condition=None,
        user_profile_url=row["profileUrl"],
        company_profile_url=row["query"],
        status_code=status_code,
    )


# --------------------------- Helper Functions ------------------------ #


def _create_tables():
    """
    Create tables in the specified database.
    """
    try:
        Base.metadata.create_all(engine)
        logger.info("Tables created successfully.")
    except SQLAlchemyError as e:
        logger.error(f"Error creating tables: {e}")
        raise


def _insert_data(model, filter_condition=None, **kwargs):
    """
    Checks for the existence of an entry based on the filter condition and inserts if not exists.

    Args:
        model: ORM model class
        filter_condition: Filter condition for checking existence
        kwargs: Attributes for creating the new instance if it doesn't exist
    """
    obj = None
    if filter_condition is not None:
        obj = session.query(model).filter(filter_condition).one_or_none()

    if obj is None:
        instance = model(**kwargs)
        instance.insert(session)


def _fetch_data(model, norm_by_column="name", filter_condition=None):
    """
    Fetches data from the database based on the provided model and filter condition.

    Args:
        model: ORM model class to fetch data from.
        filter_condition (Optional): Condition to filter results.

    Returns:
        pd.DataFrame: DataFrame containing the fetched data or an empty DataFrame if no data is found.
    """
    logging.info(f"Fetching data from {model.__name__}...")

    if filter_condition is not None:
        data = session.query(model).filter(filter_condition).all()
    else:
        data = session.query(model).all()

    if not data:
        logging.warning(f"No records found in {model.__name__}.")
        return pd.DataFrame()

    df = pd.DataFrame(
        [
            {col.name: getattr(record, col.name) for col in model.__table__.columns}
            for record in data
        ]
    )
    df["norm_name"] = df[norm_by_column].apply(normalize_name)
    logging.info(f"Successfully fetched data from {model.__name__}.")
    return df


def normalize_name(name: str) -> str:
    """
    Normalizes speaker names by removing accents and converting to lowercase.

    Args:
        name (str): Speaker's name.

    Returns:
        str: Normalized speaker name.
    """
    return unidecode(name).strip().lower()
