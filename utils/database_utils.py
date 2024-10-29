import os
import logging
import psycopg2
import numpy as np
import pandas as pd

from unidecode import unidecode
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models.database.models import Base
from psycopg2 import sql, OperationalError
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
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


def _create_tables():
    """
    Create tables in the specified database.
    """
    try:
        session, engine = create_database_session_and_engine()
        Base.metadata.create_all(engine)
        logger.info("Tables created successfully.")
    except SQLAlchemyError as e:
        logger.error(f"Error creating tables: {e}")
        raise
    finally:
        session.close()


# -------------------------- Speaker CRUD Operations -------------------------- #


def get_speakers(session, filter=False):
    """
    Fetches all speaker data from the database and converts it to a DataFrame.

    Args:
        session (Session): SQLAlchemy database session.
        filter (boolean): If set to true, return conference speakers which linkedIn URL is None,
        else return all speakers

    Returns:
        pd.DataFrame: DataFrame containing speaker information.

    Raises:
        Exception: If any error occurs during the database query.
    """
    try:
        if filter:
            logger.info("Loading conference speakers without LinkedIn URL...")
            speakers = (
                session.query(Speaker).filter(Speaker.linkedIn_url.is_(None)).all()
            )
        else:
            logging.info("Fetching speakers data from the database...")
            speakers = session.query(Speaker).all()

        if not speakers:
            logging.warning("No speakers found in the database.")

        speaker_data = [
            {
                "name": speaker.name,
                "website_url": speaker.website_url,
                "linkedin_url": speaker.linkedIn_url,
                "norm_name": normalize_name(speaker.name),
            }
            for speaker in speakers
        ]
        logging.info("Successfully fetched conference speakers.")
        return pd.DataFrame(speaker_data)
    except Exception as e:
        logger.info(f"An error occurred while fetching speakers data: {e}")
        raise


def update_speakers(df, session, column_name="name"):
    """
    Updates LinkedIn URLs of conference speakers in the database.

    Args:
        df (pd.DataFrame): DataFrame containing speaker data.
        column_name (str): The column name corresponding to speaker names in the DataFrame.
    """
    logger.info("Updating speakers LinkedIn URLs in the database...")

    updated_speakers = 0
    speakers_with_linkedIn_url = 0

    try:
        for _, row in df.iterrows():
            speaker_name = row[column_name]
            new_linkedin_url = row["linkedin_url"]

            speaker = (
                session.query(Speaker)
                .filter(Speaker.name == speaker_name)
                .one_or_none()
            )

            if speaker:
                if speaker.linkedIn_url == None:
                    speaker.linkedIn_url = new_linkedin_url
                    speaker.update(session)
                    updated_speakers += 1
                else:
                    speakers_with_linkedIn_url += 1
            else:
                logger.info(f"Speaker '{speaker_name}' not found in the database.")

        logger.info(f"Updated LinkedIn URLs for {updated_speakers} speakers.")
        logger.info(
            f"Skipped {speakers_with_linkedIn_url} speakers (already had LinkedIn URLs)."
        )
    except Exception as e:
        session.rollback()
        logger.error(f"Failed to update LinkedIn URLs for speakers - ERROR: {e}")
        raise


# -------------------------- Talk CRUD Operations -------------------------- #


def get_talks(session):
    """
    Fetches all conference talk data from the database and converts it to a DataFrame.

    Args:
        session (Session): SQLAlchemy database session.

    Returns:
        pd.DataFrame: DataFrame containing conference talk information.

    Raises:
        Exception: If any error occurs during the database query.
    """

    try:
        logging.info("Fetching conference talks data from the database...")
        talks = session.query(Talk).all()

        if not talks:
            logging.warning("No conference talks found in the database.")

        talk_data = [
            {
                "speaker_name": talk.speaker_name,
                "conference_name": talk.conference_name,
                "conference_year": talk.conference_year,
                "talk_title": talk.talk_title,
                "norm_name": normalize_name(talk.speaker_name),
            }
            for talk in talks
        ]
        logging.info("Successfully fetched conference talks data.")
        return pd.DataFrame(talk_data)
    except Exception as e:
        logging.error(f"An error occurred while fetching conference talks data: {e}")
        raise


def insert_talks(df, session):
    """
    Inserts talk data into the database.

    Args:
        df (pd.DataFrame): DataFrame containing speaker and conference details.
    """
    df = df.replace({np.nan: None})
    conf_name = df.loc[0, "conf_name"]
    conf_year = int(df.loc[0, "conf_year"])

    try:
        # Insert new conference
        conference = Conference(name=conf_name, year=conf_year)
        conference.insert(session)

        for _, row in df.iterrows():
            speaker_name = row["speaker_name"]

            # Check if speaker exists
            speaker_exists = (
                session.query(Speaker)
                .filter(Speaker.name == speaker_name)
                .one_or_none()
            )

            # Insert new speaker if not exists
            if not speaker_exists:
                speaker = Speaker(
                    name=speaker_name,
                    website_url=row["website_url"],
                    linkedIn_url=row["linkedIn_url"],
                )
                speaker.insert(session)

            talk = Talk(
                speaker_name=speaker_name,
                conference_name=conf_name,
                conference_year=conf_year,
                talk_title=row["talk_title"],
                company=row["company"],
            )
            talk.insert(session)
    except IntegrityError:
        session.rollback()
        logger.info(
            f"Conference '{conf_name} ({conf_year})' already exists in the database."
        )
    except Exception as e:
        session.rollback()
        logger.error(f"Failed to insert conference and speakers - ERROR: {e}")
        raise


# -------------------------- LinkedIn User CRUD Operations -------------------------- #


def get_linkedIn_users(session):
    """
    Load all LinkedIn users from the database.

    Returns:
    - pd.DataFrame: Dataframe containing LinkedIn users data.
    """
    logger.info("Loading LinkedIn users from the database...")

    try:
        linkedIn_users = session.query(LinkedInUser).all()
        linkedIn_users_data = [
            {
                "linkedin_url": linkedIn_user.profile_url,
                "name": linkedIn_user.name,
                "norm_name": normalize_name(linkedIn_user.name),
            }
            for linkedIn_user in linkedIn_users
        ]
        df = pd.DataFrame(linkedIn_users_data)
        logging.info("Successfully fetched LinkedIn users from database.")
        return df
    except Exception as e:
        logger.error(f"An error occurred while fetching linkedIn users data: {e}")
        return []


# -------------------------- User Company Association CRUD Operations -------------------------- #


def get_user_companies(session, filter=None):
    try:
        if not filter:
            logger.info(
                "Loading companies associated with LinkedIn users from the database..."
            )
        user_companies = session.query(UserCompanyAssociation)

        if filter:
            user_companies = user_companies.filter_by(user_profile_url=filter)

        user_companies = user_companies.all()

        if not user_companies:
            if not filter:
                logging.warning("No user companies found in the database.")
            return []

        user_companies_data = [
            {
                "association_id": user_company.association_id,
                "user_profile_url": user_company.user_profile_url,
                "company_profile_url": user_company.company_profile_url,
                "status_code": user_company.status_code,
            }
            for user_company in user_companies
        ]
        if not filter:
            logger.info(
                "Successfully fetched companies associated with LinkedIn users."
            )
        return user_companies_data
    except Exception as e:
        logger.error(f"An error occurred while fetching user companies data: {e}")
        raise


def insert_user_companies(session, row, status_code):
    try:
        profile_url = row["profileUrl"]
        company_url = row["query"]

        # Helper function to check existence and insert if not exists
        def check_and_insert(model, profile_url, **kwargs):
            exists = (
                session.query(model)
                .filter(model.profile_url == profile_url)
                .one_or_none()
            )
            if not exists:
                instance = model(profile_url=profile_url, **kwargs)
                # instance.insert(session)  # Uncomment this to perform the insert
                return instance
            return exists

        # Check and insert LinkedIn User
        check_and_insert(
            LinkedInUser,
            profile_url,
            name=row["name"],
            first_name=row["firstName"],
            last_name=row["lastName"],
            description=row["job"],
            location=row["location"],
        )

        # Check and insert LinkedIn Company
        check_and_insert(LinkedInCompany, company_url, name=row["company"])

        # Create UserCompanyAssociation
        user_company_association = UserCompanyAssociation(
            user_profile_url=profile_url,
            company_profile_url=company_url,
            status_code=status_code,
        )
        # user_company_association.insert(session)  # Uncomment this to perform the insert

    except Exception as e:
        session.rollback()
        logger.error(f"Failed to insert employee and company: {e}")
        raise


# --------------------------- Helper Functions ------------------------ #


def normalize_name(name: str) -> str:
    """
    Normalizes speaker names by removing accents and converting to lowercase.

    Args:
        name (str): Speaker's name.

    Returns:
        str: Normalized speaker name.
    """
    return unidecode(name).strip().lower()


# def update_user_companies_status_code(df, session):
#     updated_counter = 0
#     non_updated_counter = 0

#     for _, row in df.iterrows():
#         association_id = row["association_id"]
#         user_company_record = (
#             session.query(UserCompanyAssociation)
#             .filter(UserCompanyAssociation.association_id == association_id)
#             .one_or_none()
#         )

#         if user_company_record:
#             status_code = user_company_record.status_code
#             if status_code in [1, 2]:
#                 user_company_record.status_code = 0
#                 updated_counter += 1
#                 # user_company_record.update(session)
#             else:
#                 non_updated_counter += 1
#         else:
#             logger.info(
#                 f"Cannot find user company association which id is {association_id}"
#             )
#             print(f"Cannot find user company association which id is {association_id}")
#     logger.info(f"I have update status codes for {updated_counter} employees.")
#     print(
#         f"I have update status codes for {updated_counter} employees. Not updated for {non_updated_counter} employees."
#     )
