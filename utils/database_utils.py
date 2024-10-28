import os
import logging
import psycopg2
import numpy as np
import pandas as pd

from unidecode import unidecode
from sqlalchemy import create_engine
from models.database.models import Base
from psycopg2 import sql, OperationalError
from sqlalchemy.orm import sessionmaker, joinedload
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from models.database.models import (
    Talk,
    Speaker,
    Conference,
    LinkedInUser,
    LinkedInCompany,
)

logger = logging.getLogger(__name__)


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


def update_conf_speakers(df, column_name="name"):
    """
    Updates LinkedIn URLs of conference speakers in the database.

    Args:
        df (pd.DataFrame): DataFrame containing speaker data.
        column_name (str): The column name corresponding to speaker names in the DataFrame.
    """
    logger.info("Updating speakers LinkedIn URLs in the database...")
    session, _ = create_database_session_and_engine()

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


def insert_conf_speakers(df):
    """
    Inserts conference and speaker data into the database.

    Args:
        df (pd.DataFrame): DataFrame containing speaker and conference details.
    """
    df = df.replace({np.nan: None})
    conf_name = df.loc[0, "conf_name"]
    conf_year = int(df.loc[0, "conf_year"])

    session, _ = create_database_session_and_engine()

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


def get_conf_speakers(session, filter=False):
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


def get_conf_talks(session):
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


def get_linkedIn_users(session):
    """
    Load all LinkedIn users from the database.

    Returns:
    - pd.DataFrame: Dataframe containing LinkedIn users data.
    """
    logger.info("Loading LinkedIn users from the database...")

    try:
        linkedIn_users = (
            session.query(LinkedInUser)
            .options(joinedload(LinkedInUser.companies))
            .all()
        )
        linkedIn_users_data = [
            {
                "linkedin_url": linkedIn_user.profile_url,
                "name": linkedIn_user.name,
                "norm_name": normalize_name(linkedIn_user.name),
                "firt_name": linkedIn_user.first_name,
                "last_name": linkedIn_user.last_name,
                "description": linkedIn_user.description,
                "location": linkedIn_user.location,
                "companies": [
                    {"company_name": company.name, "company_url": company.profile_url}
                    for company in linkedIn_user.companies
                ],
            }
            for linkedIn_user in linkedIn_users
        ]
        df = pd.DataFrame(linkedIn_users_data)
        logging.info("Successfully fetched LinkedIn users from database.")
        return df
    except Exception as e:
        logger.error(f"An error occurred while fetching linkedIn users data: {e}")
        return []


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


def insert_company_employees(df, session):
    """
    Inserts LinkedIn company and employee data into the database.

    Args:
        df (pd.DataFrame): DataFrame containing employee details.
        session (Session): Active SQLAlchemy session for database interaction.
    """
    logging.info("Inserting companies and employees data into the database...")

    num_of_existing_users = 0
    num_of_added_users = 0

    num_of_added_companies = 0

    logger.info(
        f"Inserting {len(df['profileUrl'].unique())} employee/s into the database..."
    )
    logger.info(
        f"Inserting {len(df['query'].unique())} company/ies into the database..."
    )
    try:
        for _, row in df.iterrows():
            profile_url = row["profileUrl"]
            company_url = row["query"]

            # Check if LinkedIn User exists
            user_exists = (
                session.query(LinkedInUser)
                .filter(LinkedInUser.profile_url == profile_url)
                .one_or_none()
            )

            # Insert new speaker if not exists
            if not user_exists:
                user = LinkedInUser(
                    profile_url=profile_url,
                    name=row["name"],
                    first_name=row["firstName"],
                    last_name=row["lastName"],
                    description=row["job"],
                    location=row["location"],
                )

                user.insert(session)
                num_of_added_users += 1
            else:
                num_of_existing_users += 1

            # Check if LinkedIn Company exists
            company_exists = (
                session.query(LinkedInCompany)
                .filter(LinkedInCompany.profile_url == company_url)
                .one_or_none()
            )

            if not company_exists:
                num_of_added_companies += 1
                company = LinkedInCompany(profile_url=company_url, name=row["company"])
                company.insert(session)

            # Associate user with the company if not already associated
            if company and user:
                if company not in user.companies:
                    user.companies.append(company)
                    user.update(session)

        logger.info(
            f"Inserted {num_of_added_users} new employee/s into the database ({num_of_existing_users} employees already existed)."
        )
        logger.info(
            f"Inserted {num_of_added_companies} new company/ies into the database."
        )
        logger.info("Successfully inserted employees and companies into the database.")
    except Exception as e:
        session.rollback()
        logger.error(f"Failed to insert employees and companies: {e}")
        raise


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


def normalize_name(name: str) -> str:
    """
    Normalizes speaker names by removing accents and converting to lowercase.

    Args:
        name (str): Speaker's name.

    Returns:
        str: Normalized speaker name.
    """
    return unidecode(name).strip().lower()
