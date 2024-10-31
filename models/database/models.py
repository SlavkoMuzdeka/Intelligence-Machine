import logging

from sqlalchemy.orm import relationship, declarative_base
from sqlalchemy import Column, ForeignKey, Integer, String, ForeignKeyConstraint

Base = declarative_base()

logger = logging.getLogger(__name__)


class BaseModel(Base):
    __abstract__ = True

    def insert(self, session):
        """Adds the instance to the database and commits the transaction."""
        try:
            session.add(self)
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(
                f"Error inserting {self.__class__.__name__} - {self.format()}: {e}"
            )

    def update(self, session):
        """Commits any changes made to the instance to the database."""
        try:
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(
                f"Error updating {self.__class__.__name__} - {self.format()}: {e}"
            )

    def delete(self, session):
        """Deletes the instance from the database and commits the transaction."""
        try:
            session.delete(self)
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Error deleting {self.__class__.__name__}: {e}")

    def format(self):
        """Returns a dictionary representation of the instance."""
        return {
            column.name: getattr(self, column.name) for column in self.__table__.columns
        }


# -------------------------- Conference Speaker Classes -------------------------- #


class Speaker(BaseModel):
    """
    Represents a speaker with a unique name, website URL and LinkedIn URL.

    Attributes:
        name (str): The speaker's unique name (Primary Key).
        website_url (str): The URL of the speaker's website.
        linkedin_url (str): The LinkedIn URL of the speaker.
    """

    __tablename__ = "speaker"

    name = Column(String, primary_key=True)
    website_url = Column(String)
    linkedin_url = Column(String)

    def __init__(self, name: str, website_url: str, linkedin_url: str):
        self.name = name
        self.website_url = website_url
        self.linkedin_url = linkedin_url


class Conference(BaseModel):
    """
    Represents a conference with a name and year.

    Attributes:
        name (str): The conference's name (Primary Key).
        year (int): The year the conference took place (Primary Key).
    """

    __tablename__ = "conference"

    name = Column(String, primary_key=True)
    year = Column(Integer, primary_key=True)

    def __init__(self, name: str, year: int):
        self.name = name
        self.year = year


class Talk(BaseModel):
    """
    Represents a talk given at a conference by a speaker.

    Attributes:
        talk_id (int): The unique identifier for the talk (Primary Key).
        speaker_name (str): The name of the speaker giving the talk (Foreign Key).
        conference_name (str): The name of the conference where the talk was given.
        conference_year (int): The year of the conference.
        talk_title (str): The title of the talk.
        company (str): The company where the speaker works.
    """

    __tablename__ = "talk"

    talk_id = Column(Integer, primary_key=True, autoincrement=True)
    speaker_name = Column(String, ForeignKey("speaker.name"), nullable=False)
    conference_name = Column(String, nullable=False)
    conference_year = Column(Integer, nullable=False)
    talk_title = Column(String)
    company = Column(String)

    # Define foreign key constraints
    __table_args__ = (
        ForeignKeyConstraint(
            ["conference_name", "conference_year"],
            ["conference.name", "conference.year"],
        ),
    )

    def __init__(
        self,
        speaker_name: str,
        conference_name: str,
        conference_year: str,
        talk_title: str,
        company: str,
    ):
        self.speaker_name = speaker_name
        self.conference_name = conference_name
        self.conference_year = conference_year
        self.talk_title = talk_title
        self.company = company


# -------------------------- Company Employees Classes -------------------------- #


class LinkedInUser(BaseModel):
    """
    Represents a LinkedIn user profile.

    Attributes:
        profile_url (str): The unique URL of the user's LinkedIn profile (Primary Key).
        name (str): The full name of the user.
        first_name (str): The first name of the user.
        last_name (str): The last name of the user.
        description (str): The user's profile description.
        location (str): The user's location.
    """

    __tablename__ = "linkedin_user"

    profile_url = Column(String, primary_key=True)
    name = Column(String)
    first_name = Column(String)
    last_name = Column(String)
    description = Column(String)
    location = Column(String)

    companies = relationship(
        "LinkedInCompany",
        secondary="user_company_association",
        back_populates="employees",
    )

    def __init__(
        self,
        profile_url: str,
        name: str = None,
        first_name: str = None,
        last_name: str = None,
        description: str = None,
        location: str = None,
    ):
        self.profile_url = profile_url
        self.name = name
        self.first_name = first_name
        self.last_name = last_name
        self.description = description
        self.location = location

    def format(self):
        """
        Returns a dictionary representation of the LinkedInUser instance.
        """
        return {
            "profile_url": self.profile_url,
            "name": self.name,
            "first_name": self.first_name,
            "last_name": self.last_name,
            "description": self.description,
            "location": self.location,
            "status_code": self.status_code,
            "companies": [
                {"company_name": company.name, "company_url": company.profile_url}
                for company in self.companies
            ],
        }


class LinkedInCompany(BaseModel):
    """
    Represents a LinkedIn company profile.

    Attributes:
        profile_url (str): The unique URL of the company's LinkedIn profile (Primary Key).
        name (str): The name of the company.
    """

    __tablename__ = "linkedin_company"

    profile_url = Column(String, primary_key=True)
    name = Column(String, nullable=False)

    employees = relationship(
        "LinkedInUser",
        secondary="user_company_association",
        back_populates="companies",
    )

    def __init__(self, profile_url: str, name: str):
        self.profile_url = profile_url
        self.name = name


class UserCompanyAssociation(BaseModel):
    """
    Represents the association between a LinkedIn user and a company,
    with a unique employment status for each relationship.

    Attributes:
        association_id (int): Unique identifier for each association (Primary Key).
        user_profile_url (str): The LinkedIn profile URL of the user (Foreign Key).
        company_profile_url (str): The LinkedIn profile URL of the company (Foreign Key).
        status_code (int): Employment status of the user in the company.
    """

    __tablename__ = "user_company_association"

    association_id = Column(Integer, primary_key=True, autoincrement=True)
    user_profile_url = Column(
        String, ForeignKey("linkedin_user.profile_url"), nullable=False
    )
    company_profile_url = Column(
        String, ForeignKey("linkedin_company.profile_url"), nullable=False
    )
    status_code = Column(Integer, nullable=False)

    # Define foreign key constraints
    __table_args__ = (
        ForeignKeyConstraint(
            ["user_profile_url"],
            ["linkedin_user.profile_url"],
        ),
        ForeignKeyConstraint(
            ["company_profile_url"],
            ["linkedin_company.profile_url"],
        ),
    )

    def __init__(
        self, user_profile_url: str, company_profile_url: str, status_code: int
    ):
        self.user_profile_url = user_profile_url
        self.company_profile_url = company_profile_url
        self.status_code = status_code

    def set_status_code(self, code: int):
        """
        Sets the status code for the user-company relationship.

        Args:
            code (int): The status code to set.
        """
        if code in (0, 1, 2, 3):
            self.status_code = code
        else:
            raise ValueError("Invalid status code. Must be 0, 1, 2, or 3.")
