import logging

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import relationship, declarative_base
from sqlalchemy import Column, ForeignKey, Integer, String, ForeignKeyConstraint

Base = declarative_base()

logger = logging.getLogger(__name__)


class Speaker(Base):
    """
    Represents a speaker with a unique name and a website URL.

    Attributes:
        name (str): The speaker's unique name (Primary Key).
        website_url (str): The URL of the speaker's website.
        linkedIn_url (str): The LinkedIn URL of the speaker.
    """

    __tablename__ = "speaker"

    name = Column(String, primary_key=True)
    website_url = Column(String)
    linkedIn_url = Column(String)

    def __init__(self, name: str, website_url: str, linkedIn_url: str):
        """
        Initializes a Speaker instance with the provided name and website URL.

        Args:
            name (str): The speaker's name.
            website_url (str): The URL of the speaker's website.
            linkedIn_url (str): The LinkedIn URL of the speaker.
        """
        self.name = name
        self.website_url = website_url
        self.linkedIn_url = linkedIn_url

    def insert(self, session):
        """
        Adds the Speaker instance to the database and commits the transaction.

        Raises:
            IntegrityError: If the speaker already exists in the database.

        Example:
            speaker = Speaker(name="Name", website_url="https://x.com/name", linkedIn_url="https://linkedin.com/user")
            speaker.insert(session)
        """
        try:
            speaker_info = f"{self.name} - {self.website_url} - {self.linkedIn_url}"
            session.add(self)
            session.commit()
        except IntegrityError:
            session.rollback()
            logger.error(f"Cannot insert speaker - {speaker_info}")

    def update(self, session):
        """
        Commits any changes made to the Speaker instance to the database.
        The Speaker instance must already exist in the database.

        Example:
            speaker = Speaker.query.filter(Speaker.name == name).one_or_none()
            speaker.website_url = "https://x.com/name"
            speaker.update()
        """
        session.commit()

    def delete(self, session):
        """
        Deletes the Speaker instance from the database and commits the transaction.
        The Speaker instance must already exist in the database.

        Example:
            speaker = Speaker.query.filter(Speaker.name == name).one_or_none()
            speaker.delete()
        """
        session.delete(self)
        session.commit()

    def format(self):
        """
        Returns a dictionary representation of the Speaker instance.

        Returns:
            dict: A dictionary with the speaker's name and website URL.
        """
        return {
            "name": self.name,
            "website_url": self.website_url,
            "linkedIn_url": self.linkedIn_url,
        }


class Conference(Base):
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
        """
        Initializes a Conference instance with the provided name and year.

        Args:
            name (str): The name of the conference.
            year (int): The year of the conference.
        """
        self.name = name
        self.year = year

    def insert(self, session):
        """
        Adds the Conference instance to the database and commits the transaction.

        Example:
            conference = Conference(name="TechConf", year=2024)
            conference.insert(session)
        """
        session.add(self)
        session.commit()

    def update(self, session):
        """
        Commits any changes made to the Conference instance to the database.
        The Conference instance must already exist in the database.

        Example:
            conference = Conference.query.filter_by(name="TechConf", year=2024).one_or_none()
            conference.update()
        """
        session.commit()

    def delete(self, session):
        """
        Deletes the Conference instance from the database and commits the transaction.
        The Conference instance must already exist in the database.

        Example:
            conference = Conference.query.filter_by(name="TechConf", year=2024).one_or_none()
            conference.delete()
        """
        session.delete(self)
        session.commit()

    def format(self):
        """
        Returns a dictionary representation of the Conference instance.

        Returns:
            dict: A dictionary with the conference's name and year.
        """
        return {"name": self.name, "year": self.year}


class Talk(Base):
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
        """
        Initializes a Talk instance with the provided details.

        Args:
            speaker_name (str): The name of the speaker giving the talk.
            conference_name (str): The name of the conference.
            conference_year (int): The year of the conference.
            talk_title (str): The title of the talk.
            company (str): The company where the speaker works.
        """
        self.speaker_name = speaker_name
        self.conference_name = conference_name
        self.conference_year = conference_year
        self.talk_title = talk_title
        self.company = company

    def insert(self, session):
        """
        Adds the Talk instance to the database and commits the transaction.

        Raises:
            IntegrityError: If the talk already exists or references invalid foreign keys.

        Example:
            talk = Talk(speaker_name="John Doe", conference_name="TechConf", conference_year=2024, talk_title="Future of AI", company="Some company")
            talk.insert(session)
        """
        try:
            talk_info = f"{self.speaker_name} - {self.conference_name} - {self.conference_year} - {self.talk_title} - {self.company}"
            session.add(self)
            session.commit()
        except IntegrityError:
            session.rollback()
            logger.error(f"Cannot insert talk - {talk_info}")

    def update(self, session):
        """
        Commits any changes made to the Talk instance to the database.
        The Talk instance must already exist in the database.

        Example:
            talk = Talk.query.filter_by(talk_id=1).one_or_none()
            talk.talk_title = "New talk title"
            talk.update()
        """
        session.commit()

    def delete(self, session):
        """
        Deletes the Talk instance from the database and commits the transaction.
        The Talk instance must already exist in the database.

        Example:
            talk = Talk.query.filter_by(talk_id=1).one_or_none()
            talk.delete()
        """
        session.delete(self)
        session.commit()

    def format(self):
        """
        Returns a dictionary representation of the Talk instance.

        Returns:
            dict: A dictionary with the talk's details, including ID, speaker name, conference details, talk title.
        """
        return {
            "talk_id": self.talk_id,
            "speaker_name": self.speaker_name,
            "conference_name": self.conference_name,
            "conference_year": self.conference_year,
            "talk_title": self.talk_title,
            "company": self.company,
        }


class LinkedInUser(Base):
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
        """
        Initializes a LinkedInUser instance with the provided details.
        """
        self.profile_url = profile_url
        self.name = name
        self.first_name = first_name
        self.last_name = last_name
        self.description = description
        self.location = location

    def insert(self, session):
        """
        Adds the LinkedInUser instance to the database and commits the transaction.

        Raises:
            IntegrityError: If the user already exists or references invalid foreign keys.

        Example:
            user = LinkedInUser(profile_url="https://www.linkedin.com/in/username/", name="first_name last_name", first_name="first_name", last_name="last_name", description="Description", location="location")
            user.insert(user)
        """
        try:
            session.add(self)
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Error inserting LinkedInUser: {e}")

    def update(self, session):
        """
        Commits any changes made to the LinkedInUser instance to the database.
        The LinkedInUser instance must already exist in the database.

        Example:
            user = LinkedInUser.query.filter_by(profile_url="https://www.linkedin.com/in/username/").one_or_none()
            user.first_name = "New first name"
            user.update()
        """
        try:
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Error updating LinkedInUser: {e}")

    def delete(self, session):
        """
        Deletes the LinkedInUser instance from the database and commits the transaction.
        The LinkedInUser instance must already exist in the database.

        Example:
            user = LinkedInUser.query.filter_by(profile_url="https://www.linkedin.com/in/username/").one_or_none()
            user.delete()
        """
        try:
            session.delete(self)
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Error deleting LinkedInUser: {e}")

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


class LinkedInCompany(Base):
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
        """
        Initializes a LinkedInCompany instance with the provided details.
        """
        self.profile_url = profile_url
        self.name = name

    def insert(self, session):
        """
        Adds the LinkedInCompany instance to the database and commits the transaction.

        Raises:
            IntegrityError: If the company already exists or references invalid foreign keys.

        Example:
            company = LinkedInCompany(profile_url="https://www.linkedin.com/in/company_name/", name="company_name")
            company.insert(company)
        """
        try:
            session.add(self)
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Error inserting LinkedInCompany: {e}")

    def update(self, session):
        """
        Commits any changes made to the LinkedInCompany instance to the database.
        The LinkedInCompany instance must already exist in the database.

        Example:
            company = LinkedInCompany.query.filter_by(profile_url="https://www.linkedin.com/in/company_name/").one_or_none()
            company.name = "New company name"
            company.update()
        """
        try:
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Error updating LinkedInCompany: {e}")

    def delete(self, session):
        """
        Deletes the LinkedInCompany instance from the database and commits the transaction.
        The LinkedInCompany instance must already exist in the database.

        Example:
            company = LinkedInCompany.query.filter_by(profile_url="https://www.linkedin.com/in/company_name/").one_or_none()
            company.delete()
        """
        try:
            session.delete(self)
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Error deleting LinkedInCompany: {e}")

    def format(self):
        """
        Returns a dictionary representation of the LinkedInCompany instance.
        """
        return {
            "profile_url": self.profile_url,
            "name": self.name,
        }


class UserCompanyAssociation(Base):
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
        """
        Initializes a UserCompanyAssociation instance with the provided details.
        """
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

    def insert(self, session):
        """
        Adds the UserCompanyAssociation instance to the database and commits the transaction.

        Raises:
            IntegrityError: If the UserCompanyAssociation already exists or references invalid foreign keys.

        Example:
            user_company_association = UserCompanyAssociation(user_profile_url='<profile_url>', company_profile_url='<company_url>', status_code=0)
            user_company_association.insert(session)
        """
        try:
            user_company_association_info = f"{self.user_profile_url} - {self.company_profile_url} - {self.status_code}"
            session.add(self)
            session.commit()
        except IntegrityError:
            session.rollback()
            logger.error(
                f"Cannot insert user company association - {user_company_association_info}"
            )

    def update(self, session):
        """
        Commits any changes made to the UserCompanyAssociation instance to the database.
        The UserCompanyAssociation instance must already exist in the database.

        Example:
            user_company_association = UserCompanyAssociation.query.filter_by(association_id=1).one_or_none()
            user_company_association.status_code = 2
            user_company_association.update()
        """
        session.commit()

    def delete(self, session):
        """
        Deletes the UserCompanyAssociation instance from the database and commits the transaction.
        The UserCompanyAssociation instance must already exist in the database.

        Example:
            user_company_association = UserCompanyAssociation.query.filter_by(association_id=1).one_or_none()
            user_company_association.delete()
        """
        session.delete(self)
        session.commit()

    def format(self):
        """
        Returns a dictionary representation of the UserCompanyAssociation instance.
        """
        return {
            "user_profile_url": self.user_profile_url,
            "company_profile_url": self.company_profile_url,
            "status_code": self.status_code,
        }
