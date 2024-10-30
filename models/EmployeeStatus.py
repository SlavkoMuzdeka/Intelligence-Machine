from enum import Enum


class EmployeeStatus(Enum):
    ACTIVE = 0  # The employee is currently with the company
    NEW_HIRE = 1  # Represents a recently hired employee
    TRANSFERRED = 2  # Represents a change in employment status (e.g., moved to a different department or company)
    FORMER_EMPLOYEE = 3  # Represents an employee who has left the company
