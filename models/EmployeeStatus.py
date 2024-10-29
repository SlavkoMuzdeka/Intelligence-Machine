from enum import Enum


class EmployeeStatus(Enum):
    CURRENT = 0  # Represents that nothing has happened; the employee is still with the company
    NEW = 1  # Represents a new employment status
    SWITCHED = 2  # Represents a change in employment (e.g., to a different company)
    FORMER = 3  # Represents that the employee is no longer with the company
