# Cloudly-Dynamodb

This repository contains data access classes to simplify writing python code that interacts with
dynamodb tables.

# Version 2 Data Models

Data models allow you do define your dynamodb items as strongly typed objects.
Example:

```Python
from datetime import datetime


@dataclass
class Student(Model):
    firstName: str
    lastName: str
    dateOfBirth: datetime

    class Meta:
        data_table = 'STUDENT'
        index_name = 'STUDENT_INDEX'
        pk = 'pk'
        sk = 'sk'

# Get records
student = Student.itmes.get(pk='123455', sk='sk3939393') # Get a single record
all_students = Student.items.all(pk='123444', sk__beginswith='STUDENT#12') # Get one or more. Results is an iterable

# Save records
student = Student(firstName='John', lastName='Doe', dateOfBirth=datetime.now())
student.save() # Save a new record

Student.items.create(firstName='John', lastName='Doe', dateOfBirth=datetime.now()) # Create a new record


# Delete records
Student.items.delete(pk='123455', sk='sk3939393') # Delete a single record
Student.items.delete(pk='123444', sk__beginswith='STUDENT#12') # Delete one or more records using a filter

```
