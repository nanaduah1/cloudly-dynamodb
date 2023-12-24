# Cloudly-Dynamodb

This repository contains data access classes to simplify writing python code that interacts with
dynamodb tables.

# Version 2 Data Models

Data models allow you do define your dynamodb items as strongly typed objects.
Example:

```Python
from datetime import datetime
import boto3

@dataclass
class Student(DynamodbItem):
    firstName: str
    lastName: str
    dateOfBirth: datetime

    class Meta:
        table = 'STUDENT_TABLE'

# Get a single record
student = Student.items.get(id='123455')

# Get all records
all_students = Student.items.all()
all_students = Student.items.all(lambda q: q.sk_beginswith('KUMASI')) # Get one or more where sk begins with KUMASI


# Save records
student = Student(firstName='John', lastName='Doe', dateOfBirth=datetime.now())
student.save() # Save a new record

# Update record
student = Student.items.get(id='123455')
student.firstName = 'Jane'
student.save()

# Or update directly
Student.items.update(id='123455', firstName='Jane')

# Create directly from the model
Student.items.create(firstName='John', lastName='Doe', dateOfBirth=datetime.now()) # Create a new record


# Delete records
Student.items.delete(id="123444") # Delete a single record
```

## Controlling the partition key and sort key

You can control how the partition key and sort key are created by setting
key of the model class.

Example:

```Python

class MyItemKeyClass(DefaultItemKeyFactory):
   def for_create(self):
        return {
            'pk': f'STUDENT#{self._kwargs["firstName"]}',
            'sk': f'STUDENT#{self._kwargs["id"]}'
        }


@dataclass
class Student(DynamodbItem):
    firstName: str
    lastName: str
    dateOfBirth: datetime

    class Meta:
        table = table
        key = MyItemKeyClass
```

Note that any values you include in the partition key or sort key
becomes a required field when creating or querying. For instance in the example above,
if you want to query for a student, you must provide the firstName and id.

```Python
Student.items.get(firstName='John', id='123455')
```

## Migration from version 1

Version 2 is generally backwards compatible with version 1. However, there are some breaking changes.
The following changes need to be made to your code to migrate from version 1 to version 2.

1. Change all `from cloudly.db.dynamodb import XXXX` to `from cloudly.core.dynamodb import XXXX`
