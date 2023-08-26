# Cloudly-Dynamodb

This repository contains data access classes to simplify writing python code that interacts with
dynamodb tables.

# Version 2 Data Models

Data models allow you do define your dynamodb items as strongly typed objects.
Example:

```Python
from datetime import datetime
import boto3

table = boto3.resource('dynamodb').Table('STUDENT')



@dataclass
class Student(DynamodbItem):
    firstName: str
    lastName: str
    dateOfBirth: datetime

    class Meta:
        dynamo_table = table

# Get a single record
student = Student.itmes.get(id='123455')

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

By default, the partition key is generated from the fully qualified class name of
the model. The sort key is generated from the class name of the model and the instance id.

If this is not what you want, you can override the partition key and sort key by overriding
the \_create_pk and \_create_sk methods.

Example:

```Python

@dataclass
class Student(DynamodbItem):
    firstName: str
    lastName: str
    dateOfBirth: datetime

    class Meta:
        data_table = table

    @classmethod
    def _create_pk(cls, **kwargs):
        first_name = kwargs.get('firstName')
        return f'STUDENT#{firstName}'

    @classmethod
    def _create_sk(cls, **kwargs):
        return f'STUDENT#{kwargs.get('id')}'

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
