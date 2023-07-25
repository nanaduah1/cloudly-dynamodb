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


student = Student.itmes.get(pk='123455', sk='sk3939393') # Get a single record
all_students = Student.items.all(pk='123444', sk__beginswith='STUDENT#12') # Get one or more. Results is an iterable


```

These are designed to suit my current needs and there are not guaranteed to work for you.
Please use it at your own risk. I'm not responsible nor provide any guarantees that this is bug free and safe for your own use cases!
