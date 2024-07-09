import pandas as pd
import random
from faker import Faker

# Initialize Faker
fake = Faker()

# Function to generate a single record
def generate_record():
    return {
        "First Name": fake.first_name(),
        "Middle Name": fake.random_letter().upper() + '.',
        "Last Name": fake.last_name(),
        "Date of Birth": fake.date_of_birth(minimum_age=18, maximum_age=90).strftime('%Y-%m-%d'),
        "ULI": fake.unique.random_int(min=10000, max=99999)
    }

# Generate 1000 unique records
fake_data = [generate_record() for _ in range(1000)]

# Convert to DataFrame
df = pd.DataFrame(fake_data)

# Introduce some duplicates with slight modifications
num_duplicates = 100
for _ in range(num_duplicates):
    row = df.sample().iloc[0].to_dict()
    # Randomly change one field to create a slight difference
    field_to_modify = random.choice(["First Name", "Middle Name", "Last Name", "Date of Birth"])
    if field_to_modify == "First Name":
        row["First Name"] = fake.first_name()
    elif field_to_modify == "Middle Name":
        row["Middle Name"] = fake.random_letter().upper() + '.'
    elif field_to_modify == "Last Name":
        row["Last Name"] = fake.last_name()
    elif field_to_modify == "Date of Birth":
        row["Date of Birth"] = fake.date_of_birth(minimum_age=18, maximum_age=90).strftime('%Y-%m-%d')
    
    df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)


