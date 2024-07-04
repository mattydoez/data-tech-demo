import random
import json
import time
import logging
from faker import Faker

# Initialize Faker library
fake = Faker()

# Set up logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def generate_user():
    """
    Generate fake user data.
    """
    user = {
        "username": fake.user_name(),
        "email": fake.email(),
        "address": fake.address(),
        "phone_number": fake.phone_number()
    }
    return user

def main():
    """
    Generate fake data and write to JSON files.
    """
    try:
        logger.info("Generating fake user data...")
        users = [generate_user() for _ in range(random.randint(1, 100))]
        logger.info(f"Generated {len(users)} users.")

        with open("/opt/airflow/generator/fake_users.json", "w") as f:
            json.dump(users, f, indent=4)
            
    except Exception as e:
        logger.error(f"Error during data generation: {e}")

if __name__ == "__main__":
    main()