import random
import json
import time
import logging
from faker import Faker
import psycopg2
import os

# Initialize Faker library
fake = Faker()

# Set up logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_ids(table, id_column):
    """
    Fetch IDs from the database.
    """
    ids = []
    try:
        conn = psycopg2.connect(
            dbname="company_db",
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            host="company_db",
            port="5432"
        )
        cursor = conn.cursor()
        cursor.execute(f"SELECT {id_column} FROM {table}")
        ids = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
    except Exception as e:
        logger.error(f"Error fetching {id_column} from {table}: {e}")
    return ids

# Fetch user IDs and product IDs from the database
user_ids = get_ids(table='users', id_column='user_id')
product_ids = get_ids(table='products', id_column='product_id')


def generate_clickstream_event(user_session):
    """
    Generate fake clickstream event data.
    """
    common_ecomm_pages_with_products = ["product", "category", "cart", "checkout"]
    common_ecomm_pages_without_products = ["home", "search", "profile", "orders", "wishlist", "about", "contact", "faq", "returns", "terms", "privacy"]

    user_id = random.choice(user_ids)
    page_visited = random.choice(common_ecomm_pages_with_products + common_ecomm_pages_without_products)

    if page_visited in common_ecomm_pages_with_products:
        action = random.choice(["click", "add_to_cart", "purchase"])
    else:
        action = random.choice(["click", "visit"])

    # Ensure logical sequence of actions for purchase
    if action == "purchase":
        if user_id not in user_session or "add_to_cart" not in user_session[user_id]:
            action = "add_to_cart"

    # Update user session with the action
    if user_id not in user_session:
        user_session[user_id] = []

    user_session[user_id].append(action)

    # Ensure that product_id is only added for relevant actions
    product_id = random.choice(product_ids) if action in ["add_to_cart", "purchase"] else None

    event = {
        "user_id": user_id,
        "timestamp": fake.date_time_between(start_date="-1h", end_date="now").isoformat(),
        "page_visited": page_visited,
        "action": action,
        "product_id": product_id
    }
    return event

def weighted_random_amount():
    """
    Generate a random amount with a weighted probability.
    """
    if random.random() < 0.8:  # 80% chance to generate a value between 5 and 100
        return random.randint(5, 100)
    else:  # 20% chance to generate a value between 100 and 500
        return random.randint(100, 500)

def generate_transaction_data():
    """
    Generate fake transaction data.
    """
    user_id = random.choice(user_ids)
    product_id = random.choice(product_ids)
    transaction_data = {
        "timestamp": fake.date_time_between(start_date="-1h", end_date="now").isoformat(),
        "product_id": product_id,
        "user_id": user_id,
        "payment_method": random.choice(["Visa", "Klarna", "Mastercard", "Affirm", "Zelle", "Apple Pay"]),
        "transaction_type": random.choice([0, 1])
    }
    return transaction_data

def main():
    """
    Generate fake data and write to JSON files.
    """    
    
    # Initialize user session dictionary
    user_session = {}
    
    # Generate clickstream events and associate with users
    clickstream_events = [generate_clickstream_event(user_session) for _ in range(random.randint(1, 250))]
    
    # Generate transaction data and associate with users
    transaction_data = [generate_transaction_data() for _ in range(random.randint(1, 50))]
    
    # Write generated data to JSON files
    with open("/opt/airflow/generator/fake_clickstream_events.json", "w") as f:
        json.dump(clickstream_events, f, indent=4)
    with open("/opt/airflow/generator/fake_transaction_data.json", "w") as f:
        json.dump(transaction_data, f, indent=4)    
        
if __name__ == "__main__":
    main()