import random
import json
import time
from faker import Faker

# Initialize Faker library
fake = Faker()

def generate_user():
    """
    Generate fake user data.
    """
    user = {
        "user_id": fake.uuid4(),
        "username": fake.user_name(),
        "email": fake.email(),
        "address": fake.address(),
        "phone_number": fake.phone_number()
    }
    return user

def generate_clickstream_event(users, user_session):
    """
    Generate fake clickstream event data.
    """
    common_ecomm_pages_with_products = ["product", "category", "cart", "checkout"]
    common_ecomm_pages_without_products = ["home", "search", "profile", "orders", "wishlist", "about", "contact", "faq", "returns", "terms", "privacy"]

    user = random.choice(users)
    page_visited = random.choice(common_ecomm_pages_with_products + common_ecomm_pages_without_products)

    if page_visited in common_ecomm_pages_with_products:
        action = random.choice(["click", "add_to_cart", "purchase"])
    else:
        action = random.choice(["click", "visit"])

    # Ensure logical sequence of actions for purchase
    if action == "purchase":
        if user["user_id"] not in user_session or "add_to_cart" not in user_session[user["user_id"]]:
            action = "add_to_cart"

    # Update user session with the action
    if user["user_id"] not in user_session:
        user_session[user["user_id"]] = []

    user_session[user["user_id"]].append(action)

    # Ensure that product_id is only added for relevant actions
    product_id = fake.random_int(min=1, max=1000) if action in ["add_to_cart", "purchase"] else None

    event = {
        "event_id": fake.uuid4(),
        "user_id": user["user_id"],
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

def generate_transaction_data(users):
    """
    Generate fake transaction data.
    """
    user = random.choice(users)
    transaction_data = {
        "transaction_id": fake.uuid4(),
        "timestamp": fake.date_time_between(start_date="-1h", end_date="now").isoformat(),
        "amount": weighted_random_amount(),
        "product_id": random.randint(0, 200),
        "user_id": user["user_id"],
        "payment_method": random.choice(["Visa", "Klarna", "Mastercard", "Affirm", "Zelle", "Apple Pay"]),
        "transaction_type": random.choice([0, 1, 2])
    }
    return transaction_data

def generate_google_search_data():
    """
    Generate fake Google Search marketing data.
    """
    google_search_data = {
        "date": fake.date_time_between(start_date="-1h", end_date="now").isoformat(),
        "campaign": fake.catch_phrase(),
        "ad": fake.bs(),
        "impressions": random.randint(0, 50000),
        "clicks": random.randint(0, 1000),
        "cost": round(random.uniform(0, 500), 2),
        "conversions": random.randint(0, 10)
    }
    return google_search_data

def generate_email_marketing_data():
    """
    Generate fake Email Marketing data.
    """
    email_marketing_data = {
        "date": fake.date_time_between(start_date="-1h", end_date="now").isoformat(),
        "user_email": fake.email(),
        "campaign": fake.catch_phrase(),
        "received": random.choice([0, 1]),
        "opened": random.choice([0, 1]),
        "subscribed": random.choice([0, 1]),
        "clicks": random.randint(0, 100),
        "bounces": random.randint(0, 10),  # additional typical email field
        "unsubscribed": random.choice([0, 1])  # additional typical email field
    }
    return email_marketing_data

def generate_facebook_data():
    """
    Generate fake Facebook/Instagram marketing data.
    """
    facebook_data = {
        "platform": random.choice(["Facebook", "Instagram"]),
        "date": fake.date_time_between(start_date="-1h", end_date="now").isoformat(),
        "campaign": fake.catch_phrase(),
        "ad_unit": fake.bs(),
        "impressions": random.randint(0, 50000),
        "percent_watched": round(random.uniform(0, 100), 2),
        "clicks": random.randint(0, 1000),
        "cost": round(random.uniform(0, 500), 2),
        "conversions": random.randint(0, 10),
        "likes": random.randint(0, 500),  # additional typical social field
        "shares": random.randint(0, 100),  # additional typical social field
        "comments": random.randint(0, 100)  # additional typical social field
    }
    return facebook_data
def main():
    """
    Generate fake data and write to JSON files.
    """
    while True:
        # Generate fake user data
        users = [generate_user() for _ in range(random.randint(1, 500))]
        
        # Initialize user session dictionary
        user_session = {}
        
        # Generate clickstream events and associate with users
        clickstream_events = [generate_clickstream_event(users, user_session) for _ in range(random.randint(1, 100))]
        
        # Generate transaction data and associate with users
        transaction_data = [generate_transaction_data(users) for _ in range(random.randint(1, 100))]
        
        # Generate fake marketing data
        google_search_data = [generate_google_search_data() for _ in range(0, 1000)]

        email_marketing_data = [generate_email_marketing_data() for _ in range(0, 1000)]

        facebook_data = [generate_facebook_data() for _ in range(0, 1000)]
        
        # Write generated data to JSON files
        with open("fake_users.json", "w") as f:
            json.dump(users, f, indent=4)
        with open("fake_clickstream_events.json", "w") as f:
            json.dump(clickstream_events, f, indent=4)
        with open("fake_google_search_data.json", "w") as f:
            json.dump(google_search_data, f, indent=4)
        with open("fake_email_marketing_data.json", "w") as f:
            json.dump(email_marketing_data, f, indent=4)
        with open("fake_facebook_data.json", "w") as f:
            json.dump(facebook_data, f, indent=4)
        with open("fake_transaction_data.json", "w") as f:
            json.dump(transaction_data, f, indent=4)    
        
        # Sleep for 10 minutes before generating next batch of data
        time.sleep(600)

if __name__ == "__main__":
    main()
