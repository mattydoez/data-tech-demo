from faker import Faker
import random
import json
import time

# Initialize Faker library
fake = Faker()

def generate_product_data():
    """
    Generate fake digital marketing data.
    """
    product_data = {
        "product_id": fake.uuid4(),
        "name": '' ,
        "category": random.choice(["Electronics","Clothing and accessories","Home appliances","Furniture and home decor","Beauty and personal care products","Sports and fitness equipment","Toys and games","Books and stationery","Automotive parts and accessories","Outdoor and gardening supplies","Pet supplies","Health and wellness products","Jewelry and watches","Art and craft supplies","Food and beverages","Travel accessories","Musical instruments","Office supplies","Baby products","Party supplies"]),
        "description": random.randint(10, 100),
        "created_at": fake.date_time_between(start_date="-1h", end_date="now").isoformat(),
        "price": round(random.uniform(10, 100), 2)
    }
    return product_data

def main():
    """
    Generate fake data and write to JSON files.
    """
    while True:
        # Generate fake user data
        products = [generate_product_data() for _ in range(1, 200)]   

        # Write generated data to JSON files
        with open("fake_products.json", "w") as f:
            json.dump(products, f, indent=4)

if __name__ == "__main__":
    main()

