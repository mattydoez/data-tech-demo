import random
import json
import time
import logging
from faker import Faker
import psycopg2
import os
from datetime import datetime, timedelta

# Initialize Faker library
fake = Faker()

# Set up logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

products_with_categories = [
    ("Smartphone", "Electronics"), ("Laptop", "Electronics"), ("Tablet", "Electronics"), 
    ("Smartwatch", "Electronics"), ("Bluetooth headphones", "Electronics"), 
    ("Gaming console", "Electronics"), ("Digital camera", "Electronics"), 
    ("Wireless charger", "Electronics"), ("Portable speaker", "Electronics"), 
    ("Smart home assistant", "Electronics"), ("T-shirt", "Clothing and accessories"), 
    ("Jeans", "Clothing and accessories"), ("Jacket", "Clothing and accessories"), 
    ("Dress", "Clothing and accessories"), ("Sneakers", "Clothing and accessories"), 
    ("Sunglasses", "Clothing and accessories"), ("Handbag", "Clothing and accessories"), 
    ("Belt", "Clothing and accessories"), ("Scarf", "Clothing and accessories"), 
    ("Hat", "Clothing and accessories"), ("Refrigerator", "Home appliances"), 
    ("Washing machine", "Home appliances"), ("Microwave oven", "Home appliances"), 
    ("Blender", "Home appliances"), ("Air conditioner", "Home appliances"), 
    ("Vacuum cleaner", "Home appliances"), ("Coffee maker", "Home appliances"), 
    ("Toaster", "Home appliances"), ("Electric kettle", "Home appliances"), 
    ("Hair dryer", "Home appliances"), ("Sofa", "Furniture and home decor"), 
    ("Dining table", "Furniture and home decor"), ("Bed frame", "Furniture and home decor"), 
    ("Office chair", "Furniture and home decor"), ("Bookshelf", "Furniture and home decor"), 
    ("Desk lamp", "Furniture and home decor"), ("Wall art", "Furniture and home decor"), 
    ("Throw pillow", "Furniture and home decor"), ("Rug", "Furniture and home decor"), 
    ("Mirror", "Furniture and home decor"), ("Shampoo", "Beauty and personal care products"), 
    ("Conditioner", "Beauty and personal care products"), ("Body lotion", "Beauty and personal care products"), 
    ("Face cream", "Beauty and personal care products"), ("Lipstick", "Beauty and personal care products"), 
    ("Mascara", "Beauty and personal care products"), ("Nail polish", "Beauty and personal care products"), 
    ("Perfume", "Beauty and personal care products"), ("Electric toothbrush", "Beauty and personal care products"), 
    ("Razor", "Beauty and personal care products"), ("Treadmill", "Sports and fitness equipment"), 
    ("Yoga mat", "Sports and fitness equipment"), ("Dumbbells", "Sports and fitness equipment"), 
    ("Exercise bike", "Sports and fitness equipment"), ("Resistance bands", "Sports and fitness equipment"), 
    ("Tennis racket", "Sports and fitness equipment"), ("Football", "Sports and fitness equipment"), 
    ("Basketball", "Sports and fitness equipment"), ("Running shoes", "Sports and fitness equipment"), 
    ("Swim goggles", "Sports and fitness equipment"), ("Lego set", "Toys and games"), 
    ("Puzzle", "Toys and games"), ("Action figure", "Toys and games"), ("Board game", "Toys and games"), 
    ("Dollhouse", "Toys and games"), ("Remote control car", "Toys and games"), 
    ("Plush toy", "Toys and games"), ("Play-Doh", "Toys and games"), ("Toy train set", "Toys and games"), 
    ("Coloring book", "Toys and games"), ("Novel", "Books and stationery"), 
    ("Notebook", "Books and stationery"), ("Highlighter set", "Books and stationery"), 
    ("Fountain pen", "Books and stationery"), ("Sketchbook", "Books and stationery"), 
    ("Planner", "Books and stationery"), ("Textbook", "Books and stationery"), 
    ("Dictionary", "Books and stationery"), ("Sticky notes", "Books and stationery"), 
    ("Paper clips", "Books and stationery"), ("Car battery", "Automotive parts and accessories"), 
    ("Engine oil", "Automotive parts and accessories"), ("Windshield wipers", "Automotive parts and accessories"), 
    ("Car cover", "Automotive parts and accessories"), ("Seat covers", "Automotive parts and accessories"), 
    ("Floor mats", "Automotive parts and accessories"), ("Tire pressure gauge", "Automotive parts and accessories"), 
    ("Car vacuum cleaner", "Automotive parts and accessories"), ("Air freshener", "Automotive parts and accessories"), 
    ("Jumper cables", "Automotive parts and accessories"), ("Lawn mower", "Outdoor and gardening supplies"), 
    ("Garden hose", "Outdoor and gardening supplies"), ("Plant pots", "Outdoor and gardening supplies"), 
    ("Gardening gloves", "Outdoor and gardening supplies"), ("Outdoor furniture set", "Outdoor and gardening supplies"), 
    ("BBQ grill", "Outdoor and gardening supplies"), ("Camping tent", "Outdoor and gardening supplies"), 
    ("Hiking backpack", "Outdoor and gardening supplies"), ("Watering can", "Outdoor and gardening supplies"), 
    ("Patio umbrella", "Outdoor and gardening supplies"), ("Dog food", "Pet supplies"), 
    ("Cat litter", "Pet supplies"), ("Pet bed", "Pet supplies"), ("Aquarium", "Pet supplies"), 
    ("Bird cage", "Pet supplies"), ("Hamster wheel", "Pet supplies"), ("Pet toys", "Pet supplies"), 
    ("Dog leash", "Pet supplies"), ("Cat scratching post", "Pet supplies"), 
    ("Fish tank filter", "Pet supplies"), ("Multivitamins", "Health and wellness products"), 
    ("First aid kit", "Health and wellness products"), ("Blood pressure monitor", "Health and wellness products"), 
    ("Thermometer", "Health and wellness products"), ("Yoga block", "Health and wellness products"), 
    ("Essential oils", "Health and wellness products"), ("Protein powder", "Health and wellness products"), 
    ("Massage chair", "Health and wellness products"), ("Sleep mask", "Health and wellness products"), 
    ("Heating pad", "Health and wellness products"), ("Gold necklace", "Jewelry and watches"), 
    ("Silver bracelet", "Jewelry and watches"), ("Diamond ring", "Jewelry and watches"), 
    ("Pearl earrings", "Jewelry and watches"), ("Wristwatch", "Jewelry and watches"), 
    ("Charm bracelet", "Jewelry and watches"), ("Cufflinks", "Jewelry and watches"), 
    ("Brooch", "Jewelry and watches"), ("Anklet", "Jewelry and watches"), ("Pocket watch", "Jewelry and watches"), 
    ("Acrylic paint set", "Art and craft supplies"), ("Sketch pencils", "Art and craft supplies"), 
    ("Canvas", "Art and craft supplies"), ("Knitting needles", "Art and craft supplies"), 
    ("Embroidery kit", "Art and craft supplies"), ("Craft scissors", "Art and craft supplies"), 
    ("Glue gun", "Art and craft supplies"), ("Bead set", "Art and craft supplies"), 
    ("Calligraphy pens", "Art and craft supplies"), ("Watercolor palette", "Art and craft supplies"), 
    ("Organic tea", "Food and beverages"), ("Coffee beans", "Food and beverages"), 
    ("Olive oil", "Food and beverages"), ("Chocolate", "Food and beverages"), 
    ("Wine", "Food and beverages"), ("Cheese", "Food and beverages"), ("Cereal", "Food and beverages"), 
    ("Pasta", "Food and beverages"), ("Honey", "Food and beverages"), ("Spices set", "Food and beverages"), 
    ("Luggage", "Travel accessories"), ("Travel pillow", "Travel accessories"), 
    ("Passport holder", "Travel accessories"), ("Packing cubes", "Travel accessories"), 
    ("Travel adapter", "Travel accessories"), ("Backpack", "Travel accessories"), 
    ("Travel toiletries kit", "Travel accessories"), ("Sunglasses case", "Travel accessories"), 
    ("Portable charger", "Travel accessories"), ("Travel blanket", "Travel accessories"), 
    ("Acoustic guitar", "Musical instruments"), ("Electric keyboard", "Musical instruments"), 
    ("Drum set", "Musical instruments"), ("Violin", "Musical instruments"), ("Flute", "Musical instruments"), 
    ("Saxophone", "Musical instruments"), ("Ukulele", "Musical instruments"), 
    ("Trumpet", "Musical instruments"), ("Harmonica", "Musical instruments"), 
    ("Microphone", "Musical instruments"), ("Desk organizer", "Office supplies"), 
    ("Stapler", "Office supplies"), ("Printer paper", "Office supplies"), 
    ("File cabinet", "Office supplies"), ("Whiteboard", "Office supplies"), 
    ("Desk lamp", "Office supplies"), ("Shredder", "Office supplies"), ("Mouse pad", "Office supplies"), 
    ("Office chair mat", "Office supplies"), ("Letter opener", "Office supplies"), 
    ("Baby stroller", "Baby products"), ("Baby monitor", "Baby products"), ("Crib", "Baby products"), 
    ("Diapers", "Baby products"), ("Baby bottle", "Baby products"), ("High chair", "Baby products"), 
    ("Baby swing", "Baby products"), ("Pacifier", "Baby products"), ("Baby bath tub", "Baby products"), 
    ("Baby carrier", "Baby products"), ("Balloons", "Party supplies"), ("Party hats", "Party supplies"), 
    ("Streamers", "Party Supplies") ]

def get_user_emails():
    """
    Fetch emails from the users table in the database.
    """
    emails = []
    try:
        conn = psycopg2.connect(
            dbname="company_db",
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            host="company_db",
            port="5432"
        )
        cursor = conn.cursor()
        cursor.execute("SELECT email FROM users")
        emails = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
    except Exception as e:
        logger.error(f"Error fetching user emails: {e}")
    return emails

user_emails = get_user_emails()

def generate_google_search_data(campaign, ad):
    """
    Generate fake Google Search marketing data.
    """
    google_search_data = {
        "date": (datetime.now() - timedelta(days=random.randint(1, 30))).isoformat(),
        "campaign": campaign,
        "ad": ad,
        "impressions": random.randint(1000, 50000),
        "clicks": random.randint(50, 1000),
        "cost": round(random.uniform(100, 500), 2),
        "conversions": random.randint(1, 50)
    }
    return google_search_data

def generate_email_marketing_data(campaign, ad):
    """
    Generate fake Email Marketing data.
    """
    email_marketing_data = {
        "date": (datetime.now() - timedelta(days=random.randint(1, 7))).isoformat(),
        "user_email": random.choice(user_emails),
        "campaign": campaign,
        "ad": ad,
        "received": random.choice([0, 1]),
        "opened": random.choice([0, 1]),
        "subscribed": random.choice([0, 1]),
        "clicks": random.randint(0, 100),
        "bounces": random.randint(0, 10),
        "unsubscribed": random.choice([0, 1])
    }
    return email_marketing_data

def generate_facebook_data(campaign, ad):
    """
    Generate fake Facebook/Instagram marketing data.
    """
    facebook_data = {
        "platform": random.choice(["Facebook", "Instagram"]),
        "date": (datetime.now() - timedelta(days=random.randint(1, 30))).isoformat(),
        "campaign": campaign,
        "ad_unit": ad,
        "impressions": random.randint(1000, 50000),
        "percent_watched": round(random.uniform(0, 100), 2),
        "clicks": random.randint(50, 1000),
        "cost": round(random.uniform(100, 500), 2),
        "conversions": random.randint(1, 50),
        "likes": random.randint(0, 500),
        "shares": random.randint(0, 100),
        "comments": random.randint(0, 100)
    }
    return facebook_data

def main():
    """
    Generate fake data and write to JSON files.
    """
    try:
        google_search_data = []
        email_marketing_data = []
        facebook_data = []

        for product, category in products_with_categories:
            for i in range(5):  # 5 campaigns per category
                campaign = f"{product} - {category} - Campaign {i + 1}"
                for j in range(10):  # 10 ads per campaign
                    ad = f"{product} Ad {j + 1}"

                    google_search_data.append(generate_google_search_data(campaign, ad))
                    email_marketing_data.append(generate_email_marketing_data(campaign, ad))
                    facebook_data.append(generate_facebook_data(campaign, ad))

        logger.info(f"Generated {len(google_search_data)} Google Search marketing data records.")
        logger.info(f"Generated {len(email_marketing_data)} Email Marketing data records.")
        logger.info(f"Generated {len(facebook_data)} Facebook data records.")
        
        logger.info("Writing generated data to JSON files...") 

        with open("/opt/airflow/generator/fake_google_search_data.json", "w") as f:
            json.dump(google_search_data, f, indent=4)
        with open("/opt/airflow/generator/fake_email_marketing_data.json", "w") as f:
            json.dump(email_marketing_data, f, indent=4)
        with open("/opt/airflow/generator/fake_facebook_data.json", "w") as f:
            json.dump(facebook_data, f, indent=4)

        logger.info("Data generation complete.")
            
    except Exception as e:
        logger.error(f"Error during data generation: {e}")

if __name__ == "__main__":
    main()