import argparse
import json
import random
import threading
import time
from datetime import datetime
from uuid import uuid4

import psycopg2
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from faker import Faker
from faker.providers import BaseProvider

fake = Faker()


# Custom Product Provider for realistic product names
class ProductProvider(BaseProvider):
    def product_name(self):
        categories = [
            'Laptop',
            'Smartphone',
            'Tablet',
            'Monitor',
            'Headphones',
            'Smartwatch',
            'Camera',
            'Printer',
            'Speaker',
            'Television',
            'Game Console',
            'Router',
            'Desktop',
            'Graphics Card',
            'Drone',
            'VR Headset',
            'Smart Home Hub',
            'Fitness Tracker',
            'E-Reader',
            'Microwave',
            'Refrigerator',
            'Dishwasher',
            'Washing Machine',
            'Air Purifier',
            'Projector',
            'Soundbar',
            'Wearable',
            'Smart Glasses',
            'Portable SSD',
            'Gaming Mouse',
            'Gaming Chair',
            'Home Security Camera',
        ]

        brands = [
            'ASUS',
            'Nokia',
            'Samsung',
            'Apple',
            'Dell',
            'HP',
            'Lenovo',
            'Sony',
            'Microsoft',
            'Google',
            'Xiaomi',
            'Canon',
            'LG',
            'Bose',
            'Panasonic',
            'Huawei',
            'Acer',
            'OnePlus',
            'Motorola',
            'Razer',
            'Philips',
            'Nintendo',
            'Alienware',
            'Corsair',
            'GoPro',
            'DJI',
            'Ring',
            'Fitbit',
            'Garmin',
            'Polaroid',
            'Epson',
            'Brother',
            'Sennheiser',
            'JBL',
            'Vizio',
            'Logitech',
            'Zyxel',
            'Netgear',
            'BenQ',
            'Oculus',
            'HyperX',
            'SteelSeries',
            'Kingston',
            'Seagate',
            'Western Digital',
            'TP-Link',
            'Dyson',
            'TCL',
            'Sharp',
            'Hisense',
            'Vizio',
        ]

        model_names = [
            'Pro',
            'Max',
            'Plus',
            'Ultra',
            'Series 5',
            'X200',
            'G7',
            'Alpha',
            'Elite',
            'Z1',
            'Edge',
            'Prime',
            'M9',
            'X',
            '2024 Edition',
            'S',
            'Air',
            'P500',
            'T200',
            'Note',
            'G5',
            'SE',
            'Advanced',
            'Mini',
            'Lite',
            'Extreme',
            'Prime',
            'Nano',
            'Neo',
            'Studio',
            'A10',
            'Titan',
            'Quantum',
            'Vision',
            'Performance',
            'Ranger',
            'Zoom',
            'Mavic',
            'Optic',
            'Precision',
            'Cloud',
            'Storm',
            'Turbo',
            'Inspire',
            'Studio',
            'Vision',
            'VR2',
        ]

        adjectives = [
            'High Performance',
            'Professional',
            'Budget',
            '4K',
            '8K',
            'Portable',
            'Compact',
            'Durable',
            'Gaming',
            'Wireless',
            'Bluetooth',
            'Noise-Cancelling',
            'Curved',
            'Ultra-Thin',
            'Water-Resistant',
            'Touchscreen',
            'Dual-Band',
            'Ergonomic',
            'Energy-Efficient',
            'Smart',
            'HD',
            'Full HD',
            'LED',
            'OLED',
            'Smart',
            'Fast-Charging',
            'Lightweight',
            'Foldable',
            'Modular',
            'Expandable',
            'Dual SIM',
            'Solar-Powered',
            'Eco-Friendly',
        ]

        category = self.random_element(categories)
        brand = self.random_element(brands)
        model_name = (
            self.random_element(model_names) if self.random_int(0, 1) else ""
        )
        adjective = (
            self.random_element(adjectives) if self.random_int(0, 1) else ""
        )

        product_name = f"{category} {brand}"
        if model_name:
            product_name += f" {model_name}"
        if adjective:
            product_name = f"{adjective} {product_name}"

        return product_name


# Initialize Faker and add ProductProvider
fake = Faker()
fake.add_provider(ProductProvider)


# Create Kafka topics with 5 partitions
def create_topic(topic_name, num_partitions, replication_factor):
    conf = {'bootstrap.servers': 'kafka:9092'}
    admin_client = AdminClient(conf)
    new_topic = NewTopic(
        topic=topic_name,
        num_partitions=num_partitions,
        replication_factor=replication_factor,
    )
    fs = admin_client.create_topics([new_topic])
    for topic, f in fs.items():
        try:
            f.result()
            print(f"Topic '{topic}' created with {num_partitions} partitions.")
        except Exception as e:
            print(f"Failed to create topic '{topic}': {e}")


# Generate user and product data
def gen_user_and_product_data(
    num_user_records: int, num_product_records: int
) -> None:
    conn = psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="postgres",
        host="postgres",
    )
    curr = conn.cursor()
    for id in range(num_user_records):
        curr.execute(
            """INSERT INTO commerce.users (id, username, password)
            VALUES (%s, %s, %s)""",
            (id, fake.user_name(), fake.password()),
        )
    for id in range(num_product_records):
        curr.execute(
            """INSERT INTO commerce.products (id, name, description, price)
            VALUES (%s, %s, %s, %s)""",
            (
                id,
                fake.product_name(),
                fake.text(),
                fake.random_int(min=1, max=1000),
            ),
        )
    conn.commit()
    curr.close()
    conn.close()


# Generate a random user agent string
def random_user_agent():
    return fake.user_agent()


# Generate a random IP address
def random_ip():
    return fake.ipv4()


# Generate a click event
def generate_click_event(user_id, product):
    click_id = str(uuid4())
    product_id, product_name, price = product
    url = fake.uri()
    user_agent = random_user_agent()
    ip_address = random_ip()
    datetime_occured = datetime.now()
    click_event = {
        "click_id": click_id,
        "user_id": user_id,
        "product_id": product_id,
        "product": product_name,
        "price": price,
        "url": url,
        "user_agent": user_agent,
        "ip_address": ip_address,
        "datetime_occured": datetime_occured.strftime("%Y-%m-%d %H:%M:%S.%f")[
            :-3
        ],
    }
    return click_event


# Generate a checkout event
def generate_checkout_event(user_id, product):
    product_id, product_name, price = product
    payment_method = fake.credit_card_provider()
    total_amount = price * random.uniform(1, 3)
    shipping_address = fake.address()
    billing_address = fake.address()
    user_agent = random_user_agent()
    ip_address = random_ip()
    datetime_occured = datetime.now()
    checkout_event = {
        "checkout_id": str(uuid4()),
        "user_id": user_id,
        "product_id": product_id,
        "payment_method": payment_method,
        "total_amount": total_amount,
        "shipping_address": shipping_address,
        "billing_address": billing_address,
        "user_agent": user_agent,
        "ip_address": ip_address,
        "datetime_occured": datetime_occured.strftime("%Y-%m-%d %H:%M:%S.%f")[
            :-3
        ],
    }
    return checkout_event


# Push events to Kafka
def push_to_kafka(event, topic):
    producer = Producer({'bootstrap.servers': 'kafka:9092'})
    producer.produce(topic, json.dumps(event).encode('utf-8'))
    producer.flush()


# Save click events to PostgreSQL
def save_click_to_db(conn, click_event):
    curr = conn.cursor()
    curr.execute(
        """INSERT INTO commerce.clicks
           (click_id, user_id, product_id, product,
           price, url, user_agent, ip_address, datetime_occured)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        (
            click_event["click_id"],
            click_event["user_id"],
            click_event["product_id"],
            click_event["product"],
            click_event["price"],
            click_event["url"],
            click_event["user_agent"],
            click_event["ip_address"],
            click_event["datetime_occured"],
        ),
    )
    conn.commit()


# Save checkout events to PostgreSQL
def save_checkout_to_db(conn, checkout_event):
    curr = conn.cursor()
    curr.execute(
        """INSERT INTO commerce.checkouts
           (checkout_id, user_id, product_id,
           payment_method, total_amount,
           shipping_address, billing_address,
           user_agent, ip_address, datetime_occured)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        (
            checkout_event["checkout_id"],
            checkout_event["user_id"],
            checkout_event["product_id"],
            checkout_event["payment_method"],
            checkout_event["total_amount"],
            checkout_event["shipping_address"],
            checkout_event["billing_address"],
            checkout_event["user_agent"],
            checkout_event["ip_address"],
            checkout_event["datetime_occured"],
        ),
    )
    conn.commit()


# Generate click events for a user
def generate_click_events_for_user(
    conn, user_id, products, num_clicks_before_checkout
):
    for _ in range(num_clicks_before_checkout):
        product = random.choice(products)
        click_event = generate_click_event(user_id, product)
        push_to_kafka(click_event, 'clicks')
        save_click_to_db(conn, click_event)
        time.sleep(random.uniform(0.01, 0.05))
    if random.random() < 0.5:
        product = random.choice(products)
        checkout_event = generate_checkout_event(user_id, product)
        push_to_kafka(checkout_event, 'checkouts')
        save_checkout_to_db(conn, checkout_event)
        time.sleep(random.uniform(0.01, 0.1))


# Generate clickstream data using threading
def gen_clickstream_data(
    num_click_records: int, num_threads: int = 100
) -> None:
    conn = psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="postgres",
        host="postgres",
    )
    user_ids = list(range(0, 100))
    curr = conn.cursor()
    curr.execute("SELECT id, name, price FROM commerce.products")
    products = curr.fetchall()
    curr.close()

    def worker():
        for _ in range(num_click_records // num_threads):
            user_id = random.choice(user_ids)
            num_clicks_before_checkout = random.randint(1, 10)
            generate_click_events_for_user(
                conn, user_id, products, num_clicks_before_checkout
            )

    threads = []
    for _ in range(num_threads):
        t = threading.Thread(target=worker)
        threads.append(t)
        t.start()
    for t in threads:
        t.join()

    conn.close()


if __name__ == "__main__":
    numpartition = 5
    replication_factor = 1
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-nu",
        "--num_user_records",
        type=int,
        help="Number of user records to generate",
        default=100,
    )
    parser.add_argument(
        "-np",
        "--num_product_records",
        type=int,
        help="Number of product records to generate",
        default=1000,
    )
    parser.add_argument(
        "-nc",
        "--num_click_records",
        type=int,
        help="Number of click records to generate",
        default=100000,
    )
    parser.add_argument(
        "-nt",
        "--num_threads",
        type=int,
        help="Number of threads to use for data generation",
        default=100,
    )
    args = parser.parse_args()

    # Create Kafka topics with 5 partitions
    create_topic('clicks', numpartition, replication_factor)
    create_topic('checkouts', numpartition, replication_factor)

    # Generate users and products
    gen_user_and_product_data(args.num_user_records, args.num_product_records)

    # Generate clickstream data
    gen_clickstream_data(args.num_click_records, args.num_threads)
