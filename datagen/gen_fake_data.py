import argparse
import json
import random
import threading
import time
from datetime import datetime
from uuid import uuid4

import psycopg2
from confluent_kafka import Producer
from faker import Faker

fake = Faker()


# Generate user and product data
def gen_user_data(num_user_records: int) -> None:
    conn = psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="postgres",
        host="postgres",
    )
    for id in range(num_user_records):
        curr = conn.cursor()
        curr.execute(
            """INSERT INTO commerce.users
             (id, username, password) VALUES (%s, %s, %s)""",
            (id, fake.user_name(), fake.password()),
        )
        curr.execute(
            """INSERT INTO commerce.products
             (id, name, description, price) VALUES (%s, %s, %s, %s)""",
            (id, fake.name(), fake.text(), fake.random_int(min=1, max=1000)),
        )
        conn.commit()

        # Update 10% of the time
        if random.randint(1, 100) >= 90:
            curr.execute(
                "UPDATE commerce.users SET username = %s WHERE id = %s",
                (fake.user_name(), id),
            )
            curr.execute(
                "UPDATE commerce.products SET name = %s WHERE id = %s",
                (fake.name(), id),
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
def generate_click_event(user_id, product_id=None):
    click_id = str(uuid4())
    product_id = product_id or str(uuid4())
    product = fake.word()
    price = fake.pyfloat(left_digits=2, right_digits=2, positive=True)
    url = fake.uri()
    user_agent = random_user_agent()
    ip_address = random_ip()
    datetime_occured = datetime.now()

    click_event = {
        "click_id": click_id,
        "user_id": user_id,
        "product_id": product_id,
        "product": product,
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
def generate_checkout_event(user_id, product_id):
    payment_method = fake.credit_card_provider()
    total_amount = fake.pyfloat(left_digits=3, right_digits=2, positive=True)
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
    conn, user_id, product_ids, num_clicks_before_checkout
):
    for _ in range(num_clicks_before_checkout):
        product_id = random.choice(product_ids)
        click_event = generate_click_event(user_id, product_id)
        push_to_kafka(click_event, 'clicks')
        save_click_to_db(conn, click_event)

        # Optional delay, reduce to increase throughput
        time.sleep(random.uniform(0.01, 0.05))

    # Simulate a checkout event after several clicks
    if random.random() < 0.5:  # 50% chance to checkout
        product_id = random.choice(product_ids)
        checkout_event = generate_checkout_event(user_id, product_id)
        push_to_kafka(checkout_event, 'checkouts')
        save_checkout_to_db(conn, checkout_event)

        # Optional delay, reduce to increase throughput
        time.sleep(random.uniform(0.01, 0.1))


# Main function to generate clickstream data using threading
def gen_clickstream_data(
    num_click_records: int, num_threads: int = 10
) -> None:
    conn = psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="postgres",
        host="postgres",
    )

    user_ids = list(range(0, 100))  # Assuming we have 100 users
    product_ids = [str(uuid4()) for _ in range(1000)]  # Assuming 1000 products

    def worker():
        for _ in range(num_click_records // num_threads):
            user_id = random.choice(user_ids)
            num_clicks_before_checkout = random.randint(1, 10)
            generate_click_events_for_user(
                conn, user_id, product_ids, num_clicks_before_checkout
            )

    threads = []
    for _ in range(num_threads):
        t = threading.Thread(target=worker)
        threads.append(t)
        t.start()

    # Wait for all threads to finish
    for t in threads:
        t.join()

    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-nu",
        "--num_user_records",
        type=int,
        help="Number of user records to generate",
        default=100,
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
        default=10,
    )
    args = parser.parse_args()
    gen_user_data(args.num_user_records)
    gen_clickstream_data(args.num_click_records, args.num_threads)
