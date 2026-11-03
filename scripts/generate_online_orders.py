"""
Creates the ONLINE STORE's data in MongoDB (database lusitania_online,
collection "orders"), the NoSQL source of the project. A tool to SIMULATE
a source system, not part of the pipeline.

Each order is one JSON document with its items nested inside, using the
real customers and products from LusitaniaOLTP (Project 1):

    {
      "orderNumber": "WEB0001234",
      "orderDate":   "2025-06-10T14:32:00Z",
      "status":      "delivered",
      "currency":    "EUR",
      "customer":    {"code": "C00010", "email": "...", "country": "PT"},
      "shipping":    {"city": "Porto", "country": "PT", "cost": 4.99},
      "payment":     {"method": "card"},
      "items":       [{"sku": "SKU-00010", "name": "...", "qty": 1, "unitPrice": 517.5}, ...]
    }

On purpose, the data contains problems that the SILVER layer must fix:
  - ~1.5% duplicate orders (same orderNumber stored twice, later status)
  - ~8% guest checkouts with no "customer" object, only "guestEmail"
  - three different date formats in "orderDate"
  - some SKUs in lowercase ("sku-00010")
  - ~3% orders without a "payment" field
  - prices in GBP and USD for orders shipped abroad

Run:   python scripts/generate_online_orders.py
Safe to rerun: it empties the collection first. Same seed = same data.
"""
import random
from datetime import datetime, timedelta, timezone

import pyodbc
from pymongo import MongoClient

from lusitania import config

SEED = 42
N_ORDERS = 8000
START = datetime(2025, 1, 1, tzinfo=timezone.utc)
END = datetime(2026, 10, 31, tzinfo=timezone.utc)

# Approximate rates used only to price foreign orders at generation time.
# Silver converts back to EUR with the REAL daily rates from the API.
APPROX_RATE = {"EUR": 1.0, "GBP": 0.85, "USD": 1.08}
COUNTRY_CURRENCY = {"PT": "EUR", "ES": "EUR", "FR": "EUR", "DE": "EUR", "GB": "GBP", "US": "USD"}
FOREIGN_CITIES = {
    "ES": ["Madrid", "Vigo", "Sevilla"], "FR": ["Paris", "Lyon"], "DE": ["Berlin", "Munich"],
    "GB": ["London", "Manchester"], "US": ["New York", "Boston"],
}
STATUSES = ["paid", "shipped", "delivered", "delivered", "delivered", "cancelled", "returned"]


def load_reference_data():
    """Customers and products from the SQL Server source, so both sources share keys."""
    with pyodbc.connect(config.sql_connection_string()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT CustomerCode, Email, City FROM sales.Customer")
        customers = [tuple(r) for r in cur.fetchall()]
        cur.execute("SELECT SKU, ProductName, ListPrice FROM product.Product WHERE IsActive = 1")
        products = [(r.SKU, r.ProductName, float(r.ListPrice)) for r in cur.fetchall()]
    return customers, products


def format_date(rng: random.Random, dt: datetime):
    """Three formats on purpose: silver must parse all of them into one timestamp."""
    roll = rng.random()
    if roll < 0.80:
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")          # ISO 8601 text (most common)
    if roll < 0.95:
        return dt.strftime("%d/%m/%Y %H:%M")              # Portuguese style text
    return dt                                             # a real BSON date


def make_order(rng: random.Random, n: int, customers, products):
    order_dt = START + timedelta(seconds=rng.randint(0, int((END - START).total_seconds())))

    country = rng.choices(list(COUNTRY_CURRENCY), weights=[80, 6, 4, 3, 5, 2])[0]
    currency = COUNTRY_CURRENCY[country]
    rate = APPROX_RATE[currency]

    items = []
    for line_no, (sku, name, price) in enumerate(rng.sample(products, rng.randint(1, 4)), start=1):
        items.append({
            "line": line_no,
            "sku": sku.lower() if rng.random() < 0.05 else sku,
            "name": name,
            "qty": rng.choices([1, 2, 3], weights=[75, 20, 5])[0],
            "unitPrice": round(price * rate, 2),
        })

    doc = {
        "orderNumber": f"WEB{n:07d}",
        "orderDate": format_date(rng, order_dt),
        "status": rng.choice(STATUSES),
        "currency": currency,
        "shipping": {
            "city": rng.choice(FOREIGN_CITIES[country]) if country != "PT" else None,  # PT: filled below
            "country": country,
            "cost": 0.0 if rng.random() < 0.4 else round(4.99 * rate, 2),
        },
        "items": items,
        "createdAt": order_dt,
    }

    if rng.random() < 0.08:                                # guest checkout: no customer object
        doc["guestEmail"] = f"guest{n}@example.net"
        if country == "PT":
            doc["shipping"]["city"] = rng.choice(["Lisboa", "Porto", "Braga", "Faro"])
    else:
        code, email, city = rng.choice(customers)
        doc["customer"] = {"code": code, "email": email, "country": country}
        if country == "PT":
            doc["shipping"]["city"] = city

    if rng.random() > 0.03:                                # ~3% have no payment info
        doc["payment"] = {"method": rng.choice(["card", "card", "mbway", "paypal"])}

    return doc


def main():
    rng = random.Random(SEED)
    customers, products = load_reference_data()
    print(f"Reference data: {len(customers)} customers, {len(products)} products from {config.OLTP_DATABASE}")

    orders = [make_order(rng, n, customers, products) for n in range(1, N_ORDERS + 1)]

    # Duplicates: the same order stored again later with a newer status
    duplicates = []
    for original in rng.sample(orders, int(N_ORDERS * 0.015)):
        dup = dict(original)
        dup["status"] = "delivered" if original["status"] != "returned" else "returned"
        dup["createdAt"] = original["createdAt"] + timedelta(days=rng.randint(1, 5))
        duplicates.append(dup)

    client = MongoClient(config.MONGO_URI)
    collection = client[config.MONGO_DATABASE]["orders"]
    collection.delete_many({})
    collection.insert_many(orders + duplicates)

    total = collection.count_documents({})
    print(f"Inserted {total} documents into {config.MONGO_DATABASE}.orders "
          f"({len(orders)} orders + {len(duplicates)} duplicates)")
    print("Guest checkouts:", collection.count_documents({"customer": {"$exists": False}}))
    print("Without payment:", collection.count_documents({"payment": {"$exists": False}}))
    print("By currency:", {c: collection.count_documents({"currency": c}) for c in APPROX_RATE})
    client.close()


if __name__ == "__main__":
    main()
