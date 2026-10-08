"""Prepare the Olist CSVs for replay and build the customers Delta dimension.

Responsibilities:

* validate that the raw Olist CSVs are present under ``DATA_DIR``,
* build the static customers *parquet* dimension consumed by the enrichment
  job (written to ``CUSTOMERS_PATH``),
* optionally (``--sample``) generate a small synthetic dataset so the whole
  pipeline can be exercised without downloading the 100 MB Kaggle dump.
"""

from __future__ import annotations

import argparse
import os
import random
import sys
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import structlog

log = structlog.get_logger("seed_data")

REQUIRED_FILES = [
    "olist_orders_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_customers_dataset.csv",
    "olist_order_payments_dataset.csv",
]

_STATES = ["SP", "RJ", "MG", "ES", "RS", "PR", "SC", "BA", "PE", "CE", "GO", "DF"]
_CATEGORIES = [
    "beleza_saude",
    "informatica_acessorios",
    "cama_mesa_banho",
    "moveis_decoracao",
    "esporte_lazer",
    "brinquedos",
    "relogios_presentes",
    "telefonia",
    "automotivo",
    "eletronicos",
]
_PAYMENT_TYPES = ["credit_card", "boleto", "voucher", "debit_card"]
# Share of sample orders delivered after the estimated date.
_LATE_RATE = 0.1

KAGGLE_HINT = (
    "Raw Olist CSVs not found. Download them with:\n"
    "    kaggle datasets download -d olistbr/brazilian-ecommerce -p data/raw --unzip\n"
    "or run this script with --sample to generate a synthetic dataset."
)


def generate_sample(data_dir: Path, n_customers: int, n_orders: int) -> None:
    """Generate a small synthetic Olist-shaped dataset.

    Args:
        data_dir: Directory to write the CSVs into.
        n_customers: Number of customers to generate.
        n_orders: Number of orders to generate.
    """
    data_dir.mkdir(parents=True, exist_ok=True)
    base_ts = datetime(2024, 1, 1)

    customers = [
        {
            "customer_id": f"cust_{i:06d}",
            "customer_unique_id": str(uuid.uuid4()),
            "customer_zip_code_prefix": random.randint(1000, 99999),
            "customer_city": f"city_{random.randint(1, 200)}",
            "customer_state": random.choice(_STATES),
        }
        for i in range(n_customers)
    ]
    pd.DataFrame(customers).to_csv(
        data_dir / "olist_customers_dataset.csv", index=False
    )

    products = [
        {
            "product_id": f"prod_{i:05d}",
            "product_category_name": random.choice(_CATEGORIES),
        }
        for i in range(200)
    ]
    pd.DataFrame(products).to_csv(data_dir / "olist_products_dataset.csv", index=False)

    orders, items, payments = [], [], []
    for i in range(n_orders):
        oid = f"order_{i:06d}"
        cust = random.choice(customers)
        purchase = base_ts + timedelta(minutes=random.randint(0, 525600))
        # Like Olist: the estimate is a date (midnight) 10-30 days out, and
        # about 1 in 10 orders arrives after the estimated day.
        estimated_days = random.randint(10, 30)
        estimated = (purchase + timedelta(days=estimated_days)).replace(
            hour=0, minute=0, second=0
        )
        if random.random() < _LATE_RATE:
            delivered = estimated + timedelta(
                days=random.randint(1, 10), hours=random.randint(0, 23)
            )
        else:
            delivered = purchase + timedelta(
                days=random.randint(2, estimated_days - 1),
                hours=random.randint(0, 23),
            )
        orders.append(
            {
                "order_id": oid,
                "customer_id": cust["customer_id"],
                "order_status": "delivered",
                "order_purchase_timestamp": purchase.strftime("%Y-%m-%d %H:%M:%S"),
                "order_approved_at": (
                    purchase + timedelta(minutes=random.randint(2, 30))
                ).strftime("%Y-%m-%d %H:%M:%S"),
                "order_delivered_customer_date": delivered.strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
                "order_estimated_delivery_date": estimated.strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
            }
        )
        price = round(random.uniform(10, 600), 2)
        items.append(
            {
                "order_id": oid,
                "order_item_id": 1,
                "product_id": random.choice(products)["product_id"],
                "seller_id": f"seller_{random.randint(1, 500):04d}",
                "shipping_limit_date": purchase.strftime("%Y-%m-%d %H:%M:%S"),
                "price": price,
                "freight_value": round(random.uniform(5, 40), 2),
            }
        )
        payments.append(
            {
                "order_id": oid,
                "payment_sequential": 1,
                "payment_type": random.choice(_PAYMENT_TYPES),
                "payment_installments": random.randint(1, 10),
                "payment_value": price + round(random.uniform(5, 40), 2),
            }
        )

    pd.DataFrame(orders).to_csv(data_dir / "olist_orders_dataset.csv", index=False)
    pd.DataFrame(items).to_csv(data_dir / "olist_order_items_dataset.csv", index=False)
    pd.DataFrame(payments).to_csv(
        data_dir / "olist_order_payments_dataset.csv", index=False
    )
    log.info(
        "sample.generated",
        customers=n_customers,
        orders=n_orders,
        products=len(products),
    )


def build_customers_dimension(data_dir: Path, customers_path: Path) -> int:
    """Write the static customers parquet dimension from the CSV.

    Args:
        data_dir: Directory containing ``olist_customers_dataset.csv``.
        customers_path: Output directory for the parquet dimension.

    Returns:
        The number of distinct customers written.
    """
    customers_path.mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(data_dir / "olist_customers_dataset.csv")
    df = df.drop_duplicates(subset=["customer_id"])
    out = customers_path / "customers.parquet"
    df.to_parquet(out, index=False, engine="pyarrow")
    log.info("customers.parquet.written", rows=len(df), path=str(out))
    return len(df)


def main() -> None:
    """CLI entry point for seeding/validating the dataset."""
    parser = argparse.ArgumentParser(description="Seed Olist data for replay.")
    parser.add_argument(
        "--sample", action="store_true", help="Generate a synthetic dataset."
    )
    parser.add_argument("--customers", type=int, default=2000)
    parser.add_argument("--orders", type=int, default=10000)
    args = parser.parse_args()

    data_dir = Path(os.environ.get("DATA_DIR", "data/raw"))
    customers_path = Path(os.environ.get("CUSTOMERS_PATH", "data/reference/customers"))

    if args.sample:
        generate_sample(data_dir, args.customers, args.orders)

    missing = [f for f in REQUIRED_FILES if not (data_dir / f).exists()]
    if missing:
        log.error("seed.missing_files", missing=missing)
        print(KAGGLE_HINT)
        sys.exit(1)

    count = build_customers_dimension(data_dir, customers_path)
    log.info("seed.done", customers=count, data_dir=str(data_dir))


if __name__ == "__main__":
    main()
