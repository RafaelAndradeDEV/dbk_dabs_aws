"""Generate coherent fake marketing data and land it as parquet in S3.

Creates four related datasets that feed the RFM / CLV case:

    customers           -> one row per customer, with an acquisition_channel
    orders              -> orders per customer (dates after signup)
    order_items         -> 1..N lines per order
    marketing_campaigns -> spend / delivery per acquisition channel

Files are written to:  s3://<bucket>/<env>/marketing/<table>/LOAD00000001.parquet
(the same layout the bronze models read with read_files / spark.read.parquet).

Output is deterministic: the same --seed and --as-of produce identical files in every
environment. CI runs this as the first task of each implementation (see the
`seed_sample_data` tasks), so every target seeds its own landing zone.

Usage (Databricks job/notebook, writes to S3 via the `aws_scope_s3` secret scope):

    python generate_marketing_data.py --env dev --bucket s3://project-dbk-coredev-bucket

Usage (local smoke test, writes parquet to a local folder instead of S3):

    python generate_marketing_data.py --env dev --local-path ./_sample_data
"""

import argparse
import io
import os
import random
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
from faker import Faker

# Acquisition channels must match between customers and campaigns (CAC join key).
ACQUISITION_CHANNELS = ["Paid Search", "Paid Social", "Email", "Organic", "Referral"]
ORDER_CHANNELS = ["web", "app", "store"]
ORDER_STATUSES = ["completed", "completed", "completed", "cancelled", "returned"]
PRODUCT_CATEGORIES = ["Apparel", "Home", "Beauty", "Electronics", "Outdoor", "Grocery"]

TABLES = ("customers", "orders", "order_items", "marketing_campaigns")

# Spark cannot read TIMESTAMP(NANOS) parquet (newer pandas/pyarrow default); force microseconds.
PARQUET_OPTS = {"coerce_timestamps": "us", "allow_truncated_timestamps": True}


# Anchor date for every generated timestamp; set from --as-of in main().
AS_OF = datetime(2026, 6, 1, tzinfo=timezone.utc)  # noqa: UP017


def _now_utc():
    return AS_OF


def generate_customers(fake, num_customers, rng):
    """Build the customer master with a signup date and acquisition channel."""
    rows = []
    for cid in range(1, num_customers + 1):
        signup = _now_utc() - timedelta(days=int(rng.integers(30, 730)))
        rows.append(
            {
                "CustomerId": cid,
                "FullName": fake.name(),
                "Email": fake.email(),
                "Country": fake.country(),
                "SignupDate": signup.date(),
                # Skew the channel mix so segments/ROI differ across channels.
                "AcquisitionChannel": rng.choice(ACQUISITION_CHANNELS, p=[0.30, 0.25, 0.20, 0.15, 0.10]),
                "ModifiedDate": _now_utc(),
                "__extracted_at__": _now_utc(),
            }
        )
    return pd.DataFrame(rows)


def generate_orders_and_items(customers_df, rng):
    """Build orders (Poisson count per customer) and 1..N items per order."""
    orders, items = [], []
    order_id = 0
    item_id = 0
    for _, c in customers_df.iterrows():
        signup = pd.Timestamp(c["SignupDate"], tz="UTC")
        # Frequency varies per customer -> spreads R/F/M and segments.
        num_orders = int(rng.poisson(3))
        for _ in range(num_orders):
            order_id += 1
            max_offset = max((_now_utc() - signup.to_pydatetime()).days, 1)
            order_ts = signup.to_pydatetime() + timedelta(days=int(rng.integers(0, max_offset)))
            orders.append(
                {
                    "OrderId": order_id,
                    "CustomerId": int(c["CustomerId"]),
                    "OrderTimestamp": order_ts,
                    "Channel": rng.choice(ORDER_CHANNELS),
                    "Status": rng.choice(ORDER_STATUSES),
                    "ModifiedDate": _now_utc(),
                    "__extracted_at__": _now_utc(),
                }
            )
            for _ in range(int(rng.integers(1, 6))):
                item_id += 1
                items.append(
                    {
                        "OrderItemId": item_id,
                        "OrderId": order_id,
                        "ProductId": int(rng.integers(1, 500)),
                        "Category": rng.choice(PRODUCT_CATEGORIES),
                        "Quantity": int(rng.integers(1, 5)),
                        "UnitPrice": round(float(rng.uniform(10, 400)), 2),
                        "Discount": round(float(rng.choice([0, 0, 0.1, 0.15, 0.25])), 2),
                        "ModifiedDate": _now_utc(),
                        "__extracted_at__": _now_utc(),
                    }
                )
    return pd.DataFrame(orders), pd.DataFrame(items)


def generate_campaigns(rng):
    """Build campaign spend / delivery per acquisition channel."""
    rows = []
    campaign_id = 0
    for channel in ACQUISITION_CHANNELS:
        # 2 campaigns per channel so spend aggregates non-trivially.
        for _ in range(2):
            campaign_id += 1
            impressions = int(rng.integers(50_000, 500_000))
            clicks = int(impressions * rng.uniform(0.01, 0.06))
            start = _now_utc() - timedelta(days=int(rng.integers(120, 700)))
            rows.append(
                {
                    "CampaignId": campaign_id,
                    "Channel": channel,
                    "CampaignName": f"{channel} - {start.strftime('%Y-%m')}",
                    "StartDate": start.date(),
                    "EndDate": (start + timedelta(days=30)).date(),
                    "Cost": round(float(rng.uniform(5_000, 50_000)), 2),
                    "Impressions": impressions,
                    "Clicks": clicks,
                    "ModifiedDate": _now_utc(),
                    "__extracted_at__": _now_utc(),
                }
            )
    return pd.DataFrame(rows)


def write_local(frames, env, local_path):
    """Write parquet files to a local folder (offline smoke test)."""
    for table, df in frames.items():
        out_dir = os.path.join(local_path, env, "marketing", table)
        os.makedirs(out_dir, exist_ok=True)
        df.to_parquet(os.path.join(out_dir, "LOAD00000001.parquet"), index=False, engine="pyarrow", **PARQUET_OPTS)
        print(f"  wrote {len(df):>6} rows -> {out_dir}/LOAD00000001.parquet")


def write_s3(frames, env, bucket):
    """Write parquet files to S3 using boto3 + the `aws_scope_s3` secret scope."""
    import boto3  # pylint: disable=incompatible-with-uc
    from databricks.sdk.runtime import dbutils

    access_key = dbutils.secrets.get("aws_scope_s3", "ACCESS_KEY_ID")
    secret_key = dbutils.secrets.get("aws_scope_s3", "SECRET_ACCESS_KEY")
    # pylint: disable-next=incompatible-with-uc
    s3 = boto3.client("s3", aws_access_key_id=access_key, aws_secret_access_key=secret_key)

    for table, df in frames.items():
        key = f"{env}/marketing/{table}/LOAD00000001.parquet"
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False, engine="pyarrow", **PARQUET_OPTS)
        buffer.seek(0)
        s3.upload_fileobj(buffer, bucket, key)
        buffer.close()
        print(f"  wrote {len(df):>6} rows -> s3://{bucket}/{key}")


def main():
    """Generate the four datasets and land them locally or in S3."""
    parser = argparse.ArgumentParser(description="Generate fake marketing data for the RFM/CLV case.")
    parser.add_argument("--env", default="dev", help="Environment prefix (dev/qa/prod).")
    parser.add_argument("--bucket", default="project-dbk-coredev-bucket", help="Target S3 bucket (name or s3:// URI).")
    parser.add_argument("--num-customers", type=int, default=2000, help="Number of customers to generate.")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility.")
    parser.add_argument(
        "--as-of", default="2026-06-01", help="Anchor date (YYYY-MM-DD) for all generated dates; keeps output fixed."
    )
    parser.add_argument("--local-path", default=None, help="If set, write to this local folder instead of S3.")
    args = parser.parse_args()
    bucket = args.bucket.removeprefix("s3://").strip("/")

    global AS_OF
    AS_OF = datetime.strptime(args.as_of, "%Y-%m-%d").replace(tzinfo=timezone.utc)  # noqa: UP017

    rng = np.random.default_rng(args.seed)
    random.seed(args.seed)
    fake = Faker()
    Faker.seed(args.seed)

    print(f"Generating {args.num_customers} customers (seed={args.seed}, as_of={args.as_of})...")
    customers = generate_customers(fake, args.num_customers, rng)
    orders, items = generate_orders_and_items(customers, rng)
    campaigns = generate_campaigns(rng)

    frames = {
        "customers": customers,
        "orders": orders,
        "order_items": items,
        "marketing_campaigns": campaigns,
    }

    if args.local_path:
        print(f"Writing parquet locally under {args.local_path}/{args.env}/marketing/ ...")
        write_local(frames, args.env, args.local_path)
    else:
        print(f"Writing parquet to s3://{bucket}/{args.env}/marketing/ ...")
        write_s3(frames, args.env, bucket)

    print("Done.")


if __name__ == "__main__":
    main()
