from typing import Any
import random
import dagster as dg
import pandas as pd
import os

SUPERSTORE_FILEPATH = os.path.join(
    os.path.dirname(__file__), "datasets", "superstore.csv"
)

SUPERSTORE_PREP = os.path.join(
    os.path.dirname(__file__), "datasets", "superstore_prep.csv"
)

SUPERSTORE_ACTIVITY_DATASET = os.path.join(
    os.path.dirname(__file__), "datasets", "superstore_activity_dataset.csv"
)


def col_pair_generator(col_name: str, row_ids: list[Any]):
    return {"col_name": col_name, "row_ids": row_ids}


def scramble_case(text):
    return "".join(random.choice([char.upper(), char.lower()]) for char in text)


dtypes = {
    "Row ID": int,
    "Validity": str,
    "Order ID": str,
    "Shipping Placement Duration": object,
    "Ship Mode": str,
    "Customer ID": str,
    "Customer Name": str,
    "Segment": str,
    "Country/Region": str,
    "City": str,
    "State/Province": str,
    "Postal Code": str,
    "Region": str,
    "Product ID": str,
    "Category": str,
    "Sub-Category": str,
    "Product Name": str,
    "Sales": float,
    "Quantity": int,
    "Discount": float,
    "Profit": float,
    "Loss/Profit?": str,
}


@dg.asset(group_name="ingestion")
def superstore_raw() -> dg.MaterializeResult:

    date_cols = ["Order Date", "Ship Date"]

    df = pd.read_csv(
        filepath_or_buffer=SUPERSTORE_FILEPATH,
        dtype=dtypes,
        parse_dates=date_cols,
    )

    row_count = df.shape[0]

    df.to_csv(SUPERSTORE_PREP, index=False)

    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(row_count),
            "preview": dg.MetadataValue.md(df.head().to_markdown()),
        }
    )


@dg.asset(group_name="transform", deps=[superstore_raw])
def superstore_activity_dataset() -> pd.DataFrame:

    date_cols = ["Order Date", "Ship Date"]

    df = pd.read_csv(
        filepath_or_buffer=SUPERSTORE_PREP,
        dtype=dtypes,
        parse_dates=date_cols,
    )

    missing_order_ids = [88, 241, 417, 613, 713, 777, 811, 820, 858, 894]
    missing_customer_ids = [54, 93, 169, 340, 396, 565, 818, 908, 909, 974]
    missing_order_dates = [75, 83, 140, 273, 304, 382, 476, 613, 773, 990]
    missing_product_ids = [85, 232, 417, 546, 576, 785, 809, 844, 931, 957]

    missing_cols = [
        col_pair_generator("Order ID", missing_order_ids),
        col_pair_generator("Customer ID", missing_customer_ids),
        col_pair_generator("Order Date", missing_order_dates),
        col_pair_generator("Product ID", missing_product_ids),
    ]

    for pair in missing_cols:
        df.loc[df["Row ID"].isin(pair["row_ids"]), pair["col_name"]] = None

    # # duplicate orders
    # duplicate_order_ids = [1173, 1218, 1348, 1414, 1485, 1631, 1638, 1657, 1942, 1999]

    # Ship Date is older than Order Date
    older_ship_dates = [3135, 3197, 3246, 3247, 3278, 3388, 3406, 3432, 3453, 3477]
    for row_id in older_ship_dates:
        df.loc[df["Row ID"] == row_id, "Ship Date"] = df.loc[
            df["Row ID"] == row_id, "Ship Date"
        ].apply(lambda x: x.replace(year=2015))

    # Outlier Ship Date
    outlier_ship_dates = [3518, 3529, 3545, 3571, 3624, 3630, 3634, 3672, 3673, 3722]
    for row_id in outlier_ship_dates:
        df.loc[df["Row ID"] == row_id, "Ship Date"] = df.loc[
            df["Row ID"] == row_id, "Ship Date"
        ].apply(lambda x: x.replace(year=2050))

    # Outlier Order Date
    outlier_order_dates = [3798, 3811, 3834, 3876, 3887, 3890, 3896, 3912, 3917, 3950]
    for row_id in outlier_order_dates:
        df.loc[df["Row ID"] == row_id, "Order Date"] = df.loc[
            df["Row ID"] == row_id, "Order Date"
        ].apply(lambda x: x.replace(year=1998))

    # malformed country/region
    malformed_country = [4041, 4119, 4133, 4135, 4209, 4238, 4299, 4327, 4369, 4453]
    for row_id in malformed_country:
        df.loc[df["Row ID"] == row_id, "Country/Region"] = df.loc[
            df["Row ID"] == row_id, "Country/Region"
        ].apply(scramble_case)

    # malformed segment
    malformed_segment = [4504, 4504, 4607, 4628, 4633, 4726, 4818, 4894, 4915, 4943]
    for row_id in malformed_segment:
        df.loc[df["Row ID"] == row_id, "Segment"] = df.loc[
            df["Row ID"] == row_id, "Segment"
        ].apply(scramble_case)

    # Ship Mode string is different cases
    malformed_shipmode = [5002, 5007, 5077, 5079, 5102, 5129, 5130, 5151, 5165, 5190]
    for row_id in malformed_shipmode:
        df.loc[df["Row ID"] == row_id, "Ship Mode"] = df.loc[
            df["Row ID"] == row_id, "Ship Mode"
        ].apply(scramble_case)

    # negative quantity
    negative_quantities = [5200, 5207, 5212, 5258, 5298, 5331, 5369, 5394, 5410, 5471]
    for row_id in negative_quantities:
        df.loc[df["Row ID"] == row_id, "Quantity"] = df.loc[
            df["Row ID"] == row_id, "Quantity"
        ].apply(lambda x: x * -1)

    # sales is not a number
    nan_sales = [5512, 5530, 5756, 5767, 5787, 5807, 5845, 5875, 5928, 5986]
    for row_id in nan_sales:
        df.loc[df["Row ID"] == row_id, "Sales"] = random.choice(
            [None, "Paul", "Ian", "Aldrich", "2025-02-09"]
        )

    # profit is not a number
    nan_profit = [6066, 6078, 6078, 6106, 6161, 6172, 6256, 6266, 6268, 6400]
    for row_id in nan_profit:
        df.loc[df["Row ID"] == row_id, "Sales"] = random.choice(
            [None, "N/A", "Could be blank", "hahaha", "2025-02-09", "No profits???"]
        )

    # quantity is outlier
    outlier_quantity = [6503, 6721, 6736, 6825, 7021, 7043, 7047, 7102, 7195, 7348]
    for row_id in outlier_quantity:
        random_multiplier = random.choice([500, 1000, 1500])
        print(random_multiplier)
        df.loc[df["Row ID"] == row_id, "Quantity"] = df.loc[
            df["Row ID"] == row_id, "Quantity"
        ].apply(lambda x: x * random_multiplier)
        print(df.loc[df["Row ID"] == row_id, "Quantity"])

    # sales is outlier
    outlier_sales = [7514, 7545, 7767, 7922, 8072, 8075, 8187, 8242, 8283, 8389]
    for row_id in outlier_sales:
        random_multiplier = random.choice([10000, -10000, 50000, -50000])
        print(random_multiplier)
        df.loc[df["Row ID"] == row_id, "Sales"] = df.loc[
            df["Row ID"] == row_id, "Sales"
        ].apply(lambda x: x * random_multiplier)
        print(df.loc[df["Row ID"] == row_id, "Sales"])

    # discount is outlier
    outlier_discount = [8668, 8793, 8824, 8832, 8838, 9025, 9195, 9451, 9464, 9484]
    for row_id in outlier_discount:
        random_discount = random.choice([10, 15, -15, -10, 20, -20])
        print(random_multiplier)
        df.loc[df["Row ID"] == row_id, "Discount"] = df.loc[
            df["Row ID"] == row_id, "Discount"
        ].apply(lambda x: x + random_discount)
        print(df.loc[df["Row ID"] == row_id, "Discount"])

    df.to_csv(SUPERSTORE_ACTIVITY_DATASET, index=False)

    return df
