from typing import Any
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


@dg.asset(group_name="ingestion")
def superstore_raw() -> dg.MaterializeResult:

    dtypes = {
        "Row ID": int,
        "Order ID": str,
        "Shipping Placement Duration": int,
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
        "Quantity": float,
        "Discount": float,
        "Profit": float,
        "Loss/Profit?": str,
    }

    date_cols = ["Order Date", "Ship Date"]

    df = pd.read_csv(
        filepath_or_buffer=SUPERSTORE_FILEPATH, dtype=dtypes, parse_dates=date_cols
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

    dtypes = {
        "Row ID": int,
        "Order ID": str,
        "Shipping Placement Duration": int,
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
        "Quantity": float,
        "Discount": float,
        "Profit": float,
        "Loss/Profit?": str,
    }

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
    ]

    for pair in missing_cols:
        df.loc[df["Row ID"].isin(pair["row_ids"]), pair["col_name"]] = None

    # duplicate orders
    duplicate_orders = [
        1173,
        1218,
        1241,
        1255,
        1267,
        1348,
        1414,
        1451,
        1485,
        1631,
        1638,
        1657,
        1693,
        1704,
        1774,
        1788,
        1797,
        1932,
        1942,
        1999,
    ]

    # Ship Date is older than Order Date
    older_ship_dates = [3135, 3197, 3246, 3247, 3278, 3388, 3406, 3432, 3453, 3477]

    # Outlier Ship Date
    outlier_ship_dates = [3518, 3529, 3545, 3571, 3624, 3630, 3634, 3672, 3673, 3722]

    # Outlier Order Date
    outlier_order_dates = [3798, 3811, 3834, 3876, 3887, 3890, 3896, 3912, 3917, 3950]

    # malformed country/region
    malformed_country = [4041, 4119, 4133, 4135, 4209, 4238, 4299, 4327, 4369, 4453]

    # malformed segment
    malformed_segment = [4504, 4504, 4607, 4628, 4633, 4726, 4818, 4894, 4915, 4943]

    # Ship Mode string is different cases
    diff_cases_ship_mode = [
        5002,
        5007,
        5012,
        5019,
        5025,
        5067,
        5077,
        5077,
        5079,
        5102,
        5129,
        5130,
        5140,
        5145,
        5150,
        5151,
        5157,
        5159,
        5165,
        5190,
    ]

    # negative quantity
    negative_quantities = [5200, 5207, 5212, 5258, 5298, 5331, 5369, 5394, 5410, 5471]

    # sales is not a number
    nan_sales = [5512, 5530, 5756, 5767, 5787, 5807, 5845, 5875, 5928, 5986]

    # profit is not a number
    nan_profit = [6066, 6078, 6078, 6106, 6161, 6172, 6256, 6266, 6268, 6400]

    # quantity is outlier
    outlier_quantity = [6503, 6721, 6736, 6825, 7021, 7043, 7047, 7102, 7195, 7348]

    # sales is outlier
    outlier_sales = [7514, 7545, 7767, 7922, 8072, 8075, 8187, 8242, 8283, 8389]

    # discount is outlier
    outlier_discount = [8668, 8793, 8824, 8832, 8838, 9025, 9195, 9451, 9464, 9484]

    df.to_csv(SUPERSTORE_ACTIVITY_DATASET, index=False)

    return df
