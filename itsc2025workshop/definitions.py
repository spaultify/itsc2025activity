import dagster as dg

from itsc2025workshop import assets  # noqa: TID252

all_assets = dg.load_assets_from_modules([assets])

defs = dg.Definitions(
    assets=all_assets,
)
