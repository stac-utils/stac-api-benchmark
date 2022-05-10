"""Command-line interface."""
import asyncio
from typing import Any

import click

from . import query


@click.command()
@click.version_option()
@click.option("--url", help="The root / Landing Page url for a STAC API")
@click.option("--collection", help="The collection to operate on")
@click.option("--concurrency", default=10, help="The collection to operate on")
def main(url: str, collection: str, concurrency: int) -> None:
    """STAC API Benchmark."""
    asyncio.run(run(url, collection, concurrency))


async def run(url: str, collection: str, concurrency: int) -> None:

    print("Running STEP")
    result: Any = await query.search_with_fc(
        url, collection, query.STEP, concurrency, id_field="siteid"
    )
    print(f"STEP Results: {result[1]:.2f}s")

    print("Running TNC Ecoregions")
    result = await query.search_with_fc(
        url, collection, query.TNC_ECOREGIONS, concurrency, id_field="ECO_ID_U"
    )
    print(f"TNC Ecoregions: {result[1]:.2f}s")

    print("Running country political boundaries, in April 2019")
    result = await query.search_with_fc(
        url,
        collection,
        query.COUNTRIES,
        concurrency,
        id_field="name",
        datetime="2019-04-01T00:00:00Z/2019-05-01T00:00:00Z",
    )
    print(f"Countries: {result[1]:.2f}s")

    print("Running country political boundaries, 1000 results, cloud cover ascending")
    result = await query.search_with_fc(
        url,
        collection,
        query.COUNTRIES,
        concurrency,
        id_field="name",
        max_items=1000,
        sortby=[query.sortby("properties.eo:cloud_cover", "asc")],  # noqa
    )
    print(f"Countries: {result[1]:.2f}s")

    print("Running repeated item")
    result = await query.request_item_repeatedly(
        url, collection, times=10000, concurrency=50
    )
    print(f"Repeated: {result:.2f}s")

    print("Running point with no results")
    result = await query.request_point_with_no_results(
        url, collection, times=1000, concurrency=50
    )
    print(f"No results: {result:.2f}s")

    print("Running sort -properties.eo:cloud_cover")
    result = await query.sorting(
        url,
        collection,
        concurrency=50,
        sortby=[query.sortby("properties.eo:cloud_cover", "desc")],  # noqa
    )
    print(f"sort -properties.eo:cloud_cover : {result[1]:02f}s")

    print("Running sort +properties.eo:cloud_cover")
    result = await query.sorting(
        url,
        collection,
        concurrency=50,
        sortby=[query.sortby("properties.eo:cloud_cover", "asc")],  # noqa
    )
    print(f"sort +properties.eo:cloud_cover : {result[1]:02f}s")

    print("Running sort -properties.datetime")
    result = await query.sorting(
        url,
        collection,
        concurrency=50,
        sortby=[query.sortby("properties.datetime", "desc")],  # noqa
    )
    print(f"sort -properties.datetime : {result[1]:02f}s")

    print("Running sort +properties.datetime")
    result = await query.sorting(
        url,
        collection,
        concurrency=50,
        sortby=[query.sortby("properties.datetime", "asc")],  # noqa
    )
    print(f"sort +properties.datetime : {result[1]:02f}s")

    print("Running sort -properties.created")
    result = await query.sorting(
        url,
        collection,
        concurrency=50,
        sortby=[query.sortby("properties.created", "desc")],  # noqa
    )
    print(f"sort -properties.created : {result[1]:02f}s")

    print("Running sort +properties.created")
    result = await query.sorting(
        url,
        collection,
        concurrency=50,
        sortby=[query.sortby("properties.created", "asc")],  # noqa
    )
    print(f"sort +properties.created : {result[1]:02f}s")


if __name__ == "__main__":
    main(prog_name="stac-api-benchmark")  # pragma: no cover
