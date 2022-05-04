"""Command-line interface."""
import asyncio

import click

from . import query


@click.command()
@click.version_option()
@click.option("--url", help="The root / Landing Page url for a STAC API")
@click.option("--collection", help="The collection to operate on")
@click.option("--concurrency", default=10, help="The collection to operate on")
def main(url: str, collection: str, concurrency: int) -> None:
    """STAC API Benchmark."""
    asyncio.get_event_loop().run_until_complete(run(url, collection, concurrency))


async def run(url: str, collection: str, concurrency: int) -> None:
    result = await query.search_with_fc(
        url, collection, query.STEP, concurrency, id_field="siteid"
    )
    print(f"STEP: {result[1]:.2f}")

    result = await query.search_with_fc(
        url, collection, query.TNC_ECOREGIONS, concurrency, id_field="ECO_ID_U"
    )
    print(f"STEP: {result[1]:.2f}")

    result = await query.request_item_repeatedly(
        url, collection, times=10000, concurrency=50
    )
    print(f"Repeated: {result:02f}")

    result = await query.request_point_with_no_results(
        url, collection, times=1000, concurrency=50
    )
    print(f"No results: {result:02f}")

    result = await query.sorting(
        url,
        collection,
        concurrency=50,
        sortby=[query.sortby("properties.eo:cloud_cover", "desc")],  # noqa
    )
    print(f"sort -properties.eo:cloud_cover : {result[1]:02f}")

    result = await query.sorting(
        url,
        collection,
        concurrency=50,
        sortby=[query.sortby("properties.eo:cloud_cover", "asc")],  # noqa
    )
    print(f"sort +properties.eo:cloud_cover : {result[1]:02f}")

    result = await query.sorting(
        url,
        collection,
        concurrency=50,
        sortby=[query.sortby("properties.datetime", "desc")],  # noqa
    )
    print(f"sort -properties.datetime : {result[1]:02f}")

    result = await query.sorting(
        url,
        collection,
        concurrency=50,
        sortby=[query.sortby("properties.datetime", "asc")],  # noqa
    )
    print(f"sort +properties.datetime : {result[1]:02f}")

    result = await query.sorting(
        url,
        collection,
        concurrency=50,
        sortby=[query.sortby("properties.created", "desc")],  # noqa
    )
    print(f"sort -properties.created : {result[1]:02f}")

    result = await query.sorting(
        url,
        collection,
        concurrency=50,
        sortby=[query.sortby("properties.created", "asc")],  # noqa
    )
    print(f"sort +properties.created : {result[1]:02f}")


if __name__ == "__main__":
    main(prog_name="stac-api-benchmark")  # pragma: no cover
