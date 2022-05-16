"""Command-line interface."""
import asyncio
import logging
from typing import Any
from typing import Optional

import click
import click_log

from . import query

logger = logging.getLogger(__name__)
click_log.basic_config(logger)


@click.command()
@click.version_option()
@click.option("--url", required=True, help="The root / Landing Page url for a STAC API")
@click.option("--collection", required=True, help="The collection to operate on")
@click.option("--concurrency", default=10, help="The collection to operate on")
@click.option("--seed", default=0, help="The seed value for random query generation")
@click.option(
    "--first-queryable",
    default="cloud_cover",
    help="Name of first queryable, ranged 0-100",
)
@click.option(
    "--second-queryable",
    default="cloud_shadow_percentage",
    help="Name of second queryable, ranged 0-100",
)
@click.option(
    "--third-queryable",
    default="nodata_pixel_percentage",
    help="Name of third queryable, ranged 0-100",
)
@click.option(
    "--features",
    default=None,
    type=int,
    help="Only query this number of features from the feature collection inputs",
)
@click.option(
    "--max-items",
    default=10000,
    help="Request this maximum number of items from the API for each query.",
)
@click_log.simple_verbosity_option(logger)
def main(
    url: str,
    collection: str,
    concurrency: int,
    seed: int,
    first_queryable: str,
    second_queryable: str,
    third_queryable: str,
    features: Optional[int],
    max_items: int,
) -> None:
    """STAC API Benchmark."""
    asyncio.run(
        run(
            url=url,
            collection=collection,
            concurrency=concurrency,
            seed=seed,
            first_queryable=first_queryable,
            second_queryable=second_queryable,
            third_queryable=third_queryable,
            num_features=features,
            max_items=max_items,
        )
    )


async def run(
    url: str,
    collection: str,
    concurrency: int,
    seed: int,
    first_queryable: str,
    second_queryable: str,
    third_queryable: str,
    num_features: Optional[int],
    max_items: int,
) -> None:
    logger.info("Running STEP")
    result: Any = await query.search_with_fc(
        url=url,
        collection=collection,
        fc_filename=query.STEP,
        concurrency=concurrency,
        id_field="siteid",
        logger=logger,
        num_features=num_features,
        max_items=max_items,
    )
    logger.info(f"STEP Results: total time: {result[1]:.2f}s")

    logger.info("Running TNC Ecoregions")
    result = await query.search_with_fc(
        url=url,
        collection=collection,
        fc_filename=query.TNC_ECOREGIONS,
        concurrency=concurrency,
        id_field="ECO_ID_U",
        logger=logger,
        num_features=num_features,
        max_items=max_items,
        # these IDs have self-intersections, so can't be
        # used to query some databases (e.g., ES)
        exclude_ids=[
            "10026",
            "10096",
            "10100",
            "10123",
            "10158",
            "10201",
            "10266",
            "10213",
            "10289",
            "10321",
            "10339",
            "10342",
            "10354",
            "10356",
            "17105",
            "10378",
            "10385",
            "10425",
            "10598",
            "10633",
            "10691",
            "10700",
            "17009",
            "10015",
            "10032",
            "10040",
            "10042",
            "10046",
            "10047",
            "10050",
            "10051",
            "10076",
        ],
    )
    logger.info(f"TNC Ecoregions: {result[1]:.2f}s")

    logger.info("Running country political boundaries, in April 2019")
    result = await query.search_with_fc(
        url=url,
        collection=collection,
        fc_filename=query.COUNTRIES,
        concurrency=concurrency,
        id_field="name",
        datetime="2019-04-01T00:00:00Z/2019-05-01T00:00:00Z",
        logger=logger,
        num_features=num_features,
        max_items=max_items,
    )
    logger.info(f"Countries: {result[1]:.2f}s")

    logger.info("Running country political boundaries, cloud cover ascending")
    result = await query.search_with_fc(
        url=url,
        collection=collection,
        fc_filename=query.COUNTRIES,
        concurrency=concurrency,
        id_field="name",
        sortby=[query.sortby("properties.eo:cloud_cover", "asc")],  # noqa
        logger=logger,
        num_features=num_features,
        max_items=max_items,
    )
    logger.info(f"Countries: {result[1]:.2f}s")

    logger.info(f"Running random queries (seeded with {seed})")
    result = await query.search_with_random_queries(
        url=url,
        collection=collection,
        concurrency=concurrency,
        seed=seed,
        first_queryable=first_queryable,
        second_queryable=second_queryable,
        third_queryable=third_queryable,
        logger=logger,
        num_features=num_features,
        max_items=max_items,
    )
    logger.info(f"Random Queries (seeded with {seed}): {result[1]:.2f}s")

    repeated_item_times = 10000
    repeated_item_concurrency = 50
    logger.info(
        f"Running repeated item, times={repeated_item_times} "
        f"concurrency={repeated_item_concurrency}"
    )
    result = await query.request_item_repeatedly(
        url=url,
        collection=collection,
        times=repeated_item_times,
        concurrency=repeated_item_concurrency,
    )
    logger.info(f"Repeated: {result:.2f}s")

    logger.info("Running sort -properties.eo:cloud_cover")
    result = await query.sorting(
        url=url,
        collection=collection,
        concurrency=50,
        sortby=[query.sortby("properties.eo:cloud_cover", "desc")],  # noqa
        logger=logger,
        max_items=max_items,
    )
    logger.info(f"sort -properties.eo:cloud_cover : {result[1]:02f}s")

    logger.info("Running sort +properties.eo:cloud_cover")
    result = await query.sorting(
        url,
        collection,
        concurrency=50,
        sortby=[query.sortby("properties.eo:cloud_cover", "asc")],  # noqa
        logger=logger,
        max_items=max_items,
    )
    logger.info(f"sort +properties.eo:cloud_cover : {result[1]:02f}s")

    logger.info("Running sort -properties.datetime")
    result = await query.sorting(
        url,
        collection,
        concurrency=50,
        sortby=[query.sortby("properties.datetime", "desc")],  # noqa
        logger=logger,
        max_items=max_items,
    )
    logger.info(f"sort -properties.datetime : {result[1]:02f}s")

    logger.info("Running sort +properties.datetime")
    result = await query.sorting(
        url,
        collection,
        concurrency=50,
        sortby=[query.sortby("properties.datetime", "asc")],  # noqa
        logger=logger,
        max_items=max_items,
    )
    logger.info(f"sort +properties.datetime : {result[1]:02f}s")

    logger.info("Running sort -properties.created")
    result = await query.sorting(
        url,
        collection,
        concurrency=50,
        sortby=[query.sortby("properties.created", "desc")],  # noqa
        logger=logger,
        max_items=max_items,
    )
    logger.info(f"sort -properties.created : {result[1]:02f}s")

    logger.info("Running sort +properties.created")
    result = await query.sorting(
        url,
        collection,
        concurrency=50,
        sortby=[query.sortby("properties.created", "asc")],  # noqa
        logger=logger,
        max_items=max_items,
    )
    logger.info(f"sort +properties.created : {result[1]:02f}s")


if __name__ == "__main__":
    main(prog_name="stac-api-benchmark")  # pragma: no cover
