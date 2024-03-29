"""Command-line interface."""
import asyncio
import json
import logging
from typing import Any
from typing import Optional

import click
import click_log
from returns.result import Failure
from returns.result import Success

from . import query

# these IDs have self-intersections, so can't be used to query some databases (e.g., ES)
TNC_EXCLUDED_IDS = [
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
]

logger = logging.getLogger(__name__)
click_log.basic_config(logger)


@click.command()
@click.version_option()
@click.option("--url", required=True, help="The root / Landing Page url for a STAC API")
@click.option(
    "--collection",
    "collections",
    required=True,
    multiple=True,
    help="The collections over which to query (comma-separated)",
)
@click.option(
    "--concurrency", default=10, help="The number of concurrent request to run"
)
@click.option("--seed", default=0, help="The seed value for random query generation")
@click.option(
    "--queryable",
    "queryables",
    multiple=True,
    type=str,
    help="Name of queryable, ranged 0-100",
)
@click.option(
    "--num-features",
    default=None,
    type=int,
    help="Only query this number of features from the feature collection inputs",
)
@click.option(
    "--num-random",
    default=10000,
    help="The number of random queries to run",
)
@click.option(
    "--max-items",
    default=10000,
    help="Request this maximum number of items from the API for each query",
)
@click.option(
    "--limit",
    default=500,
    help="Request this limit of items per page from the API for each query",
)
@click.option(
    "--timeout",
    default=30,
    help="Maximum duration before each search request is considered to have timed out,"
    " in seconds",
)
@click_log.simple_verbosity_option(logger)
def main(
    url: str,
    collections: tuple[str, ...],
    concurrency: int,
    seed: int,
    queryables: tuple[str, ...],
    num_features: Optional[int],
    num_random: int,
    max_items: int,
    limit: int,
    timeout: int,
) -> None:
    """STAC API Benchmark."""
    results = asyncio.run(
        run(
            query.BenchmarkConfig(
                url=url,
                collections=collections,
                concurrency=concurrency,
                seed=seed,
                queryables=queryables,
                num_features=num_features,
                num_random=num_random,
                max_items=max_items,
                limit=limit,
                logger=logger,
                timeout=timeout,
            )
        )
    )

    print(json.dumps(results))


async def run(config: query.BenchmarkConfig) -> dict[str, float]:
    results: dict[str, float] = {}
    logger.info("Running STEP")
    result: Any = await query.search_with_fc(
        config=config,
        fc_filename=query.STEP,
        id_field="siteid",
    )
    logger.info(f"STEP Results: total time: {result[1]:.2f}s")
    results["step"] = result[1]

    logger.info("Running TNC Ecoregions")
    result = await query.search_with_fc(
        config=config,
        fc_filename=query.TNC_ECOREGIONS,
        id_field="ECO_ID_U",
        exclude_ids=TNC_EXCLUDED_IDS,
    )
    logger.info(f"TNC Ecoregions: {result[1]:.2f}s")
    results["tnc"] = result[1]

    logger.info("Running country political boundaries, in April 2019")
    result = await query.search_with_fc(
        config=config,
        fc_filename=query.COUNTRIES,
        id_field="name",
        datetime="2019-04-01T00:00:00Z/2019-05-01T00:00:00Z",
    )
    logger.info(f"Countries, April 2019: {result[1]:.2f}s")
    results["countries_apr_2019"] = result[1]

    logger.info("Running country political boundaries, cloud cover ascending")
    result = await query.search_with_fc(
        config=config,
        fc_filename=query.COUNTRIES,
        id_field="name",
        sortby=[query.es_sortby("properties.eo:cloud_cover", "asc")],  # noqa
    )
    logger.info(f"Countries, cloud cover ascending: {result[1]:.2f}s")
    results["countries_cloud_cover_asc"] = result[1]

    logger.info(f"Running random queries (seeded with {config.seed})")
    result = await query.search_with_random_queries(
        config=config,
    )
    logger.info(f"Random Queries (seeded with {config.seed}): {result[1]:.2f}s")
    results["random_queries"] = result[1]

    repeated_item_times = 10000
    repeated_item_concurrency = 50
    logger.info(
        f"Running repeated item, times={repeated_item_times} "
        f"concurrency={repeated_item_concurrency}"
    )
    result = await query.request_item_repeatedly(
        config=config,
        times=repeated_item_times,
        concurrency=repeated_item_concurrency,
    )
    logger.info(f"Repeated: {result:.2f}s")
    results["repeated"] = result

    result = await run_sort(config, "properties.eo:cloud_cover", "desc")
    results["sort_cloud_cover_desc"] = result

    result = await run_sort(config, "properties.eo:cloud_cover", "asc")
    results["sort_cloud_cover_asc"] = result

    result = await run_sort(config, "properties.datetime", "desc")
    results["sort_datetime_desc"] = result

    result = await run_sort(config, "properties.datetime", "asc")
    results["sort_datetime_asc"] = result

    result = await run_sort(config, "properties.created", "desc")
    results["sort_created_desc"] = result

    result = await run_sort(config, "properties.created", "asc")
    results["sort_created_asc"] = result

    return results


async def run_sort(
    config: query.BenchmarkConfig, field: str, direction: str
) -> list[dict[str, float]]:
    logger.info(f"Running sort {field} {direction}")
    results = []
    for collection in config.collections:
        result = await query.sorting(
            config=config,
            collection=collection,
            sortby=[query.es_sortby(field, direction)],  # noqa
        )
        match result:
            case Success(value):
                logger.info(
                    f"Results: sort {field} {direction} on {collection} "
                    f": {value.duration:.2f}s"
                )
                results.append(
                    {
                        "sort": f"{collection}_{field}_{direction}",
                        "duration": value.duration,
                    }
                )
            case Failure(value):
                logger.error(
                    f"Results: sort {field} {direction} on {collection} "
                    f": Error: {value.msg}"
                )
    return results


if __name__ == "__main__":
    main(prog_name="stac-api-benchmark")  # pragma: no cover
