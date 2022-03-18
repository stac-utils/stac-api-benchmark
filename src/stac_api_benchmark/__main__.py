"""Command-line interface."""
import concurrent.futures
from functools import reduce
from time import perf_counter
from typing import Tuple

import click
import requests
from pystac import Item
from pystac_client import Client
from pystac_client.exceptions import APIError
from src.stac_api_benchmark import query


@click.command()
@click.version_option()
@click.option("--url", help="The root / Landing Page url for a STAC API")
@click.option("--collection", help="The collection to operate on")
def main(url: str, collection: str) -> None:
    """STAC API Benchmark."""
    result = _search_with_fc(url, collection, query.STEP)
    print(f"STEP: {result[0]} {result[1]:02f}")

    result = _search_with_fc(url, collection, query.TNC_ECOREGIONS)
    print(f"TNC: {result[0]} {result[1]:02f}")

    result = _request_item_repeatedly(url, collection, times=1000, workers=50)
    print(f"Repeated: {result:02f}")

    result = _request_point_with_no_results(url, collection, times=1000, workers=50)
    print(f"No results: {result:02f}")


def _get_link_by_rel(item: Item, rel: str) -> str:
    return next(filter(lambda x: x.rel == rel, item.links)).href


def _request_item_repeatedly(url: str, collection: str, times: int, workers: int):
    catalog = Client.open(url)
    item = next(catalog.search(collections=[collection], max_items=1).get_items())
    item_url = _get_link_by_rel(item, "self")

    def get_item_by_url(url: str, timeout: int = 10) -> None:
        _ = requests.get(url=url, timeout=timeout).text

    t_start = perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = []
        for _ in range(0, times):
            futures.append(executor.submit(get_item_by_url, url=item_url))
        for future in concurrent.futures.as_completed(futures):
            _ = future.result()

    return perf_counter() - t_start


def _request_point_with_no_results(url: str, collection: str, times: int, workers: int):
    def search_with_query_that_has_no_results() -> None:
        _ = requests.get(
            url=f"{url}/search",
            params={"collections": collection, "bbox": "-179,85,-178,89"},
            timeout=10,
        ).text

    t_start = perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = []
        for _ in range(0, times):
            futures.append(executor.submit(search_with_query_that_has_no_results))
        for future in concurrent.futures.as_completed(futures):
            _ = future.result()

    return perf_counter() - t_start


def _search_with_fc(url: str, collection: str, fc_filename: str) -> Tuple[int, float]:
    catalog = Client.open(url)
    intersectses = query.load_geometries(fc_filename)
    times = []
    for intersects in intersectses:
        try:
            t_start = perf_counter()
            resp = catalog.search(
                collections=[collection], intersects=intersects, limit=1000
            )
            count = len(list(resp.get_items()))
            times.append((count, perf_counter() - t_start))
        except APIError as e:
            print(f"APIError: {e}")

    return reduce(lambda a, b: (a[0] + b[0], a[1] + b[1]), times)


if __name__ == "__main__":
    main(prog_name="stac-api-benchmark")  # pragma: no cover
