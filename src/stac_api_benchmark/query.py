"""Utilities for constructing search queries."""
import asyncio
import importlib.resources
import json
import traceback
from asyncio import Semaphore
from datetime import timezone as tz
from logging import Logger
from time import perf_counter
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union
from zipfile import ZipFile

import aiohttp
from faker import Faker
from pystac import Item
from pystac_client import Client
from pystac_client.exceptions import APIError

from .random_geojson import generate_random_polygon

STEP = "step_september152014_70rndsel_igbpcl.geojson"
TNC_ECOREGIONS = "tnc_terr_ecoregions.geojson.zip"
COUNTRIES = "countries.geojson"


def load_geometries(filename: str, id_field: str) -> Dict[str, Dict[str, Any]]:
    """Load a list of GeoJSON Geometry objects from a file."""
    return geometries_from(load_geojson(filename), id_field)


def load_geojson(filename: str) -> Any:
    if filename.endswith(".zip"):
        with importlib.resources.path("geojson_files", filename) as f:
            with ZipFile(f) as zf:
                with zf.open(zf.infolist()[0]) as fo:
                    return json.loads(fo.read())
    else:
        return json.loads(importlib.resources.read_text("geojson_files", filename))


def geometries_from(
    geojson: Dict[str, Any], id_field: str
) -> Dict[str, Dict[str, Any]]:
    return {str(f["properties"][id_field]): f["geometry"] for f in geojson["features"]}


def get_link_by_rel(item: Item, rel: str) -> str:
    return str(next(filter(lambda x: x.rel == rel, item.links)).href)


async def get_item_by_url(url: str, sem: Semaphore, timeout: int = 10) -> None:
    async with sem:
        async with aiohttp.ClientSession() as session:
            _ = await (await session.get(url, timeout=timeout)).text()


async def request_item_repeatedly(
    url: str, collection: str, times: int, concurrency: int
) -> float:
    catalog = Client.open(url)
    item = next(catalog.search(collections=[collection], max_items=1).get_items())
    item_url = get_link_by_rel(item, "self")

    sem = Semaphore(concurrency)

    cos = [get_item_by_url(item_url, sem) for _ in range(0, times)]
    t_start = perf_counter()

    pending = [asyncio.get_running_loop().create_task(co) for co in cos]
    await asyncio.gather(*pending, return_exceptions=True)
    return perf_counter() - t_start


async def search_with_query_that_has_no_results(
    url: str, collection: str, sem: Semaphore
) -> None:
    async with sem:
        async with aiohttp.ClientSession() as session:
            res = await session.get(
                url=f"{url}/search",
                params={"collections": collection, "bbox": "-179,85,-178,89"},
                timeout=10,
            )
            await res.text()


async def search(
    url: str,
    collection: str,
    intersects: Optional[Dict[str, Any]],
    search_id: str,
    sem: Semaphore,
    logger: Logger,
    max_items: Optional[int] = None,
    sortby: Optional[List[Dict[str, str]]] = None,
    datetime: Optional[str] = None,
    filter_lang: Optional[str] = None,
    cql2_filter: Optional[Dict[str, Any]] = None,
) -> Tuple[int, float]:
    async with sem:
        logger.debug(
            f"{search_id} => "
            f"collections = [{collection}], intersects = {intersects}, "
            f"limit = 1000, max_items = {max_items}, "
            f"sortby = {sortby}, datetime = {datetime}, "
            f"filter = {json.dumps(cql2_filter) if cql2_filter else ''}"
        )
        t_start = perf_counter()
        try:
            count = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: len(
                    Client.open(url).search(
                        collections=[collection],
                        intersects=intersects,
                        limit=1000,
                        max_items=max_items,
                        sortby=sortby,
                        datetime=datetime,
                        filter_lang=filter_lang,
                        filter=cql2_filter,
                    )  # get_all_items_as_dict returns dict instead
                    # of unmarshalled STAC objects
                    .get_all_items_as_dict()["features"]
                ),
            )
            time = perf_counter() - t_start
            logger.info(f"{search_id},{count},{time:.2f}")
            return count, time
        except APIError as e:
            logger.error(f"{search_id}: APIError: {e}")
            time = perf_counter() - t_start
            return -1, time
        except Exception as e:
            logger.error(f"{search_id}: Exception: {e}")
            logger.error(traceback.format_exc())
            time = perf_counter() - t_start
            return -1, time


async def search_with_random_queries(
    url: str,
    collection: str,
    concurrency: int,
    seed: int,
    first_queryable: str,
    second_queryable: str,
    third_queryable: str,
    logger: Logger,
    num_features: Optional[int],
    max_items: int,
) -> Tuple[List[Union[Tuple[int, float], Exception]], float]:
    sem = Semaphore(concurrency)
    Faker.seed(seed)
    fake = Faker()

    cos = []
    for i in range(num_features or 100):
        geometry = generate_random_polygon(
            num_vertices=fake.random_int(min=4, max=10),
            seed=seed,
            ave_radius=5.0,
            center_lon=fake.random_int(min=-180, max=180),
            center_lat=fake.random_int(min=-90, max=90),
        )
        interval_duration = fake.random_int(min=1, max=90)
        start_datetime = fake.date_time_between(start_date="-5y", tzinfo=tz.utc)
        end_datetime = fake.date_time_between(
            start_date=start_datetime, end_date=f"+{interval_duration}d", tzinfo=tz.utc
        )
        datetime_interval = f"{start_datetime.isoformat()}/{end_datetime.isoformat()}"

        cql2_filter = {
            "op": "and",
            "args": [
                {
                    "op": "<=",
                    "args": [
                        {"property": first_queryable},
                        fake.random_int(min=0, max=25),
                    ],
                },
                {
                    "op": "<=",
                    "args": [
                        {"property": second_queryable},
                        fake.random_int(min=0, max=25),
                    ],
                },
                {
                    "op": "<=",
                    "args": [
                        {"property": third_queryable},
                        fake.random_int(min=0, max=25),
                    ],
                },
            ],
        }

        cos.append(
            search(
                url=url,
                collection=collection,
                intersects=geometry,
                search_id=f"{i}",
                sem=sem,
                datetime=datetime_interval,
                # sortby=sortby,
                filter_lang="cql2-json",
                cql2_filter=cql2_filter,
                logger=logger,
                max_items=max_items,
            )
        )

    t_start = perf_counter()

    pending = [asyncio.get_running_loop().create_task(co) for co in cos]

    results = await asyncio.gather(*pending, return_exceptions=True)

    time = perf_counter() - t_start
    return results, time  # noqa


async def search_with_fc(
    url: str,
    collection: str,
    fc_filename: str,
    concurrency: int,
    id_field: str,
    logger: Logger,
    num_features: Optional[int],
    max_items: int,
    datetime: Optional[str] = None,
    sortby: Optional[List[Dict[str, str]]] = None,
    exclude_ids: Optional[List[str]] = None,
) -> Tuple[List[Union[Tuple[int, float], Exception]], float]:
    logger.info("id,item count,duration (sec)")

    intersectses = load_geometries(fc_filename, id_field)
    sem = Semaphore(concurrency)
    id_to_geometries = [
        (search_id, intersects) for (search_id, intersects) in intersectses.items()
    ]
    if num_features is not None:
        id_to_geometries = id_to_geometries[:num_features]
    cos = [
        search(
            url=url,
            collection=collection,
            intersects=intersects,
            search_id=search_id,
            sem=sem,
            max_items=max_items,
            datetime=datetime,
            sortby=sortby,
            logger=logger,
        )
        for (search_id, intersects) in id_to_geometries
        if exclude_ids is None or search_id not in exclude_ids
    ]

    t_start = perf_counter()

    pending = [asyncio.get_running_loop().create_task(co) for co in cos]
    results = await asyncio.gather(*pending, return_exceptions=True)

    time = perf_counter() - t_start

    return results, time  # noqa


async def sorting(
    url: str,
    collection: str,
    concurrency: int,
    sortby: List[Dict[str, str]],
    logger: Logger,
    max_items: int,
) -> Tuple[List[Union[Tuple[int, float], Exception]], float]:
    sem = Semaphore(concurrency)
    t_start = perf_counter()
    result = await search(
        url=url,
        collection=collection,
        intersects=None,
        search_id="1",
        sem=sem,
        sortby=sortby,
        logger=logger,
        max_items=max_items,
    )

    time = perf_counter() - t_start
    return [result], time  # noqa


def sortby(field: str, direction: str) -> dict[str, str]:
    return {"field": field, "direction": direction}
