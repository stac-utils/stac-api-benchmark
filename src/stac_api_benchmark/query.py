"""Utilities for constructing search queries."""
import asyncio
import importlib.resources
import json
from asyncio import Semaphore
from time import perf_counter
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union
from zipfile import ZipFile

import aiohttp
from pystac import Item
from pystac_client import Client
from pystac_client.exceptions import APIError

STEP = "step_september152014_70rndsel_igbpcl.geojson"
TNC_ECOREGIONS = "tnc_terr_ecoregions.geojson.zip"


def load_geometries(filename: str, id_field: str) -> Dict[str, Dict[str, Any]]:
    """Load a list of GeoJSON Geometry objects from a file."""
    return geometries_from(load_geojson(filename), id_field)


def load_geojson(filename: str) -> Any:
    if filename.endswith(".zip"):
        with importlib.resources.path("geojson", filename) as f:
            with ZipFile(f) as zf:
                with zf.open(zf.infolist()[0]) as fo:
                    return json.loads(fo.read())
    else:
        return json.loads(importlib.resources.read_text("geojson", filename))


def geometries_from(
    geojson: Dict[str, Any], id_field: str
) -> Dict[str, Dict[str, Any]]:
    return {f["properties"][id_field]: f["geometry"] for f in geojson["features"]}


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


async def request_point_with_no_results(
    url: str, collection: str, times: int, concurrency: int
) -> float:
    sem = Semaphore(concurrency)

    cos = [
        search_with_query_that_has_no_results(url, collection, sem)
        for _ in range(0, times)
    ]
    t_start = perf_counter()
    pending = [asyncio.get_running_loop().create_task(co) for co in cos]
    await asyncio.gather(*pending, return_exceptions=True)

    return perf_counter() - t_start


async def search(
    url: str,
    collection: str,
    intersects: Optional[Dict[str, Any]],
    search_id: str,
    sem: Semaphore,
    max_items: Optional[int] = None,
    sortby: Optional[List[Dict[str, str]]] = None,
) -> Tuple[int, float]:
    async with sem:
        try:
            t_start = perf_counter()
            count = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: len(
                    list(
                        Client.open(url)
                        .search(
                            collections=[collection],
                            intersects=intersects,
                            limit=1000,
                            max_items=max_items,
                            sortby=sortby,
                        )
                        .get_items()
                    )
                ),
            )
            time = perf_counter() - t_start
            print(f"{search_id}: {count} items in\t{time:.3f}s")
            return count, time
        except APIError as e:
            print(f"APIError: {e}")
            raise e
        except Exception as e:
            print(f"Exception: {e}")
            raise e


async def search_with_fc(
    url: str, collection: str, fc_filename: str, concurrency: int, id_field: str
) -> Tuple[List[Union[Tuple[int, float], Exception]], float]:
    intersectses = load_geometries(fc_filename, id_field)
    sem = Semaphore(concurrency)
    cos = [
        search(url, collection, intersects, search_id, sem)
        for (search_id, intersects) in intersectses.items()
    ]
    t_start = perf_counter()

    pending = [asyncio.get_running_loop().create_task(co) for co in cos]

    results = await asyncio.gather(*pending, return_exceptions=True)

    time = perf_counter() - t_start
    return results, time  # noqa


async def sorting(
    url: str, collection: str, concurrency: int, sortby: List[Dict[str, str]]
) -> Tuple[List[Union[Tuple[int, float], Exception]], float]:
    sem = Semaphore(concurrency)
    t_start = perf_counter()
    cos = [search(url, collection, None, "1", sem, 10000, sortby)]

    pending = [asyncio.get_running_loop().create_task(co) for co in cos]

    results = await asyncio.gather(*pending, return_exceptions=True)

    time = perf_counter() - t_start
    return results, time  # noqa


def sortby(field: str, direction: str) -> dict[str, str]:
    return {"field": field, "direction": direction}
