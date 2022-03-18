"""Utilities for constructing search queries."""
import importlib.resources
import json
from typing import Any
from typing import Dict
from typing import List
from zipfile import ZipFile

STEP = "step_september152014_70rndsel_igbpcl.geojson"
TNC_ECOREGIONS = "tnc_terr_ecoregions.geojson.zip"


def load_geometries(filename: str) -> List[Dict[str, Any]]:
    """Load a list of GeoJSON Geometry objects from a file."""
    return _geometries_from(_load_geojson(filename))


def _load_geojson(filename: str) -> Dict[str, Any]:
    if filename.endswith(".zip"):
        with importlib.resources.path("geojson", filename) as f:
            with ZipFile(f) as zf:
                with zf.open(zf.infolist()[0]) as fo:
                    content = fo.read()
    else:
        content = importlib.resources.read_text("geojson", filename)
    return json.loads(content)


def _geometries_from(geojson: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [f["geometry"] for f in geojson["features"]]
