"""Module for generating random GeoJSON shapes."""
import math
from random import Random
from typing import Optional
from typing import Tuple

from geojson import Polygon


# derived from
# https://github.com/jazzband/geojson/blob/master/geojson/utils.py (BSD-3-Clause)
def generate_random_polygon(
    num_vertices: int = 5,
    bbox: Tuple[float, float, float, float] = (-180.0, -90.0, 180.0, 90.0),
    seed: Optional[int] = None,
    ave_radius: float = 5.0,
    center_lon: float = 0.0,
    center_lat: float = 0.0,
) -> Polygon:
    r = Random(seed)

    lon_min = bbox[0]
    lon_max = bbox[2]

    lat_min = bbox[1]
    lat_max = bbox[3]

    def clip(x: float, _min: float, _max: float) -> float:
        if _min > _max:
            return x
        elif x < _min:
            return _min
        elif x > _max:
            return _max
        else:
            return x

    def create_polygon() -> Polygon:
        irregularity = clip(0.1, 0, 1) * 2 * math.pi / num_vertices
        spikeyness = clip(0.5, 0, 1) * ave_radius

        angle_steps = []
        lower = (2 * math.pi / num_vertices) - irregularity
        upper = (2 * math.pi / num_vertices) + irregularity
        _sum = 0.0
        for _ in range(num_vertices):
            tmp = r.uniform(lower, upper)
            angle_steps.append(tmp)
            _sum = _sum + tmp

        k = _sum / (2 * math.pi)
        for i in range(num_vertices):
            angle_steps[i] = angle_steps[i] / k

        points = []
        angle = r.uniform(0, 2 * math.pi)

        for i in range(num_vertices):
            r_i = clip(r.gauss(ave_radius, spikeyness), 0, 2 * ave_radius)
            x = center_lon + r_i * math.cos(angle)
            y = center_lat + r_i * math.sin(angle)
            x = (x + 180.0) * (abs(lon_min - lon_max) / 360.0) + lon_min
            y = (y + 90.0) * (abs(lat_min - lat_max) / 180.0) + lat_min
            x = clip(x, lon_min, lon_max)
            y = clip(y, lat_min, lat_max)
            points.append((x, y))
            angle = angle + angle_steps[i]

        first_val = points[0]
        points.append(first_val)
        return Polygon([points])

    return create_polygon()
