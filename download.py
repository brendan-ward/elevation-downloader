import asyncio

from tqdm import tqdm
from aiohttp import ClientSession, TCPConnector, ClientTimeout
import numpy as np
from mercantile import tiles as get_tiles

from pymbtiles import MBtiles, Tile

CONCURRENCY = 10
BATCH_SIZE = 1000  # number of tiles per batch
TILE_URL = "https://s3.amazonaws.com/elevation-tiles-prod/terrarium/{z}/{x}/{y}.png"
EMPTY_TILE = 757  # if content-length is this value, tile is empty and not useful
WORLD_BOUNDS = [-180, -85, 180, 85]

loop = asyncio.get_event_loop()


def get_center(bounds, min_zoom=0):
    return [
        min_zoom,
        ((bounds[2] - bounds[0]) / 2) + bounds[0],
        ((bounds[3] - bounds[1]) / 2) + bounds[1],
    ]


def flip_y(y, z):
    return (1 << z) - 1 - y


def download(
    mbtiles, url, min_zoom, max_zoom, bounds=None, skip_existing=True, concurrency=10
):
    async def fetch_tile(session, url, z, x, y):
        tile_url = url.format(z=z, x=x, y=y)

        async with session.head(tile_url) as head:
            try:
                length = int(head.headers.get("Content-Length"))
            except:
                length = EMPTY_TILE

            if length != EMPTY_TILE:
                async with session.get(tile_url) as r:
                    return Tile(z, x, flip_y(y, z), await r.read())

            else:
                # print("empty tile: {} {} {}".format(z, x, y))
                return None

    async def fetch_tiles(tiles):
        async with ClientSession(
            timeout=ClientTimeout(total=60),
            connector=TCPConnector(limit=concurrency, verify_ssl=False),
        ) as session:
            futures = [
                asyncio.ensure_future(fetch_tile(session, url, *tile)) for tile in tiles
            ]
            for task in tqdm(asyncio.as_completed(futures), total=len(futures)):
                await task

            results = [f.result() for f in futures if f.result() is not None]
            mbtiles.write_tiles(results)

    for zoom in range(min_zoom, max_zoom + 1):
        print("zoom {}".format(zoom))

        tiles = []

        if bounds is None:
            xy = np.array(
                np.meshgrid(np.arange(0, 2 ** zoom), np.arange(0, 2 ** zoom))
            ).T.reshape(-1, 2)

            # filter tiles based on ones we already have
            tiles = [
                (zoom, x, y)
                for x, y in xy
                if not (skip_existing and mbtiles.has_tile(zoom, x, flip_y(y, zoom)))
            ]

        else:
            bounded_tiles = get_tiles(*bounds, zooms=[zoom, zoom])
            tiles = [
                (z, x, y)
                for x, y, z in bounded_tiles
                if not (skip_existing and mbtiles.has_tile(z, x, flip_y(y, z)))
            ]

        if tiles:
            print("zoom {} has {} tiles to fetch".format(zoom, len(tiles)))
            for i in range(0, len(tiles), BATCH_SIZE):
                loop.run_until_complete(fetch_tiles(tiles[i : i + BATCH_SIZE]))

        else:
            print("no tiles to fetch")


min_zoom = 0
max_zoom = 8

# Approx bounds of South America
bounds = [-95.273438, -57.326521, -32.695313, 13.239945]

with MBtiles("../data/elevation.mbtiles", "r+") as mbtiles:  # FIXME: w => r+
    mbtiles.meta = {
        "name": "elevation",
        "description": "Mapzen Terrarium Elevation Tiles",
        "version": "1.0",
        "attribution": "Mapzen",
        "credits": "Mapzen",
        "type": "overlay",
        "format": "png",
        "bounds": ",".join(str(x) for x in bounds or WORLD_BOUNDS),
        "center": ",".join(str(x) for x in get_center(bounds or WORLD_BOUNDS)),
        "minzoom": str(min_zoom),
        "maxzoom": str(max_zoom),
    }

    download(
        mbtiles, TILE_URL, min_zoom, max_zoom, bounds=bounds, concurrency=CONCURRENCY
    )
