import os
import glob
import asyncio
import pickle
from time import time

from tqdm import tqdm
from aiohttp import ClientSession, TCPConnector, ClientTimeout
import numpy as np
from mercantile import tiles as get_tiles

from pymbtiles import MBtiles, Tile

CONCURRENCY = 30
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


def download(mbtiles, url, min_zoom, max_zoom, bounds=None, concurrency=10):
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

        # read in info about previously read tiles
        prev_tiles = set()
        progress_filename = "progress-{}.pickle".format(zoom)
        if os.path.exists(progress_filename):
            with open(progress_filename, "rb") as fp:
                try:
                    prev_tiles = pickle.load(fp)
                except:
                    prev_tiles = set()
            print("previously read {} tiles".format(len(prev_tiles)))

        if bounds is None:
            xy = np.array(
                np.meshgrid(np.arange(0, 2 ** zoom), np.arange(0, 2 ** zoom))
            ).T.reshape(-1, 2)

            tiles = [(zoom, int(x), int(y)) for x, y in xy]

        else:
            bounded_tiles = get_tiles(*bounds, zooms=zoom)
            tiles = [(z, x, y) for x, y, z in bounded_tiles]

        # filter tiles based on ones we already have in the database
        tiles = [
            (z, x, y) for z, x, y in tiles if not mbtiles.has_tile(z, x, flip_y(y, z))
        ]

        if prev_tiles:
            tiles = [tile for tile in tiles if not tile in prev_tiles]

        if tiles:
            print("zoom {} has {} tiles to fetch".format(zoom, len(tiles)))
            for i in range(0, len(tiles), BATCH_SIZE):
                tiles_to_fetch = tiles[i : i + BATCH_SIZE]
                if tiles_to_fetch:
                    loop.run_until_complete(fetch_tiles(tiles_to_fetch))

                    with open(progress_filename, "wb") as fp:
                        prev_tiles = prev_tiles | set(tiles_to_fetch)
                        pickle.dump(prev_tiles, fp)

        else:
            print("no tiles to fetch")


min_zoom = 0
max_zoom = 0
mode = "r+"

# Approx bounds of South America
bounds = [-95.273438, -57.326521, -32.695313, 13.239945]

start = time()

# clear out any progress files
if mode == "w":
    for filename in glob.glob("progress-*.pickle"):
        os.remove(filename)


with MBtiles("../data/elevation2.mbtiles", mode) as mbtiles:  # FIXME: w => r+
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


print("Done downloading all tiles in {:,.2f} seconds".format(time() - start))

for filename in glob.glob("progress-*.pickle"):
    os.remove(filename)
