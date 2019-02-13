import asyncio

from tqdm import tqdm
from aiohttp import ClientSession, TCPConnector
import numpy as np

from pymbtiles import MBtiles, Tile

CONCURRENCY = 30
BATCH_SIZE = 100  # number of tiles per batch
TILE_URL = "https://s3.amazonaws.com/elevation-tiles-prod/terrarium/{z}/{x}/{y}.png"
EMPTY_TILE = 757  # if content-length is this value, tile is empty and not useful

loop = asyncio.get_event_loop()


def download(mbtiles, url, min_zoom, max_zoom, skip_existing=True, concurrency=10):
    async def fetch_tile(session, url, z, x, y):
        tile_url = url.format(z=z, x=x, y=y)

        async with session.head(tile_url) as head:
            length = int(head.headers.get("Content-Length"))

            if length != EMPTY_TILE:
                async with session.get(tile_url) as r:
                    flipped_y = (1 << z) - 1 - y
                    return Tile(z, x, flipped_y, await r.read())

            else:
                print("empty tile: {} {} {}".format(z, x, y))
                return None

    async def fetch_tiles(tiles):
        async with ClientSession(
            connector=TCPConnector(limit=concurrency, verify_ssl=False)
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

        xy = np.array(
            np.meshgrid(np.arange(0, 2 ** zoom), np.arange(0, 2 ** zoom))
        ).T.reshape(-1, 2)

        tiles = []

        # filter tiles based on ones we already have
        for x, y in xy:
            if skip_existing and mbtiles.has_tile(zoom, x, y):
                # print("has {} {} {}".format(zoom, x, y))
                continue
            else:
                # print("get {} {} {}".format(zoom, x, y))
                tiles.append((zoom, x, y))

        if tiles:
            print("zoom {} has {} tiles to fetch".format(zoom, len(tiles)))
            for i in range(0, len(tiles), BATCH_SIZE):
                loop.run_until_complete(fetch_tiles(tiles[i : i + BATCH_SIZE]))

        else:
            print("no tiles to fetch")


min_zoom = 0
max_zoom = 4

with MBtiles("../data/elevation.mbtiles", "r+") as mbtiles:  # FIXME: w => r+
    mbtiles.meta = {
        "name": "elevation",
        "description": "Mapzen Terrarium Elevation Tiles",
        "version": "1.0",
        "attribution": "Mapzen",
        "credits": "Mapzen",
        "type": "overlay",
        "format": "png",
        "bounds": ",".join(str(x) for x in [-180, -85, 180, 85]),
        "center": ",".join(str(x) for x in [0, 0, 0]),
        "minzoom": str(min_zoom),
        "maxzoom": str(max_zoom),
    }

    download(mbtiles, TILE_URL, min_zoom, max_zoom, concurrency=CONCURRENCY)
