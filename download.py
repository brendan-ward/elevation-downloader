import asyncio
from collections import namedtuple

from tqdm import tqdm
from aiohttp import ClientSession, TCPConnector


from pymbtiles import MBtiles


TILE_URL = "https://s3.amazonaws.com/elevation-tiles-prod/terrarium/{z}/{x}/{y}.png"
EMPTY_TILE = 757  # if content-length is this value, tile is empty and not useful

Tile = namedtuple("Tile", ["z", "x", "y"])


def flip_y(tile):
    """ Invert tile Y coordinate to match xyz tiling scheme """
    return Tile(tile.z, tile.x, (1 << tile.z) - 1 - tile.y)


@asyncio.coroutine
def wait_with_progress(futures):
    for f in tqdm(asyncio.as_completed(futures), total=len(futures)):
        yield from f


def download(mbtiles, url, min_zoom, max_zoom, skip_existing=True, concurrency=10):
    async def fetch_tile(session, url, tile):
        tile_url = url.format(z=tile.z, x=tile.x, y=tile.y)

        async with session.head(tile_url, ssl=False) as head:
            length = int(head.headers.get("Content-Length"))

            if length != EMPTY_TILE:

                async with session.get(tile_url, ssl=False) as r:
                    data = await r.read()
                    # return flip_y(tile), data
                    mbtiles.write_tile(*flip_y(tile), data=data)

            else:
                print("empty tile: {}".format(tile))

    async def fetch_tiles():
        async with ClientSession(connector=TCPConnector(limit=concurrency)) as session:
            futures = []

            for z in range(min_zoom, max_zoom + 1):
                for x in range(0, 2 ** z):
                    for y in range(0, 2 ** z):
                        if skip_existing and mbtiles.has_tile(z, x, y):
                            continue

                        else:
                            futures.append(
                                asyncio.ensure_future(
                                    fetch_tile(session, url, Tile(z, x, y))
                                )
                            )

            if len(futures):
                print("Getting ready to download {} tiles".format(len(futures)))

                for f in tqdm(asyncio.as_completed(futures), total=len(futures)):
                    await f

            else:
                print("No tiles to download")

    asyncio.get_event_loop().run_until_complete(fetch_tiles())


min_zoom = 0
max_zoom = 5

with MBtiles("../data/elevation.mbtiles", "r+") as mbtiles:
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

    download(mbtiles, TILE_URL, min_zoom, max_zoom, True)
