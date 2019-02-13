from collections import namedtuple
from requests import Session

from pymbtiles import MBtiles


TILE_URL = "https://s3.amazonaws.com/elevation-tiles-prod/terrarium/{z}/{x}/{y}.png"
EMPTY_TILE = 757  # if content-length is this value, tile is empty and not useful

Tile = namedtuple("Tile", ["z", "x", "y"])


outfilename = "../data/elevation.mbtiles"
mode = "r+"  # "w"
zoom = [0, 4]
skip_existing = True

tiles = []
for z in range(zoom[0], zoom[1] + 1):
    for i in range(0, 2 ** z):
        for j in range(0, 2 ** z):
            tiles.append(Tile(z, i, j))


# tiles = (Tile(0, 0, 0), Tile(12, 692, 1800))

with MBtiles(outfilename, mode) as out:
    if mode == "w":
        out.meta = {
            "name": "elevation",
            "description": "Mapzen Terrarium Elevation Tiles",
            "version": "1.0",
            "attribution": "Mapzen",
            "credits": "Mapzen",
            "type": "overlay",
            "format": "png",
            # 'bounds': ','.join('{0:4f}'.format(v) for v in bounds),
            # 'center': [0,0,0],
            # 'minzoom': str(zoom[0]),
            # 'maxzoom': str(zoom[-1]),
        }

    session = Session()

    for tile in tiles:
        if mode == "r+" and skip_existing and out.has_tile(*tile):
            continue

        url = TILE_URL.format(z=tile.z, x=tile.x, y=tile.y)
        length = int(session.head(url).headers["Content-Length"])

        if length != EMPTY_TILE:
            print("downloading {}".format(url))
            data = session.get(url).content

            if data:
                out.write_tile(*tile, data)

        else:
            print("empty tile: {}".format(tile))

