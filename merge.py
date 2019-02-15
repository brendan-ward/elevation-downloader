import sqlite3

from pymbtiles import MBtiles, Tile

BATCH_SIZE = 1000

srcfilename = "../data/elevation2.mbtiles"
targetfilename = "../data/elevation.mbtiles"


with MBtiles(targetfilename, "r+") as target:
    with sqlite3.connect(srcfilename) as src:
        cursor = src.cursor()

        offset = 0
        while True:
            print("offset {}".format(offset))
            cursor.execute(
                "select zoom_level, tile_column, tile_row from tiles limit {limit} offset {offset}".format(
                    offset=offset, limit=BATCH_SIZE
                )
            )
            offset += BATCH_SIZE

            tiles_to_copy = cursor.fetchall()
            count = len(tiles_to_copy)

            tiles_to_copy = [
                tile for tile in tiles_to_copy if not target.has_tile(*tile)
            ]
            print("{} tiles to copy".format(len(tiles_to_copy)))

            tiles = []
            for z, x, y in tiles_to_copy:
                data = cursor.execute(
                    "SELECT tile_data FROM tiles "
                    "where zoom_level=? and tile_column=? and tile_row=? LIMIT 1",
                    (z, x, y),
                ).fetchone()[0]

                tiles.append(Tile(z, x, y, data))

            target.write_tiles(tiles)

            if count < BATCH_SIZE:
                print("No more tiles to fetch")
                break

