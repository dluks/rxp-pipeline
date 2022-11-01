import os
import glob
import multiprocessing
import json
import argparse
import pdal


def process_tile(tile, args):
    """Convert las tile to ply

    Args:
        tile (list): List of tuples containing tile ID and tile filepath,
            e.g. [(0, "path/to/tile.las")]
        args (object): argparse.Namespace object containing CLI argument values
    """
    # add zeroes to tile_id to ensure the name of saved tile is ###.ply, if tiles are not named in this format it seems to break the classification script
    tile_id_str = str(tile[0])
    while len(tile_id_str) < 3:
        tile_id_str = "0" + tile_id_str

    reader = {"type": f"readers{'.las'}", "filename": tile[-1]}
    writer = {
        "type": f"writers{'.ply'}",
        "storage_mode": "little endian",
        "dims": "X, Y, Z, Reflectance, Deviation, ReturnNumber, NumberOfReturns,",
        "filename": os.path.join(args.odir, f"{tile_id_str}.ply"),
    }
    JSON = json.dumps([reader, writer])

    pipeline = pdal.Pipeline(JSON)
    pipeline.execute()


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--project", "-p", required=True, type=str, help="LAS files directory"
    )
    parser.add_argument("--odir", type=str, default=".", help="output directory")
    parser.add_argument(
        "--pre-tiled",
        action="store_false",
        help="Data is pre-tiled and does not need additional tiling",
    )
    parser.add_argument(
        "--num-prcs", type=int, default=10, help="number of cores to use"
    )
    parser.add_argument("--verbose", action="store_true", help="print something")

    args = parser.parse_args()

    args.las = list(enumerate(sorted(glob.glob(os.path.join(args.project, "*.las")))))

    # read in and write to ply
    try:
        Pool = multiprocessing.Pool(args.num_prcs)
        m = multiprocessing.Manager()
        args.Lock = m.Lock()
        Pool.starmap(process_tile, [(las, args) for las in args.las])
    except Exception as e:
        Pool.close()
    Pool.close()
    Pool.join()
