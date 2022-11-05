import os
import glob
import multiprocessing
import json
import argparse
import pdal


def tile_points(args):
    cmds = []
    reader = {"type": "readers.las", "filename": os.path.join(args.project, "*.las")}
    cmds.append(reader)
    merge = {"type": "filters.merge"}
    cmds.append(merge)
    split = {
        "type": "filters.splitter",
        "length": 10,
    }
    cmds.append(split)
    writer = {"type": "writers.las", "filename": "tile_#.las"}
    cmds.append(writer)

    # link commmands and pass to pdal
    JSON = json.dumps(cmds)
    pipeline = pdal.Pipeline(JSON)
    pipeline.execute()


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
        "-p", "--project", required=True, type=str, help="LAS files directory"
    )
    parser.add_argument("--odir", type=str, default=".", help="Output directory")
    parser.add_argument(
        "--tile",
        action="store_true",
        help="Boolean indicating whether or not the point cloud(s) should be tiled according to the --tilesize",
    )
    parser.add_argument(
        "-ts",
        "--tilesize",
        default=None,
        help="Size with which to tile (or re-tile) point cloud(s). If files are already tiled then they will be re-tiled.",
    )
    parser.add_argument(
        "--num-prcs", type=int, default=10, help="Number of cores to use"
    )
    parser.add_argument("--verbose", action="store_true", help="Print more stuff")

    args = parser.parse_args()

    args.las = list(enumerate(sorted(glob.glob(os.path.join(args.project, "*.las")))))

    if args.tile and not args.tilesize:
        # Set default tilesize
        args.tilesize = 15

    if args.tilesize and not args.tile:
        parser.error("--tile must be True if --tilesize is provided.")

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
