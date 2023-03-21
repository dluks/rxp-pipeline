import argparse
import glob
import json
import multiprocessing
import os

import pdal


def tile_index(ply, args):
    """Takes a ply filename and args object and appends the X, Y tile origin to a
    tile index file.

    Args:
        ply (str): Path to the ply file
        args (obj): argparse Namespace object
    """
    if args.verbose:
        with args.Lock:
            print(f"processing: {ply}")

    reader = {"type": f"readers{os.path.splitext(ply)[1]}", "filename": ply}
    stats = {"type": "filters.stats", "dimensions": "X,Y"}
    JSON = json.dumps([reader, stats])
    pipeline = pdal.Pipeline(JSON)
    pipeline.execute()
    metadata = pipeline.metadata
    X = metadata["metadata"]["filters.stats"]["statistic"][0]["average"]
    Y = metadata["metadata"]["filters.stats"]["statistic"][1]["average"]
    T = int(os.path.split(ply)[1].split(".")[0])

    with args.Lock:
        with open(args.tile_index, "a") as fh:
            fh.write(f"{T} {X} {Y}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--idir", type=str, required=True, help="directory where tiles are stored")
    parser.add_argument("-t", "--tile-index", default="tile_index.dat", help="tile index file")
    parser.add_argument("--num-prcs", type=int, default=10, help="number of cores to use")
    parser.add_argument("--verbose", action="store_true", help="print something")
    args = parser.parse_args()

    try:
        m = multiprocessing.Manager()
        args.Lock = m.Lock()
        pool = multiprocessing.Pool(args.num_prcs)
        pool.starmap(
            tile_index,
            [(ply, args) for ply in glob.glob(os.path.join(args.idir, "*.ply"))],
        )
    except Exception as e:
        print(e)
        pool.close()
    pool.close()
    pool.join()
