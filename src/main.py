import argparse
import os
import time
import numpy as np

from mpi4py import MPI
from collections import Counter
from utils import extract_location, extract_user, load_geo_location, load_tweet, create_block

start_time = time.time()
COMM = MPI.COMM_WORLD
SIZE = COMM.Get_size()
RANK = COMM.Get_rank()

def main(geo_file_path, twitter_data_path):
    """
    Arguments:
    
    geo_file_path: path of Australia geo location code data
    twitter_data_path: path of twitter data file
    """

    if RANK == 0:
        # Read location data from file
        location_dict = load_geo_location(geo_file_path)
        dataset_size = os.path.getsize(twitter_data_path)
        size_per_core = dataset_size / SIZE

        # Divide tweet data into blocks for each core
        block_list = []
        for block_start, block_end in create_block(twitter_data_path, dataset_size, size_per_core):
            block_list.append({"block_start": block_start, "block_end": block_end})

    else:
        block_list = None

    print(block_list)

    scattered_data = COMM.scatter(block_list, root = 0)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = 'Social Media Analytics')
    parser.add_argument('-dataset', type = str, help = 'Path to twitter dataset')
    parser.add_argument('-location', type = str, help = 'Path to a list of location code')
    args = parser.parse_args()
    dataset = args.dataset
    location = args.location
    print(1)
    main(location, dataset)