import argparse
import os
import time
import numpy as np

from mpi4py import MPI
from collections import Counter
from utils import extract_location, extract_user, load_geo_location, load_tweet

def main(geo_file_path, twitter_data_path):
    """
    Arguments:
    
    geo_file_path: path of Australia geo location code data
    twitter_data_path: path of twitter data file
    """
    start_time = time.time()
    COMM = MPI.COMM_WORLD
    SIZE = COMM.Get_size()
    RANK = COMM.Get_rank()

    if RANK == 0:
        # Read location data from file
        location_dict = load_geo_location(geo_file_path)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = 'Social Media Analytics')
    parser.add_argument('-dataset', type = str, help = 'Path to twitter dataset')
    parser.add_argument('-location', type = str, help = 'Path to a list of location code')
    args = parser.parse_args()
    dataset = args.dataset
    location = args.location

    main(location, dataset)