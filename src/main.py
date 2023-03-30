import argparse
import os
import time
import numpy as np

from mpi4py import MPI
from collections import Counter
from utils import extract_location, extract_user, load_geo_location, load_tweet, create_block, print_most_common_user, print_result_gcc_count, print_most_cities_count
from twitterData import twitterData

start_time = time.time()
COMM = MPI.COMM_WORLD
SIZE = COMM.Get_size()
RANK = COMM.Get_rank()

def main(geo_file_path, twitter_data_path):
    """
    Arguments:
    geo_file_path --- path of Australia geo location code data
    twitter_data_path --- path of twitter data files
    """
    # Initialise twitter class for the current process
    twitter_data = twitterData()

    if RANK == 0:
        # Equally split dataset to each process
        dataset_size = os.path.getsize(twitter_data_path)
        # print("Dataset file total size = " + str(dataset_size))
        size_per_core = dataset_size / SIZE

        # Divide tweet data into blocks for each core
        block_list = []
        for block_start, block_end in create_block(twitter_data_path, dataset_size, size_per_core):
            block_list.append({"block_start": block_start, "block_end": block_end})

    else:
        block_list = None

    # Distribute data blocks to different cores
    scattered_data = COMM.scatter(block_list, root = 0)
    print("RANK #" + str(RANK) + " responsible for block between byte " + str(
        scattered_data['block_start']) + " and byte " + str(scattered_data['block_end']))

    # Each process starts to process their allocated data blocks
    twitter_data.tweet_processer(twitter_data_path, geo_file_path, 
                                 scattered_data['block_start'], 
                                 scattered_data['block_end'])
    
    processed_results = twitter_data.get_processed_result()
    # Gather result from each process
    combined_results = COMM.gather(processed_results, root = 0)
    
    # Add all the calculation together in process 0 and print result
    if RANK == 0:
        gcc_count_combined = Counter()
        user_count_combined = Counter()
        city_count_combined = dict()

        # Sum up the counter from each process
        for i in combined_results:
            gcc_count_combined += i['gcc_count']
            user_count_combined += i['user_count']
            city_count_combined.update(i['city_counter'])

        print("\n=================== Results ===================\n")
        print("Top 10 Tweeters")
        print_most_common_user(user_count_combined)
        print("\nTotal Number of Tweets in Various Capital Cities")
        print_result_gcc_count(gcc_count_combined)
        print("\nTop 10 Number of Unique City Locations and #Tweets")
        print_most_cities_count(city_count_combined)
        run_time = time.time() - start_time
        print('\nPROGRAM RUN TIME: ' + str(run_time))
        

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = 'Social Media Analytics')
    parser.add_argument('-dataset', type = str, help = 'Path to twitter dataset')
    parser.add_argument('-location', type = str, help = 'Path to a list of location code')
    args = parser.parse_args()
    dataset = args.dataset
    location = args.location
    main(location, dataset)