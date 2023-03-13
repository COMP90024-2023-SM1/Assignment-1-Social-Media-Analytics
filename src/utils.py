import re
import json
import ijson

def create_block(file_path: str, dataset_size, size_per_core: int):
    """
    Divide the dataset into block for each individual process

    Arguments:
    file_path --- path to the dataset
    dataset_size --- size of the dataset
    size_per_core --- allocated size for each core
    """
    with open(file_path, 'r', encoding='utf-8') as f:

        # Obtain the current position in the file stream
        block_end = f.tell()
        while True:
            block_start = block_end

            if f.seek(f.tell() + int(size_per_core)) >= dataset_size:
                block_end = dataset_size
                yield block_start, block_end
                break
            line = f.readline()

            # Keep reading until a tweet record is complete
            while True:
                if line == '  },\n' or line == '  }\n':
                    block_end = f.tell()
                    break
                line = f.readline()
            if block_end > dataset_size:
                block_end = dataset_size
            yield block_start, block_end
            if block_end == dataset_size:
                break

def extract_location(tweet):
    """
    Extract the location of a given tweet

    Arguments:
    tweet --- tweet in JSON format
    """
    location = tweet['includes']['places'][0]['full_name']
    return location


def extract_user(tweet):
    """
    Extract the user of a given tweet

    Arguments:
    tweet --- tweet in JSON format
    """
    author_id = tweet['data']['author_id']
    return author_id

def load_geo_location(file_path: str):
    """
    Load the file containing geo location of Australia

    Arguments:
    file_path --- path to the file
    """
    with open(file_path, 'r') as geo_file:
        geo_location = json.load(geo_file)
        return geo_location

def load_tweet(file_path: str):
    with open(file_path, 'r') as tweet_file:
        tweet = json.load(tweet_file)
        return tweet

def print_result_city_count(city_counter):
    """
    Print result for each capital cities

    Arguments:
    city_counter --- counter for the cities
    """

def print_most_common_user(user_counter):
    """
    Print result for common user counting

    Arguments:
    user_counter --- counter for the users
    """