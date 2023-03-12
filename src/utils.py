import re
import json
import ijson

def create_chunk(file_path: str, n_rank):
    """
    Divide the dataset into chunks for each individual process
    """
    with open(file_path, 'rb') as file:
        

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