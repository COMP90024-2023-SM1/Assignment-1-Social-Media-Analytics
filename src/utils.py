import re
import json
import ijson

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