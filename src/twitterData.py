import json

from collections import Counter, defaultdict
from utils import *


class twitterData():

    def __init__(self):
        """
        Initialise tweeter data object for each worker process with 
        location counter and user tweet counter
        """

        self.location_counter = Counter()
        self.city_counter = defaultdict(lambda: defaultdict(int))

    def process_tweet(self, tweet, location_dict):
        """
        This function takes a tweet and extract its location and user
        ID and perform counting

        Arguments:
        tweet --- a single tweet record in JSON format
        """
        
        # gcc_list = ['1gsyd', '2gmel', '3gbri', '4gade', '5gper', '6ghob', '7gdar']
        tweet_user = tweet[0]
        tweet_location = tweet[1]
        for gcc, location in location_dict.items():
            if tweet_location in location or tweet_location.split(' ')[0] in location:
                if tweet_user == '702290904460169216':
                    print('Process:', tweet_location, gcc)
                    print('---------')
                self.location_counter[gcc] += 1
                self.city_counter[tweet_user][gcc] += 1
                break


    def tweet_processer(self, file_path, geo_file_path, block_start, block_end):
        """
        Function that allows individual worker process to read and
        process tweets

        Arguments:
        file_path --- path to the dataset
        block_start --- byte starting position allocated for the worker process
        block_end --- byte ending position allocated for the worker process
        """
        location_dict = load_geo_location(geo_file_path)

        with open(file_path, 'rb') as file:
            tweet = []
            if block_start == 0:
                block_start = 4
            file.seek(block_start)
            pattern_author_id = r'"author_id":\s*"(\d+)"'
            pattern_city = r'"full_name": "([^"]+)"'
            while file.tell() != block_end:
                # Read the file line by line, treat them as string rather than json
                line = file.readline().decode('utf-8')
                if ('author_id' in line): # If author_id is in the line, extract the author_id
                    match_author_id = re.search(pattern_author_id, line)
                    if match_author_id:
                        author_id = match_author_id.group(1)
                        tweet.append(author_id)
                elif ('full_name' in line): # If full_name is in the line, extract the city
                    match_city = re.search(pattern_city, line)
                    if match_city:
                        city = match_city.group(1)
                        if '702290904460169216' in tweet:
                            print('Load:',city)
                        tweet.append(reformat_string(city))
                        if '702290904460169216' in tweet:
                            print('Formatted:', reformat_string(city))
                        self.process_tweet(tweet, location_dict)
                        tweet = []
                    
    def get_processed_result(self):
        """
        This function returns the results in dictionary form
        """
        return {"gcc_count": self.location_counter,
                "city_counter": dict(self.city_counter)}