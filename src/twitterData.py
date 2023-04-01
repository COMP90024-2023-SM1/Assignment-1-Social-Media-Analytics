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
        self.user_counter = Counter()
        self.city_counter = defaultdict(lambda: defaultdict(int))

    def process_tweet(self, tweet, location_dict):
        """
        This function takes a tweet and extract its location and user
        ID and perform counting

        Arguments:
        tweet --- a single tweet record in JSON format
        """

        tweet_user = extract_user(tweet)
        self.user_counter[tweet_user] += 1

        # gcc_list = ['1gsyd', '2gmel', '3gbri', '4gade', '5gper', '6ghob', '7gdar']
        tweet_location = extract_location(tweet)
        for gcc, location in location_dict.items():
            if tweet_location in location:
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
            one_tweet = ''
            if block_start == 0:
                block_start = 4
            
            file.seek(block_start)
            while file.tell() != block_end:
                line = file.readline().decode('utf-8')

                # If reached the end of a tweet record
                if line == '  },\n' or line == '  }\n':
                    one_tweet += line.split(',')[0]
                    one_tweet = fix_json(one_tweet) # fix the format
                    self.process_tweet(json.loads(one_tweet), location_dict)

                    # Reset tweet after processed
                    one_tweet = ''
                else:
                    one_tweet += line
                    
    def get_processed_result(self):
        """
        This function returns the results in dictionary form
        """
        return {"gcc_count": self.location_counter, 
                "user_count": self.user_counter, 
                "city_counter": dict(self.city_counter)}