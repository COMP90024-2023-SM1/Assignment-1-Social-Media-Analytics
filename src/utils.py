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
    return location.lower()#.split(',')[0].lower()


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
    This function returns a dictionary of the area names in each
    greater capital cities in Australia

    Arguments:
    file_path --- path to the file
    """
    location_dict = {'1gsyd':[], '2gmel':[], '3gbri':[], '4gade':[], '5gper':[], '6ghob':[]}
    state_abrv_dict = {'(nsw)': 'New South Wales', '(vic.)': 'Victoria', '(qld)': 'Queensland', 
                       '(sa)': 'South Australia', '(wa)': 'Western Australia', '(tas.)': 'Tasmania'}
    with open(file_path, 'rb') as f:
        for location_name, value in json.load(f).items():

            # Check if the location is within a greater capital city
            if value['gcc'] in location_dict.keys():
                if location_name.split(' ')[-1] in state_abrv_dict.keys():
                    new_name = location_name[0:len(location_name) - len(location_name.split(' ')[-1]) - 1]
                    new_name = new_name + ', ' + state_abrv_dict[location_name.split(' ')[-1]]
                    location_dict[value['gcc']].append(new_name)
                    print(new_name)
                else:
                    location_dict[value['gcc']].append(location_name) 

    return location_dict

def load_tweet(file_path: str):
    with open(file_path, 'r') as tweet_file:
        tweet = json.load(tweet_file)
        return tweet

def print_result_gcc_count(gcc_counter):
    """
    Print result for each capital cities

    Arguments:
    gcc_counter --- counter for the cities
    """
    long_name = {'1gsyd': '(Greater Sydney)', '2gmel': '(Greater Melbourne)',
                 '3gbri': '(Greater Brisbane)', '4gade': '(Greater Adelaide)',
                 '5gper': '(Greater Perth)', '6ghob': '(Greater Hobart)'}
    print(f"{'Greater Capital City': <30}{'Number of Tweets Made': >20}")
    for gcc, tweet_count in gcc_counter.items():
        print(f"{gcc + ' ' + long_name[gcc] : <30}{tweet_count : >12}")

def print_most_common_user(user_counter):
    """
    Print result for common user counting

    Arguments:
    user_counter --- counter for the users
    """
    print(f"{'Rank': <8}{'Author ID': <30}{'Number of Tweets Made': ^15}")
    rank = 1
    user_counter = user_counter.most_common(10)
    for author_id, tweet_count in user_counter:
        print(f"{'#' + str(rank) : <8}{author_id : <30}{tweet_count : ^15}")
        rank += 1

def print_most_cities_count(cities_counter):

    # define a custom key function to sort by number of items and sum of nested dictionary values
    def sort_key(item):
        return (-len(item[1]), -sum(item[1].values()))

    # sort the dictionary by custom key function and take the top 5 items
    sorted_dict_items = dict(sorted(cities_counter.items(), key=sort_key)[:10])
    # iterate through the dictionary using a for loop
    rank = 1
    print(f"{'Rank': <8}{'Author ID': <30}{'Number of Unique City Locations and #Tweets': ^15}")
    for author_id, value in sorted_dict_items.items():
        total = sum(value.values())
        str_breakdown = ''
        i = 0
        for place, number in value.items():
            if i == 0:
                str_breakdown = str_breakdown + str(number) + str(place)
            else:
                str_breakdown = str_breakdown + ',' + str(number) + str(place)
            i += 1
        print(f"{'#' + str(rank) : <8}{author_id : <30}{str(len(value)) + '(#' + str(total) + ' tweets - '}{str_breakdown + ')'}")
        rank += 1