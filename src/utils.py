import json

STATE_ABRV_DICT = {'(nsw)': 'new south wales', '(vic.)': 'victoria', '(qld)': 'queensland', 
                       '(nt)': 'northern territory', '(sa)': 'south australia', '(wa)': 'western australia', 
                       '(tas.)': 'tasmania'}


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

            # Directly jump to the end of size allocation and check if position reach the end
            # of the dataset
            if f.seek(f.tell() + int(size_per_core)) >= dataset_size:
                block_end = dataset_size
                yield block_start, block_end
                break
            line = f.readline()

            # Keep reading until a tweet record is complete
            # Prevent reading stop in the middle of a tweet record
            while True:
                if line == '  },\n' or line == '  }\n':
                    block_end = f.tell()
                    break
                line = f.readline()
            if block_end > dataset_size:
                block_end = dataset_size

            # Add result to a generator    
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
    state_abrv_dict --- dictionary mapping state abbreviations to full names
    """
    location_dict = {'1gsyd': set(), '2gmel': set(), '3gbri': set(), '4gade': set(), '5gper': set(), '6ghob': set(), '7gdar': set()}
 
    with open(file_path, 'r') as f:
        for location_name, value in json.load(f).items():
            gcc = value['gcc']
            if gcc in location_dict:
                if ' - ' in location_name:
                    new_name = location_name.split('(')[1].split(' - ')[0]
                else:
                    new_name = location_name
                location_dict[gcc].add(new_name)
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
                 '5gper': '(Greater Perth)', '6ghob': '(Greater Hobart)',
                 '7gdar': '(Greater Darwin)'}
    print(f"{'Greater Capital City': <30}{'Number of Tweets Made': >20}")
    for gcc, tweet_count in gcc_counter.most_common():
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

def print_most_cities_count(city_counter):
    # define a custom key function to sort by number of items and sum of nested dictionary values
    def sort_key(item):
        return (-len(item[1]), -sum(item[1].values()))

    # sort the dictionary by custom key function and take the top 10 items
    sorted_dict_items = dict(sorted(city_counter.items(), key=sort_key)[:10])

    # iterate through the dictionary using a for loop
    print(f"{'Rank': <8}{'Author ID': <30}{'Number of Unique City Locations and #Tweets': ^15}")
    for rank, (author_id, value) in enumerate(sorted_dict_items.items(), start=1):
        total = sum(value.values())
        str_breakdown = ','.join(f"{number}{place}" for place, number in value.items())
        print(f"{'#' + str(rank): <8}{author_id: <30}{str(len(value))}(#{total} tweets - {str_breakdown})")


def fix_json(txt):
    # Get a substring between the first { and the last } in a string
    if txt[0] != '{':
        return txt[1:]
    return txt