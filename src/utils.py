import json
import re

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
                block_end = f.tell()
                break

def load_geo_location(file_path: str):
    """
    This function returns a dictionary of the area names in each
    greater capital cities in Australia

    Arguments:
    file_path --- path to the file
    """
    location_dict = {'1gsyd': set(), '2gmel': set(), '3gbri': set(), '4gade': set(),
                     '5gper': set(), '6ghob': set(), '7gdar': set(), '8acte': set()}
 
    with open(file_path, 'r') as f:
        for location_name, value in json.load(f).items():
            gcc = value['gcc']
            if gcc in location_dict:
                new_name = reformat_string(location_name)
                location_dict[gcc].add(new_name)
    return location_dict

def print_result_gcc_count(gcc_counter):
    """
    Print result for each capital cities

    Arguments:
    gcc_counter --- counter for the cities
    """
    long_name = {'1gsyd': '(Greater Sydney)', '2gmel': '(Greater Melbourne)',
                 '3gbri': '(Greater Brisbane)', '4gade': '(Greater Adelaide)',
                 '5gper': '(Greater Perth)', '6ghob': '(Greater Hobart)',
                 '7gdar': '(Greater Darwin)', '8acte': '(Greater Canberra)'}
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
        str_breakdown = ','.join(f"{number}{place[1:]}" for place, number in value.items())
        print(f"{'#' + str(rank): <8}{author_id: <30}{str(len(value))}(#{total} tweets - {str_breakdown})")

def reformat_string(txt):
    # Remove all non-alphanumeric characters & additioal spaces and convert to lowercase
    match = re.search(r'[^\w\s]', txt)
    if match:
        result = txt[:match.start()]
    else:
        result = txt
    return result.rstrip().lower()
