import json
import re
from collections import defaultdict

STATE_ABRV_DICT = {'(nsw)': 'new south wales', 
                   '(vic.)': 'cictoria', 
                   '(qld)': 'queensland', 
                   '(sa)': 'south australia', 
                   '(wa)': 'western australia', 
                   '(tas.)': 'tasmania', 
                   '(nt)': 'northern territory'}

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
    location_dict = defaultdict(set)
    
    with open(file_path, 'r') as f:
        for location_name, value in json.load(f).items(): 
            gcc = value['gcc']
            if location_name.split(' ')[-1] in STATE_ABRV_DICT.keys():
                    new_name = location_name[0:len(location_name) - len(location_name.split(' ')[-1]) - 1]
                    new_name = reformat_string(new_name + ' ' + STATE_ABRV_DICT[location_name.split(' ')[-1]])
            elif ' - ' in location_name:
                    new_name = location_name.split('(')
                    if len(new_name) == 1:
                        continue
                    new_name = reformat_string(new_name[1].split(' - ')[0])
            else:
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
    rank = 1
    for gcc, tweet_count in gcc_counter.most_common():
        if gcc in long_name.keys():
            print(f"{str(rank) + gcc[1:] + ' ' + long_name[gcc] : <30}{tweet_count : >12}")
            rank += 1

def print_most_common_user(user_counter):
    """
    Print result for common user counting

    Arguments:
    user_counter --- counter for the users
    """
    # Convert defaultdict to dict
    user_counter = dict(user_counter)
    print(f"{'Rank': <8}{'Author ID': <30}{'Number of Tweets Made': ^15}")
    top_k = 10
    rank = 1
    user_counter = sorted([(key, sum(values[k] for k in values)) for key, values in user_counter.items()], key=lambda x: x[1], reverse=True)[:top_k]
    for author_id, tweet_count in user_counter:
        print(f"{'#' + str(rank) : <8}{author_id : <30}{tweet_count : ^15}")
        rank += 1

def print_most_cities_count(city_counter):
    gcc_list = ['1gsyd', '2gmel', '3gbri', '4gade', '5gper', '6ghob', '7gdar']
    
    # Convert defaultdict to dict
    city_counter = dict(city_counter)

    # Remove items with keys not in gcc_list
    city_counter = {k: {k2: v2 for k2, v2 in v.items() if k2 in gcc_list} for k, v in city_counter.items()}

    # Sort items based on the number of unique cities and the sum of the tweets
    top_k = 10
    city_counter = dict(sorted(city_counter.items(), key=lambda x: (len(x[1]), sum(x[1].values())), reverse=True)[:top_k])

    # iterate through the dictionary using a for loop
    print(f"{'Rank': <8}{'Author ID': <30}{'Number of Unique City Locations and #Tweets': ^15}")
    for rank, (author_id, value) in enumerate(city_counter.items(), start=1):
        total = sum(value.values())
        str_breakdown = ','.join(f"{number}{place[1:]}" for place, number in value.items())
        print(f"{'#' + str(rank): <8}{author_id: <30}{str(len(value))}(#{total} tweets - {str_breakdown})")

def reformat_string(txt):
    # Remove all non-alphanumeric characters & additioal spaces and convert to lowercase
    txt = re.sub(r'[^a-zA-Z0-9,\s]', ' ', txt).lower()
    txt = re.sub(r'\s+', ' ', txt).strip()
    return txt
