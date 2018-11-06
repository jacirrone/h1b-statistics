import sys,json,os
from DataFrame import *
Format_dir = 'src/formats/'


def top_values(dataset, feature):
    feature, counts = dataset.unique_column_values(feature, return_counts=True, ignore_na=True)
    #sort in descending order based on frequency and then alphabetically
    sorted_ = sorted( zip(feature,counts), key=lambda val:(-val[1], val[0]))
    unzipped_ = list(zip(*sorted_))
    return list(unzipped_[0]), list(unzipped_[1])

#Compute the topk statistics
def topk_stats(dataset, feature, k=10):
    features, counts = top_values(dataset, feature)
    total = sum(counts)
    percent_conversion = [100.0 / total] * k
    percents = [str(round(a*b, 1)) +'%' for a,b in zip(counts, percent_conversion)]
    return [[ features[i] , counts[i], percents[i]] for i in range(0, min(k, len(counts))) ]

def get_occupation_stats(dataset):
    certified_applications = dataset.filter('CASE_STATUS', "CERTIFIED")
    results = topk_stats(certified_applications, 'SOC_NAME')
    col_names = ['TOP_OCCUPATIONS','NUMBER_CERTIFIED_APPLICATIONS','PERCENTAGE' ]
    return DataFrame(results, columns= col_names)

def get_state_stats(dataset):
    certified_applications = dataset.filter('CASE_STATUS', "CERTIFIED")
    results = topk_stats(certified_applications, 'WORKSITE_STATE')
    col_names = ['TOP_STATES','NUMBER_CERTIFIED_APPLICATIONS','PERCENTAGE' ]
    return DataFrame(results, columns= col_names)


def adjust_col_names(raw_data):
    fileformats = os.listdir(Format_dir)
    standard_mapping = {}
    for fileformat in fileformats:
        new_mapping = json.loads(open(Format_dir+fileformat).read())
        #use the latest mapping
        standard_mapping = {**standard_mapping, **new_mapping}
    return raw_data.rename_columns(columns = standard_mapping)

def main_compute_h1b_stats(input_file, occupation_stat_file, state_stat_file):
    input_data = DataFrame.read_csv(input_file)
    data = adjust_col_names(input_data)
    data = DataFrame.read_csv(input_file)
    occupation_stats = get_occupation_stats(data)
    state_stats = get_state_stats(data)
    occupation_stats.to_csv(occupation_stat_file)
    state_stats.to_csv(state_stat_file)

main_compute_h1b_stats(sys.argv[1], sys.argv[2], sys.argv[3])
