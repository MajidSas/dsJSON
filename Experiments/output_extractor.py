from glob import glob
import re
import pandas as pd
import numpy as np
def extract_task(l):
    if "Finished task" not in l:
        return -1, 0
    m = re.search(r'(?<=Finished task).*?(?=in stage)', l)
    tid = float(m.group(0))
    m = re.search(r'(?<=in stage ).*?(?=\(TID)', l)
    stage = float(m.group(0))

    m = re.search(r'(?<=\) in ).*?(?= on )', l)
    time = float(m.group(0)[:-2])

    return stage, time/1000

def extract_job(l):
    if "finished:" not in l:
        return "", -1, 0
    m = re.search(r'(?<=Job ).*?(?= finished)', l)
    job = float(m.group(0))
    if "collect at" in l:
        m = re.search(r'(?<=collect at ).*?(?=.scala)', l)
    else:
        m = re.search(r'(?<=finished: ).*?(?= at)', l)
    name = m.group(0)
    m = re.search(r'(?<=took ).*?(?= s)', l)
    time = float(m.group(0))

    return name, job, time

def extract_totaltime(l):
    m = re.search(r'(?<=system ).*?(?=elapsed)', l)
    parts = m.group(0).split(":")
    if len(parts) == 1:
        return float(parts[0])
    elif len(parts) == 2:
        return float(parts[0])*60 + float(parts[1])
    elif len(parts) == 3:
        return float(parts[0])*60*60 + float(parts[1])*60 + float(parts[2])
    


def extract_appid(l):
    m = re.search(r'(?<=with app ID ).*', l)
    return m.group(0)



# print_query = "count"
R = 5
print_dataset = "osm21"
print_option = ""
print(print_option)
# filepaths = glob('/Users/majid/Documents/json_exp_output/parti_schema_comp/parti_schema_comp/*_stderr.txt')
filepaths = glob('/Users/majid/Documents/json_exp/json_exp_output/*_stderr.txt')

# print(filepaths)
# total_times = pd.read_csv("/Users/majid/Documents/json_exp_output/total_time_log.csv",sep="\t")
values = {}

output = []

name_map = {
    "osm21": "O",
    "msbuildings": "M",
    "wikipedia": "W1",
    "wikipedia-all": "W2",
    "bestbuy16": "BestBuy",
    "imdb": "IMDB"
}

label_map = {
    # "forEach": "R",
    "full": "R",
    "count": "C",
    "count_selectiveEnd": "CE",
    "count_selectiveStart": "CS",
    "count_nofilter": "CF",
    "full_selectiveEnd": "FE",
    "full_selectiveStart": "FS",
    "full_nofilter": "FF",
    "selective": "E",
    "speculation": "S",
    "fullPass": "F",
    "jsonl": "L",
    "sparkjayway":"J",
    "beast": "B"
}

for path in filepaths:
    # if 'jayway' in path:
    #     continue
    if print_option not in path:
        continue
    split_path = path[path.rfind('/')+1:].replace("with_" + print_option, "").split('_')
    # print(path)
    # print(len(split_path))
    if(len(split_path) < 5):
        continue
    dataset = split_path[0]
    parser = split_path[1]
    query = split_path[2]
    if len(split_path[3]) > 1:
        query += "_" + split_path[3]

    # if query != print_query or parser != print_parser:
    if print_dataset not in dataset:
        continue
    
    if parser not in values:
        values[parser] = {}

    if dataset not in values[parser]:
        values[parser][dataset] = {}
    if(query not in  values[parser][dataset]):
        values[parser][dataset][query] = {
            'tasks': {},
            'jobs': {

                "total_time": 0
            }
        }

    f = open(path, 'r')

    for l in f.readlines():
        if "Finished task" in l:
            stage, time = extract_task(l)
            if stage not in values[parser][dataset][query]['tasks']:
                values[parser][dataset][query]['tasks'][stage] = time
            elif values[parser][dataset][query]['tasks'][stage] > time:
                values[parser][dataset][query]['tasks'][stage] = time
        if "finished:" in l:
            name, job, time = extract_job(l)
            # print(dataset, parser, query, name, time)
            if name not in values[parser][dataset][query]['jobs']:
                values[parser][dataset][query]['jobs'][name] = time
            else:
                values[parser][dataset][query]['jobs'][name] += time
        if "Connected to Spark cluster with app ID " in l:
            app_name = extract_appid(l)
        if "system " in l:
            values[parser][dataset][query]['jobs']["total_time"] += extract_totaltime(l)
        
    # tt = total_times[total_times["Application ID"] == app_name + " "].iloc[0]["Duration"]
    # if 'h' in tt:
    #     tt = float(tt[:-1])*60*60
    # elif 'm' in tt:
    #     tt = float(tt[:-3])*60
    # else:
    #     tt = float(tt[:-1])
    # values[parser][dataset][query]['total_time'] = tt

for parser in values:
    for query in values[parser][print_dataset]:
        dataset = print_dataset
        jobs = values[parser][dataset][query]['jobs']
        schemaInference = 0
        partitioning = 0
        query_time = 0
        # foreach = 0
        if 'SchemaInference' in jobs:
            schemaInference = round(jobs['SchemaInference']/R, 2)
        if 'load' in jobs:
            schemaInference = round(jobs['load']/R, 2)
        if 'Partitioning' in jobs:
            partitioning = round(jobs['Partitioning']/R, 2)
        if 'count' in jobs:
            query_time = round(jobs['count']/R, 2)
        if 'foreach' in jobs:
            query_time = round(jobs['foreach']/R, 2)

        # other = round(jobs['total_time']/3, 2) - (schemaInference+partitioning+query_time)
        total_time = round(jobs['total_time']/R, 2)
        # other = tt - (schemaInference+partitioning+query_time)
        # if other < 0:
        #     other += 30 # because the minutes may have been rounded down
        # other = round(, 2)
        # print(path)
        # print(*[parser+"_"+query, schemaInference, partitioning, count, foreach, other], sep=",")
        output += [[label_map[parser]+":"+label_map[query],
                    schemaInference, partitioning, query_time, total_time]]
        # output += [label_map[parser]+":"+label_map[query], total_time]
        # print(path[path.rfind('/')+1:])
        # print(values[parser][dataset][query])
output = np.array(output)
# print(output)
# print(output.shape)
sorted_array = output[np.argsort(output[:, 0])]

print("Label\tSchema\tParti\tParsing\tTotal_time")
for i in range(sorted_array.shape[0]):
    print(*sorted_array[i], sep="\t")