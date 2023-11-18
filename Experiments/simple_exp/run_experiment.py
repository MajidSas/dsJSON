import subprocess
import time

def run_cmd(command):
    t = time.time()
    subprocess.run(command)
    return time.time() - t


### This experiments shows the scalability and benefit of avoiding data 
### conversion to JSONLines, since extraction queries avoid this step

data_sizes = [16, 32, 64, 128]           # data sizes and paralleims must be big enough to observe effect
master = 'spark://ec-hn.cs.ucr.edu:7077' # replace with 'local' to use local mode
hdfs = 'hdfs://ec-hn.cs.ucr.edu:8040'    # replace with 'local' to use local mode
max_partition = 1073741824 # 1GB
min_partition = 33554432   # 32MB


convertion_times = [0.0]*len(data_sizes)

for i in range(len(data_sizes)):
    print('Data setup: processing %d out of %d' % (i+1, len(data_sizes)))
    s = data_sizes[i]
    # DATA SETUP
    # duplicate the data size to be used as the input
    command = ['python', 'bestbuy-duplicate.py', str(s)]
    result = run_cmd(command)

    # Convert the large JSON record to a JSONLines file
    # and measure time. This uses a simple script without
    # any parsing.
    command[1] = 'bestbuy_converter.py'
    convertion_times[i] = run_cmd(command)

    # Copy the data to hdfs (this is not needed if running locally)
   if 'hdfs' in hdfs: # only upload in hdfs is used
	    command = ['hdfs', 'dfs', '-put', 'bestbuy_%s.json' % s, './datasets/'] 
	    result = run_cmd(command)
	    command[3] = 'bestbuy_%s.jsonl' % s
	    result = run_cmd(command)
print(convertion_times)



sparkjayway_times = [0.0]*len(data_sizes)
spark_times = [0.0]*len(data_sizes)
dsjson_times = [0.0]*len(data_sizes)

for i in range(len(data_sizes)):
    print('Processing %d out of %d' % (i+1, len(data_sizes)))
    s = data_sizes[i]

    command = ['spark-submit', '--master', master,
               '--conf',  'spark.sql.files.maxPartitionBytes=1073741824',
               '--conf', 'spark.sql.files.minPartitionBytes=33554432',
               '--class', 'edu.ucr.cs.bdlab.Main', 'spark_jsonline.jar', 'foreach', hdfs, './datasets/bestbuy_%d.jsonl' % s, '']
    spark_times[i] = run_cmd(command)

    command = ['spark-submit', '--master', master,
               '--conf',  'spark.sql.files.maxPartitionBytes=1073741824',
               '--conf', 'spark.sql.files.minPartitionBytes=33554432',
               '--class', 'edu.ucr.cs.bdlab.Main', 'spark_jayway.jar', 'foreach', hdfs, './datasets/bestbuy_%d.jsonl' % s, '$']
    sparkjayway_times[i] = run_cmd(command)
    
    command = ['spark-submit', '--master', master,
               '--conf',  'spark.sql.files.maxPartitionBytes=1073741824',
               '--conf', 'spark.sql.files.minPartitionBytes=33554432',
               '--class', 'edu.ucr.cs.bdlab.Main', 'dsJSON-assembly-0.1.jar',
               'foreach', hdfs, './datasets/bestbuy_%d.json' % s, "$.products[*]", "speculation", "" ]
    dsjson_times[i] = run_cmd(command)

print('dataSize\t\tdsJSON\t\tconversion+Spark\t\tconversion+SparkJayWay')

for i in range(len(data_sizes)):
    print(*[data_sizes[i],
            dsjson_times[i],
            convertion_times[i]+spark_times[i],
            convertion_times[i]+sparkjayway_times[i]],
            sep='\t\t')


    


