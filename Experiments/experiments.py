import subprocess
from subprocess import PIPE
import os
def execute_cmd(command, output_file):
    if os.path.isfile(output_file + "stderr.txt"):
        return
    print("Executing: " + output_file[output_file.rfind('/')+1:-1])
    result = subprocess.run(command, stdout=PIPE, stderr=PIPE)
    with open(output_file + "stdout.txt", "w") as text_file:
        text_file.write(result.stdout.decode('utf-8'))
    with open(output_file + "stderr.txt", "w") as text_file:
        text_file.write(result.stderr.decode('utf-8'))

jar_folder = "/home/msaee007/jars/" # this should point to the folder that contains the compiled JAR files
dataset_folder = "./datasets/" # this should point to the folder containing the datasets on hdfs or locally if it is not used 
local_dataset_folder = "/home/msaee007/datasets/final/" # this should point to the folder containing the datasets locally
# no-pushdown, projection, filters, 
pairs = [("nopushdown", "None"), ("pushdown", "projection_filters"), ("filters","filters"), ("projeciton", "projection")]
output_folder = "/home/msaee007/json_exp_output/with_nopushdown/"
jsondsp_jar = "dsJSON-assembly-0.1.jar" # this should be the name of the JAR files for dsJSON
pushdown_option = "None"
master =  "spark://ec-hn.cs.ucr.edu:7077" # replace your spark cluster address use 'local' for local mode
hdfs = "hdfs://ec-hn.cs.ucr.edu:8040/" # path to hdfs cluster or use 'local' for local mode
min_partition_size = "33554432" # 32MB
max_partition_size = "2147483648" # 1GB

for pair in pairs:
    output_folder = "/home/msaee007/json_exp_output/with_" + pair[0] # replace this folder with the path to store the output logs
    pushdown_option = pair[1]
    R = 5 # number of times to repeat an experiment
    params = [
        {
            "name": "bestbuy",
            "dataset": ["bestbuy.json", "bestbuy.jsonl"],
            "jsonpath": "$.products[*]",
            "filters": [
                ("selective",
                 "productId = 1809103",
                 "?(@.productId == 1809103)"),
                ("full",
                 "productId > -1 OR productId IS NULL",
                 "?(@.productId > -1 || @.productId == null)")
                ]
        },
        {
            "name": "bestbuy16",
            "dataset": ["bestbuy_16.json", "bestbuy_16.jsonl"],
            "jsonpath": "$.products[*]",
            "filters": [
                ("selectiveStart",
                "inStoreAvailability = true",
                "?(@.inStoreAvailability == true)"),
                ("selectiveEnd",
                "shippingCost > 3.5",
                "?(@.shippingCost > 3.5)")
                ("full",
                 "productId > -1 OR productId IS NULL",
                 "?(@.productId > -1 || @.productId == null)")
                ]
        }
        {
            "name": "bestbuy32",
            "dataset": ["bestbuy_32.json", "bestbuy_32.jsonl"],
            "jsonpath": "$.products[*]",
            "filters": [
                ("full",
                 "productId > -1 OR productId IS NULL",
                 "?(@.productId > -1 || @.productId == null)")
                ]
        },
        {
            "name": "bestbuy64",
            "dataset": ["bestbuy_64.json", "bestbuy_64.jsonl"],
            "jsonpath": "$.products[*]",
            "filters": [
                ("full",
                 "productId > -1 OR productId IS NULL",
                 "?(@.productId > -1 || @.productId == null)")
                ]
        },
        # {
        #     "name": "bestbuy128",
        #     "dataset": ["bestbuy_128.json", "bestbuy_128.jsonl"],
        #     "jsonpath": "$.products[*]",
        #     "filters": [
        #         ("full",
        #          "productId > -1 OR productId IS NULL",
        #          "?(@.productId > -1 || @.productId == null)")
        #         ]
        # },
        {
            "name": "imdb",
            "dataset": ["imdb/", "imdb_jsonl/"],
            "jsonpath": "$.[*]",
            "filters": [
                ("selective",
                 "movie LIKE 'Iron Man%'",
                 "?(@.movie startsWith \"Iron Man\")"),
                ("full",
                 "",
                 "?(@.movie != null)")
            ]
        },
        {
            "name": "msbuildings",
            "dataset": ["../MSBuildings/"],
            "jsonpath": "$.features[*]",
            "filters": [("selective", "", "$.features[?(@.geometry geometryWithin \"POLYGON((-118.21289062499999 34.279914398549934,-117.94921874999999 33.99347299511967,-117.26257324218749 33.63291573870479,-116.83959960937499 33.7243396617476,-116.98242187499999 34.338900400404995,-118.21289062499999 34.279914398549934))\")]")]
        },
        {
            "name": "wikipedia-all",
            "dataset": ["latest-all.json"],
            "jsonpath": "$.[*].claims..mainsnak",
            "filters": [("full", "datatype IS NOT NULL", "")]
        },
        {
            "name": "osm21",
            "dataset": ["all_nodes.geojson"],
            "jsonpath": "$.features[*]",
            "filters": [("selective", "", "$.features[?(@.geometry geometryWithin \"POLYGON((-118.21289062499999 34.279914398549934,-117.94921874999999 33.99347299511967,-117.26257324218749 33.63291573870479,-116.83959960937499 33.7243396617476,-116.98242187499999 34.338900400404995,-118.21289062499999 34.279914398549934))\")]")]
        }
    ]

    for options in params:
        name = options["name"]
        dataset = options["dataset"]
        jsonpath = options["jsonpath"]

        # COUNTING

        ## general-json
        command1 = ["time", "spark-submit",
                "--master", master,
                "--conf", "spark.sql.files.maxPartitionBytes="+max_partition_size,
                "--conf", "spark.sql.files.minPartitionBytes="+min_partition_size,
                "--class", "edu.ucr.cs.bdlab.Main",
                jar_folder+jsondsp_jar,
                "count",
                hdfs,
                dataset_folder + dataset[0],
                jsonpath,
                "speculation",
                "",
                pushdown_option]
        print(*command1, sep=" ")
        if name != "osm21":
            for i in range(R):
                output_file = output_folder + name + "_speculation_count_nofilter_" + str(i) +"_"
                execute_cmd(command1, output_file)

            command1[-2] = "fullPass"
            for i in range(R):
                output_file = output_folder + name + "_fullPass_count_" + str(i) +"_"
                execute_cmd(command1, output_file)

        ## Spark JSONLines
        # if len(dataset) > 1:
        #     jar = "sparkjsonreader.jar"
            # if "wikipedia" in dataset:
            #     jar = "spark_jayway.jar"
            # command2 = ["time", "spark-submit",
            #         "--master", master,
            #         "--conf", "spark.sql.files.maxPartitionBytes="+max_partition_size,
            #         "--conf", "spark.sql.files.minPartitionBytes="+min_partition_size,
            #         "--class", "edu.ucr.cs.bdlab.sparkjsonreader.Main",
            #         jar_folder+jar,
            #         "count",
            #         master,
            #         dataset_folder + dataset[1],
            #         ""]
            
            # for i in range(R):
            #     output_file = output_folder + name + "_jsonl_count_" + str(i) +"_"
            #     execute_cmd(command2, output_file)
            #     output_file = output_folder + "wikipedia-all" + "_jsonl_count_" + str(i) +"_"
            #     command2[-2] = dataset_folder + "latest-all.json"
            #     execute_cmd(command2, output_file)

            # if name in ["bestbuy", "imdb"]:
            #     command2[-1] = "$"
            #     command2[-2] = dataset_folder + dataset[1]
            #     command2[-5] = jar_folder+"spark_jayway.jar"
            #     print(*command2, sep=" ")
            #     for i in range(R):
            #         output_file = output_folder + name + "_sparkjayway_count_" + str(i) +"_"
            #         execute_cmd(command2, output_file)


        ## sequentail Jayway
        # if "wikipedia" not in name:
        #     command3 = ["time", "java",
        #             "-Xmx94g",
        #             "-Xms64g",
        #             "-XX:+UseParallelGC",
        #             "-cp", 
        #             jar_folder+"java-jsonpath.jar",
        #             "Main",
        #             local_dataset_folder + dataset[0],
        #             jsonpath
        #             ]
        #     print(*command3, sep=" ")
        #     output_file = output_folder + name + "_jayway_count_"
        #     execute_cmd(command3, output_file)


        command1[-7] = "forEach"
        for i in range(R):
            output_file = output_folder + name + "_speculation_full_nofilter_" + str(i) +"_"
            execute_cmd(command1, output_file)

        # WITH SELECTION
        for (filter_name, sql, jsonpath_filter) in options["filters"]:
            print("FILTERS")
            
            # if name == "wikipedia":
            #     # print("WIKIPEDIA")
            #     for i in range(R):
            #         output_file = output_folder + "wikipedia-all" + "_speculation_full_" + str(i) +"_"
            #         command1[-4] = dataset_folder + "latest-all.json"
            #         execute_cmd(command1, output_file)
            
            # command1[-2] = "fullPass"
            # for i in range(R):
            #     output_file = output_folder + name + "_fullPass_" + filter_name + "_" + str(i) +"_"
            #     execute_cmd(command1, output_file)

            
            ## JSONLine
            # command2[-4] = "forEach"
            # command2[-1] = sql

            # if len(dataset) > 1:
            #     if "wikipedia" not in dataset:
            #         for i in range(R):
            #             output_file = output_folder + name + "_jsonl_" + filter_name + "_" + str(i) +"_"
            #             execute_cmd(command2, output_file)
            #         command2[-5] = jar_folder+"spark_jayway.jar"
            #         command2[-1] = "$"
            #         for i in range(R):
            #             output_file = output_folder + name + "_sparkjayway_" + filter_name + "_" + str(i) +"_"
            #             execute_cmd(command2, output_file)
                    
            ## general-json
            command1[-2] = sql
            command1[-3] = "speculation"


            # if name == "osm21":
            #     command1[-3] = jsonpath_filter
            command1[-7] = "count"
            for i in range(R):
                output_file = output_folder + name + "_speculation_count_" + filter_name + "_" + str(i) +"_"
                execute_cmd(command1, output_file)

            command1[-7] = "forEach"
            for i in range(R):
                output_file = output_folder + name + "_speculation_full_" + filter_name + "_" + str(i) +"_"
                execute_cmd(command1, output_file)

            ## sequentail Jayway
            # if "wikipedia" not in name:
            #     command3[-1] = jsonpath[:-3] + "[" + jsonpath_filter + "]"
                
            #     output_file = output_folder + name + "_jayway_" + filter_name + "_"
            #     execute_cmd(command3, output_file)

    # BEAST
    # command4 = ["time", "/home/msaee007/beast-0.9.1/bin/beast",
    #                 "--master", master,
    #                 "--jars",  
    #                 jar_folder+"beast-counter.jar",
    #                 "--class", "edu.ucr.cs.bdlab.beastExamples.JavaExamples",
    #                 "./MSBuildings"]
    # print(*command4, sep=" ")

    # output_file = output_folder + "msbuildings_beast_count_"
    # execute_cmd(command4, output_file)

    # command4[5] = jar_folder+"beast-selective.jar"
    # for i in range(R):
    #     output_file = output_folder + "msbuildings_beast_selective_" + str(i) + "_"
    #     execute_cmd(command4, output_file)

    # command3 = ["time", "java",
    #                 "-Xmx94g",
    #                 "-Xms64g",
    #                 "-XX:+UseParallelGC",
    #                 "-cp", 
    #                 jar_folder+"java-jsonpath.jar",
    #                 "Main",
    #                 local_dataset_folder + "bestbuy_1.json",
    #                 jsonpath
    #                 ]
    # print(*command3, sep=" ")
    # output_file = output_folder + "bestbuy1_jayway_count_"
    # execute_cmd(command3, output_file)
