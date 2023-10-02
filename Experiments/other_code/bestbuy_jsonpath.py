import json

from jsonpath_ng import jsonpath, parse

f = open('/home/msaee007/datasets/final/bestbuy_32.json')

json_data = json.load(f)

jsonpath_expression = parse('$.products[*].categoryPath[*]')
match = jsonpath_expression.find(json_data)
print(len(match))
