import json

f2 = open('/home/msaee007/datasets/bestbuy.json', 'w')
f3 = open('/home/msaee007/datasets/bestbuy.jsonl', 'w')
f2.write("""{"total":697760,"canonicalUrl":"\/v1\/products?page=3754&format=json&apiKey=cgmundjynjkgp6qrfttc3dud","totalTime":"0.979","totalPages":69776,"queryTime":"0.876","from":37531,"to":37540,"currentPage":3754,"partial":false,"products":[\n\n""")
factor=10
productId = 0
for i in range(factor):
    f = open("/home/msaee007/datasets/bestbuy_large_record.json", "r")
    data = json.load(f)
    for p in data['products']:
        if(productId > 0):
            f2.write("\n,\n")
        p['productId'] = productId
        productId += 1
        f2.write(json.dumps(p))
        f3.write(json.dumps(p)+"\n") 
    f.close()
 
f2.write("\n\n]}")
# Closing file
f.close()
f2.close()
f3.close()