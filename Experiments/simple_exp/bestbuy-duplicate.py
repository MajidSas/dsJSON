import json
import sys

factor = int(sys.argv[1])

f2 = open('./datasets/bestbuy_%d.json' % factor, 'w')

# write the header of the data
f2.write("""{"total":697760,"canonicalUrl":"\/v1\/products?page=3754&format=json&apiKey=cgmundjynjkgp6qrfttc3dud","totalTime":"0.979","totalPages":69776,"queryTime":"0.876","from":37531,"to":37540,"currentPage":3754,"partial":false,"products":[\n\n""")

productId = 0
f = open("./datasets/bestbuy_large_record.json", "r")
data = json.load(f)
for i in range(factor):
    for p in data['products']:
        if(productId > 0):
            f2.write("\n,\n")
        p['productId'] = productId
        productId += 1
        f2.write(json.dumps(p))
    f.close()
 
f2.write("\n\n]}")
f2.close()
f.close()