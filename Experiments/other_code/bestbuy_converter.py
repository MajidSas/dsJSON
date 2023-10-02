name = 'bestbuy_128'

f1 = open('/home/msaee007/datasets/final/%s.json' % name, 'r')
f2 = open('/home/msaee007/datasets/final/%s.jsonl' % name, 'w')

for line in f1:
    if 'studio' in line:
        f2.write(line)
f1.close()
f2.close()