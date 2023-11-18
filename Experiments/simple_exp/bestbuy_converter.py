import sys

factor = int(sys.argv[1])
name = 'bestbuy_%d' % factor

f1 = open('./datasets/%s.json' % name, 'r')
f2 = open('./datasets/%s.jsonl' % name, 'w')

for line in f1:
    if 'studio' in line:
        f2.write(line)
f1.close()
f2.close()