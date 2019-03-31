from itertools import groupby
# refer to https://stackoverflow.com/questions/48163846/python-groupby-to-split-list-by-delimiter
# the usage of groupby
import sys
 
def read_mapper_output(file):
    lst = []
    for line in file:
	lst.append(line.strip().split(' '))
    return lst
 
def main():
    data = read_mapper_output(sys.stdin)
    for key, keyValGroup in groupby(data, lambda x:x[0]):
        values = ' '.join(sorted(val for key, val in keyValGroup))
        print ("%s %s" % (key, values))
 
if __name__ == "__main__":
    main()
