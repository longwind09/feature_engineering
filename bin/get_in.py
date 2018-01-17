#!/usr/bin/python


import sys

import os













def main():

    in_file = sys.argv[1]
    out_file = sys.argv[2]

    line_set = set()
    for line in open(out_file).readlines():
        line_set.add(line.strip().split(':')[0])

    for line in open(in_file).readlines():
        if line.strip().split(':')[0] in line_set:
            print line,


if __name__=='__main__':
    main()
