#!/usr/bin/python


import sys

import os













def main():

    cfg_file = sys.argv[1]
    out_file = sys.argv[2]

    line_map = {}
    for line in open(cfg_file).readlines():
        feature_id = line.strip().split('=')[0].strip()
        line_map[feature_id] = line.strip()

    for line in open(out_file).readlines():
        feature_id = line.strip().split(':')[0].strip()
        if feature_id in line_map:
            print line_map[feature_id]


if __name__=='__main__':
    main()
