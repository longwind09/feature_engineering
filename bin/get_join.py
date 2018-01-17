#!/usr/bin/python


import sys

import os




def main():

    in_file = sys.argv[1]
    out_file = sys.argv[2]
    cfg_file = sys.argv[3]

    last_in=''
    last=''
    fin = open(in_file)
    fcfg=open(cfg_file)
    for line in open(out_file).readlines():
        new = line.strip().split(':')[0].strip().split('~')[0]
        if  new != last:
            last_in = fin.readline().strip()
            last = new
        print '%s\t%s\t%s'%(fcfg.readline().strip(),line.strip(),last_in)





if __name__=='__main__':
    main()
