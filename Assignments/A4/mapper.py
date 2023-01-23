#!/usr/bin/env python
# coding: utf-8



import re
import sys
import time
import datetime



if __name__ == "__main__":
    for line in sys.stdin:
        line = line.rstrip()
        words = line.split('[')
        words = re.split(r' -0800| -0700',words[1])
        time = datetime.datetime.strptime(words[0], "%d/%b/%Y:%H:%M:%S").strftime("%Y-%m")
        sys.stdout.write("{}\t1\n".format(time))
        
