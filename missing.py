#!/usr/bin/env python
# coding: utf-8

import re
import os
import argparse
import sys
import random


argulist = sys.argv
distinct_int = argulist[2].split()

# check if non-integer value is provided in the first argument
if(not argulist[1].isdigit()):
    print("The first argument is a non-integer value")

# check if part of second argument is non-integer
elif any((not i.isdigit()) 
         for i in distinct_int):
    print("The Second argument contains non-integer value")

#check if second argument not match with n-1
elif len(distinct_int)!=int(argulist[1])-1:
    print("The number of items provided in the second argument does not match N-1")

#check if second argument contains duplicates
elif len(distinct_int) != len(set(distinct_int)):
    print("There are duplicate values in the second argument")
    
else:
    n = int(argulist[1])
    distinct_int=list(map(int,distinct_int))
    print("The missing number is:",end='')
    
    for i in range(1,n+1):
        if i in distinct_int:
            continue
        else:
            print(i)
