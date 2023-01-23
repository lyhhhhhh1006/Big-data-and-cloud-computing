#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Aug 28 23:26:09 2022

@author: yihuiliu_
"""

import sys
import string
import re
import os


if len(sys.argv) == 2:
    filename =  open(sys.argv[1],"r")

elif len(sys.argv) == 1:
    filename = sys.stdin

else:
    print("usage: ", sys.argv[0], "filename")
    #sys.exit(1)
    
words =[]
with filename as stdin:
    for line in stdin:
        wordslist = line.split() 
        for word in wordslist:
            word = re.sub(r'[^a-zA-Z0-9 \n]','', word)
            word = re.sub(r'0-9','',word)
            word = re.sub(r'[_]','', word)
            #remove words with digits in the beginning
            word = re.sub(r'(^|\W)\d+','',word)
            #lowercase all of the words to make them unique
            if word.lower() not in words:
                words.append(word.lower())
    words.sort()
    
    for ones in words:
        if len(ones)>0:
            print(ones)