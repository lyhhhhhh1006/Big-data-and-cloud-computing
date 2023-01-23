#!/usr/bin/env python


import sys


if __name__ == '__main__':
	
	curkey = None
	cursum = 0
	
	for line in sys.stdin:
		key, val = line.split("\t")
		val = int(val)

		if key == curkey:
			cursum += val
			
		else:
			if curkey is not None:
				sys.stdout.write("{}\t{}\n".format(curkey, cursum))

			curkey = key
			cursum = val

	sys.stdout.write("{}\t{}\n".format(curkey, cursum))
	
