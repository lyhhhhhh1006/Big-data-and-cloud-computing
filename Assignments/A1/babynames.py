#!/usr/bin/python

import sys
import re

def extract_names(filename):
  """
  Given a file name for baby<year>.html,
  returns a list starting with the year string
  followed by the name-rank strings in alphabetical order.
  
  ['2006', 'Aaliyah 91', Aaron 57', 'Abagail 895', ' ...]

  If this seems too daunting, return

  ['2006', (male_name,rank), (female_name,rank), ....]

  The names and ranks are pairs rather than strings and they
  do not have to be sorted. For example the list might begin

  ['2006', ('Jacob','1'), ('Emma','1'), ...]
  
  """
  # +++your code here++
  names = []
  myfile = open(filename, 'r')
  str = myfile.read()

  year = re.search(r'Popularity in (\d+)', str)
  year = year.group(1)
  names.append(year)

  groups = re.findall(r'<td>(\d+)</td><td>(\w+)</td><td>(\w+)</td>', str)

#create a dictionary to store name and name's rank number
#name as the key and rank number as the value
  name_rank = {}
  for ranklist in groups:
      (rank, male_name, female_name) = ranklist
      if male_name not in name_rank:
          name_rank[male_name] = rank
      if female_name not in name_rank:
          name_rank[female_name] = rank
  sorted_names = sorted(name_rank.keys())
  
  for name in sorted_names:
      names.append(name + " " + name_rank[name])

  return names


def main():
  # This command-line parsing code is provided.
  # Make a list of command line arguments, omitting the [0] element
  # which is the script itself.

   if len(sys.argv) > 1:
       arg = sys.argv[1:]
   else:
       print("usage: ", sys.argv[0], "filename")
       sys.exit(1)

# remove the summary flag from sys.argv if it is present
   summary = False
   if arg[0] == 'usage: ':
       summary = True
       del arg[0]
       
  # +++your code here++
  # For each filename, get the names, then print the text output
  
   for filename in arg:
      names = extract_names(filename)
      text = '\n'.join(names) + '\n'
      
      if  summary:
          output = open(filename + '.summary', 'w')
          output.write(text)
          output.close()
          
      else:
          print (text)
      
#print('Yes, you are running the script correctly!')

if __name__ == '__main__':
  main()
