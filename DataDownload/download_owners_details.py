#!/Users/VK/anaconda/bin/python

import urllib.request,urllib.parse,urllib.error

fhand = urllib.request.urlopen('http://www.nasdaq.com/symbol/nflx/institutional-holdings')

for line in fhand:
        print (line.strip())
