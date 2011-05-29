__author__="Aiden Scandella"
__date__ ="$Jan 11, 2010 5:06:00 PM$"

from setuptools import setup,find_packages

setup (
  name = 'Socrata-Python',
  version = '0.2',
  packages = find_packages(),

  install_requires=['feedparser','httplib2','poster'],

  author = 'Aiden Scandella',
  author_email = 'aiden.scandella@socrata.com',

  url = 'http://www.socrata.com',
  license = '',
  long_description= 'A native Python implementation of Socrata\'s REST API, using JSON'
)
