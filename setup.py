__author__="Aiden Scandella"
__date__ ="$Jan 11, 2010 5:06:00 PM$"

from setuptools import setup,find_packages

setup (
  name = 'Socrata-Python',
  version = '0.1',
  packages = find_packages(),

  # Declare your packages' dependencies here, for eg:
  # install_requires=['foo>=3'],

  # Fill in these to make your Egg ready for upload to
  # PyPI
  author = 'Aiden Scandella',
  author_email = 'aiden.scandella@socrata.com',

  summary = 'A python library for accessing the Socrata API',
  url = 'http://www.socrata.com',
  license = '',
  long_description= 'A native Python implementation of Socrata\'s REST API, using JSON',
  
)
