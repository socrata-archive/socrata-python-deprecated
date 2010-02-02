#!/usr/bin/python

import Socrata
import ConfigParser
import sys
import urllib
from xml.dom import minidom

REDDIT_RSS = "http://www.reddit.com/.rss"

def get_text(nodelist):
    """Retrieves a string from a text node, or series of text nodes"""
    rc = ""
    for node in nodelist:
        if node.nodeType == node.TEXT_NODE:
            rc = rc + node.data
    return rc

def get_element_text(tagName, item):
    return get_text(item.getElementsByTagName(tagName)[0].childNodes)

def get_rss_dom(url):
    """Gets a minidom object from Reddit's servers"""
    return minidom.parse(urllib.urlopen(url))

def create_dataset_with_columns(title = 'RSS Feed Dataset', description = ''):
    """Creates a new Socrata dataset with columns for an RSS feed"""

    cfg = ConfigParser.ConfigParser()
    cfg.read('socrata.cfg')
    dataset = Socrata.Dataset(cfg)
    try:
        dataset.create(title, description)
    except Socrata.DuplicateDatasetError:
        print "This dataset already exists."
        return False

    dataset.add_column('Title', '', 'text', False, False, 300)
    dataset.add_column('URL', '', 'url', False, False, 300)
    dataset.add_column('Date', '', 'date')
    
    return dataset

if __name__ == "__main__":
    # Default to Reddit for an example
    feed_url = REDDIT_RSS
    # Else take their feed from command line args
    if len(sys.argv) > 1:
        feed_url = sys.argv[1]
    print "Downloading RSS feed from " + str(feed_url)
    
    r_dom = get_rss_dom(feed_url)
    
    print "Creating dataset in Socrata"
    dataset = create_dataset_with_columns()

    if  dataset:
        batch_requests = []
        # Extract relevant information
        for item in r_dom.getElementsByTagName("item"):
            data = {}
            data['Title'] = get_element_text('title', item)
            data['URL']   = get_element_text('link', item)
            data['Date']  = get_element_text('dc:date', item)
            dataset.add_row(data)

        print "You can now view the dataset:"
        print dataset.short_url()
    else:
        print "There was an error creating your dataset."
    
    print "\nFinished"
