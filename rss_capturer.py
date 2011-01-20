#!/usr/bin/python

import Socrata
import ConfigParser
import sys
import urllib
import time
import feedparser
from xml.dom import minidom

REDDIT_RSS = "http://www.reddit.com/.rss"



def create_dataset_with_columns(dataset, title = 'RSS Feed Dataset', description = ''):
    """Creates a new Socrata dataset with columns for an RSS feed"""
    try:
        dataset.create(title, description)
    except Socrata.DuplicateDatasetError:
        print "This dataset already exists."
        return False

    dataset.add_column('Title', '', 'text', False, False, 300)
    dataset.add_column('URL', '', 'url', False, False, 300)
    dataset.add_column('Date', '', 'date')
    
    return

if __name__ == "__main__":
    # Default to Reddit for an example
    feed_url = REDDIT_RSS
    # Else take their feed from command line args
    if len(sys.argv) > 1:
        feed_url = sys.argv[1]
    print "Downloading RSS feed from " + str(feed_url)
    
    rss = feedparser.parse(feed_url)
    
    cfg = ConfigParser.ConfigParser()
    cfg.read('socrata.cfg')
    dataset = Socrata.Dataset(cfg)

    print "Searching for existing dataset"
    existing = dataset.find_datasets({'q':'RSS Feed Dataset',
        'for_user': dataset.username})[0]
    if existing['count'] > 0:
        print "Dataset exists, using it"
        dataset.use_existing(existing['results'][0]['id'])
    else:
        print "Creating dataset in Socrata"
        create_dataset_with_columns(dataset)

    if  dataset:
        batch_requests = []
        # Extract relevant information
        for item in rss.entries:
            data          = {}
            data['Title'] = item.title
            data['URL']   = item.link
            data['Date']  = time.strftime("%m/%d/%Y %H:%M:%S", item.date_parsed)
            batch_requests.append(dataset.add_row_delayed(data))


        dataset._batch(batch_requests)
        print "You can now view the dataset:"
        print dataset.short_url()
    else:
        print "There was an error creating your dataset."
    
    print "\nFinished"
