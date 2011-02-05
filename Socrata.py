"""
Copyright (c) 2011 Socrata.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import json
import re
from poster.encode import multipart_encode
from poster.streaminghttp import register_openers
from httplib2 import Http
from urllib import urlencode
from urllib2 import Request, urlopen

class SocrataBase:
    """Base class for all Socrata API objects"""

    def __init__(self, configuration):
        """
        Initializes a new Socrata API object with configuration
        options specified in standard ConfigParser format
        """

        self.config = configuration
        self.username, password, host, api_host = (self.config.get('credentials', 'user'),
            self.config.get('credentials', 'password'),
            self.config.get('server', 'host'),
            self.config.get('server', 'api_host'))

        self.app_token  = self.config.get('credentials', 'app_token')
        self.api        = Http()
        self.url        = host
        self.id_pattern = re.compile('^[0-9a-z]{4}-[0-9a-z]{4}$')

        response, content = self.api.request('%s/authenticate' % api_host, 'POST',
            headers={'Content-type': 'application/x-www-form-urlencoded',
                     'X-App-Token': self.app_token},
            body=urlencode({'username': self.username, 'password': password}))
        cookies = re.search('(_blist_session_id=[^;]+)', response['set-cookie'])
        self.cookie = cookies.group(0)
        # For multipart upload/streaming
        register_openers()

    def _request(self, service, type, data = {}):
        """Generic HTTP request, encoding data as JSON and decoding the response"""
        response, content = self.api.request(
            self.url + service, type,
            headers = { 'Content-type:': 'application/json',
              'X-App-Token': self.app_token,
              'Cookie': self.cookie },
            body = json.dumps(data) )
        if content != None and len(content) > 0:
            response_parsed = json.loads(content)
            if hasattr(response_parsed, 'has_key') and \
                response_parsed.has_key('error') and response_parsed['error'] == True:
                print "Error: %s" % response_parsed['message']
            return response_parsed
        return None

    def _batch(self, data):
        payload = {'requests': data}
        return self._request('/batches', 'POST', payload)


class Dataset(SocrataBase):
    """Represents a Socrata Dataset, can be used for CRUD and more"""

    # Creates a new column, POSTing the request immediately
    def add_column(self, name, description='', type='text',
        hidden=False, rich=False, width=100):
        if not self.attached():
            return False
        data = { 'name': name, 'dataTypeName': type,
            'description': description, 'hidden': hidden,
            'width': width }
        if rich:
            data['format'] = {'formatting_option': 'Rich'}
        self.response = self._request("/views/%s/columns.json" % self.id,
            'POST', data)
        return self.response

    # Adds a new row by specifying an array of cells
    def add_row(self, data):
        if not self.attached():
            return False
        self.response = self._request("/views/%s/rows.json" % self.id,
            'POST', data)

    # For batch row importing, returns a dict to be POST'd
    # Takes same array of cells as add_row
    def add_row_delayed(self, data):
        if not self.attached():
            return False
        return {'url': "/views/%s/rows.json" % self.id,
                'requestType': 'POST',
                'body': json.dumps(data)}

    # Is this class currently associated with an existing dataset?
    def attached(self):
        return self.is_id(self.id)

    # Creates a new dataset by sending a POST request, saves four-four ID
    def create(self, name, description = '', tags = [], public = True):
        self.error = False
        data = { 'name': name, 'description': description }
        if public:
            data['flags'] = ['dataPublicRead']
        if tags.count > 0:
            data['tags'] = tags
        
        response = self._request('/views.json', 'POST', data)
        if response.has_key('error'):
            self.error = response['message']
            if response['code'] == 'authentication_required':
                raise RuntimeError('You must specify proper authentication credentials')
            elif response['code'] == 'invalid_request':
                raise DuplicateDatasetError(name)
            else:
                raise RuntimeError('API Error ' + response['message'])
        self.id = response['id']
        return True

    # Deletes the active dataset
    def delete(self):
        self.error = False
        response = self._request("/views.json?id=%s&method=delete" % self.id,
            'DELETE', None)
        return response == None

    # Call the search service with optional params
    def find_datasets(self, params={}):
        self.error = False
        sets = self._request("/api/search/views.json?%s" % urlencode(params), "GET")
        return sets

    def metadata(self):
        self.error = False
        self.response = self._request("/views/%s.json" % self.id, 'GET')
        return self.response['metadata']

    def attachments(self):
        metadata = self.metadata()
        if metadata == None or (not metadata.has_key('attachments')):
            return []
        return metadata['attachments']

    def attach_file(self, filename):
        metadata = self.metadata()
        if not metadata.has_key('attachments'):
            metadata['attachments'] = []

        response = self.multipart_post('/assets', filename)
        if not response.has_key('id'):
            print "Error uploading file to assets service: no ID returned: %s" % response
            return
        attachment = {'blobId': response['id'],
            'name': response['nameForOutput'],
            'filename': response['nameForOutput']}
        metadata['attachments'].append(attachment)
        self._request("/views/%s.json" % self.id, 'PUT', {'metadata':metadata})

    def multipart_post(self, url, filename, field='file'):
        datagen, headers       = multipart_encode({field: open(filename, "rb")})
        headers['Cookie']      = self.cookie
        headers['X-App-Token'] = self.app_token

        request = Request("%s%s" % (self.url, url), datagen, headers)
        response = urlopen(request).read()
        return json.loads(response)

    # Is the string 'id' a valid four-four ID?
    def is_id(self, id):
        return self.id_pattern.match(id) != None

    # Gets the most recent API error, if any
    def get_error(self):
        if self.error:
            return self.error
        return False

    def use_existing(self, id):
        if self.is_id(id):
            self.id = id
        else:
            return False

    def short_url(self):
        return self.config.get('server', 'host') + "/d/" + str(self.id)


class DuplicateDatasetError(Exception):
    """Raised if a dataset with the specified name already exists"""
    def __init__(self, name):
        self.name = name
    def __str__(self):
        return "Duplicate dataset with name '%s'" % self.name
