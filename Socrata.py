import json
import re
from httplib2 import Http

class Socrata:
    """Base class for all Socrata API objects"""
    
    def __init__(self, configuration):
        """
        Initializes a new Socrata API object with configuration 
        options specified in standard ConfigParser format
        """
        
        self.config = configuration
        self.api = Http()
        self.api.add_credentials(
            self.config.get('credentials', 'user'),
            self.config.get('credentials', 'password'))
        self.url = self.config.get('server', 'host')
        self.id_pattern = re.compile('^[0-9a-z]{4}-[0-9a-z]{4}$')

    def _request(self, service, type, data = {}):
        """Generic HTTP request, encoding data as JSON and decoding the response"""
        response, content = self.api.request(
            self.url + service, type,
            headers = { 'Content-type:': 'application/json' },
            body = json.dumps(data) )
        if content != None and len(content) > 0:
            return json.loads(content)
        return None

    def _batch(self, data):
        payload = json.dumps({'requests': data})
        return self._request('/batches', 'POST', payload)


class Dataset(Socrata):
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
            data['flags'] = ['dataPublic']
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

    # Is the string 'id' a valid four-four ID?
    def is_id(self, id):
        return self.id_pattern.match(id) != None

    # Gets the most recent API error, if any
    def get_error(self):
        if self.error:
            return self.error
        return False

    def short_url(self):
        return self.config.get('server', 'public_host') + "/d/" + str(self.id)


class DuplicateDatasetError(Exception):
    """Raised if a dataset with the specified name already exists"""
    def __init__(self, name):
        self.name = name
    def __str__(self):
        return "Duplicate dataset with name '%s'" % self.name