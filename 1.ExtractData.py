import requests
import json
import base64
import io
import pandas as pd
from xlrd import open_workbook
import os
import logging as log
from airflow.models import Variable

# airflow Variables
APP_ID = Variable.get('app_id')
CLIENT_SECRET = Variable.get('client_secret')
EMAIL = Variable.get('email')
RAW_DATA_LOCATION = Variable.get('raw_data_location')


# Get a token -- https://docs.microsoft.com/en-us/azure/active-directory/develop/v2-oauth2-auth-code-flow
url = 'https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token'
data = {
    'grant_type': 'client_credentials',
    'client_id': APP_ID,
    'scope': 'https://graph.microsoft.com/.default',
    'client_secret': CLIENT_SECRET
}

r = requests.post(url, data=data)
access_token = r.json().get('access_token')

#Call the graph end point and recieve response for messages in mailbox
graph_endpoint = 'https://graph.microsoft.com/v1.0{}'

request_url = graph_endpoint.format(
    '/users/' +
    EMAIL
    + '/mailFolders/{yourmailfolderid}/messages')

headers = {
    'User-Agent': 'PythonOutlook',
    'Authorization': 'Bearer {0}'.format(access_token),
    'Accept': 'application/json',
    'Content-Type': 'application/json'
}

response = requests.get(url=request_url, headers=headers)
response.json().keys()


# This function handles the paginated response recieved from the graph --https://journeyofthegeek.com/2019/01/08/using-python-to-pull-data-from-ms-graph-api-part-2/
def makeapirequest(endpoint, q_param=None):

    headers = {
        'User-Agent': 'PythonOutlook',
        'Authorization': 'Bearer {0}'.format(access_token),
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    }

    log.info('Making request to %s...', endpoint)

    if q_param != None:
        response = requests.get(endpoint, headers=headers, params=q_param)
#         print(response.url)
    else:
        response = requests.get(endpoint, headers=headers)
    if response.status_code == 200:
        json_data = json.loads(response.text)

        if '@odata.nextLink' in json_data.keys():
            log.info('Paged result returned...')
            record = makeapirequest(json_data['@odata.nextLink'], access_token)
            entries = len(record['value'])
            count = 0
            while count < entries:
                json_data['value'].append(record['value'][count])
                count += 1
        return(json_data)
    else:
        raise Exception('Request failed with ', response.status_code, ' - ',
                        response.text)


response_text = makeapirequest(request_url)

number_of_emails = len(response_text['value'])

email_ids = []
for i in range(0, number_of_emails, 1):
    email_ids.append(response_text['value'][i]['id'])

graph_endpoint = 'https://graph.microsoft.com/v1.0{}'

request_urls = []

for email_id in email_ids:
    request_urls.append(graph_endpoint.format(
        '/users/'
        + EMAIL +
        '/messages/'+email_id+'/attachments/'))

attach_jsons = []
for i in range(0, number_of_emails, 1):
    attach_jsons.append(requests.get(url=request_urls[i], headers=headers))

attach_ids = []
for i in range(0, len(attach_jsons), 1):
    attach_ids.append(attach_jsons[i].json()['value'][0]['id'])

content_dict = {}
for i in range(0, len(attach_jsons), 1):
    #Create a URL to ge the response for the attachment content
    attach_url = request_urls[i]+attach_ids[i]
    response = requests.get(url=attach_url, headers=headers)
    date = response.json()['lastModifiedDateTime'].split('T')[0]
    content_dict[date] = response.json()['contentBytes']

directory = RAW_DATA_LOCATION

filelist = [f for f in os.listdir(directory) if f.endswith(".xlsx")]

for f in filelist:
    os.remove(os.path.join(directory, f))

for key in content_dict:
    coded_string = content_dict[key]
    decoded = base64.b64decode(coded_string)
    type(decoded)  # => <class 'bytes'>
    xlsfile = open(directory+str(key)+'.xlsx', 'wb')
    xlsfile.write(decoded)
    xlsfile.close()
