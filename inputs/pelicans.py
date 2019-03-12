import json
import datetime
import os
import logging
import requests
import dateutil.parser
from pytz import utc

# save data files to this folder
DATA_FOLDER = 'data/ctt-data'
# if os.environ.get('CTT_DATA_FOLDER') is not None:
#     DATA_FOLDER = os.environ.get('CTT_DATA_FOLDER')

DEFAULT_BEGIN_DATE = datetime.datetime(2019,3,10).replace(tzinfo=utc)

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(message)s'
)

class CttApiDownloadUtility:
    ApiEndpoint = 'https://account.celltracktech.com/api/v1/'

    def __init__(self, api_token):
        self.api_token = api_token
        self.units = self.getUnits()
        logging.debug('there are {:,} units for this account'.format(len(self.units)))

    def _post(self, payload, parse_json_response=True):
        # post payload to the CTT Api Endpoint
        logging.debug('posting payload: {:,} bytes'.format(len(payload)))
        begin = datetime.datetime.now()
        response = requests.post(self.ApiEndpoint, data=payload)
        response.raise_for_status()
        if response.status_code == 200:
            request_duration = (datetime.datetime.now() - begin).total_seconds()
            logging.debug('received response: {:,} bytes in {:,} seconds'.format(len(response.text), round(request_duration,1)))
            if parse_json_response is True:
                return response.json()
            return response.text

    def getUnits(self):
        # return a list of units and their last connection dates
        request_data = {
            'token': self.api_token,
            'action': 'get-units'
        }
        unit_data = self._post(payload=json.dumps(request_data), parse_json_response=True)
        for unit in unit_data['units']:
            unit['lastData'] = dateutil.parser.parse(unit['lastData'])
            unit['lastConnection'] = dateutil.parser.parse(unit['lastConnection'])
        return unit_data['units']

    def export(self, begin):
        # export data from begin date
        logging.debug('building export request from {}'.format(begin))
        export_meta = []
        for unit in self.units:
            if unit['lastData'] > begin:
                # this unit has data after the begin date - request data 
                logging.debug('adding unit {} to export list'.format(unit['unitId']))
                export_meta.append({
                    'unitId': unit['unitId'],
                    'startDt': begin.isoformat()
                })

        if len(export_meta) < 1:
            # if there are no units with new data - nothing to return
            logging.debug('no data to export')
            return None

        # build the HTTP request payload data
        request_data = {
            'token': self.api_token,
            'action': 'data-export',
            'parameters': {
                'units': export_meta
            }
        }
        return self._post(payload=json.dumps(request_data), parse_json_response=False)

if __name__ == '__main__':
    import glob
    # get the CTT API Token from an environmental variable
    token = 'b7c93a11808bcf5fd3d64e7edb6c79c6c7aeb87a7491fd30d2306de27a28303d'#os.environ.get('CTT_API_TOKEN')
    # if token is None:
    #     raise Exception('CTT_API_TOKEN not set as an environmental variable')
    api = CttApiDownloadUtility(api_token=token)

    # assume default begin date to start
    begin = DEFAULT_BEGIN_DATE
    logging.debug('using data folder: {}'.format(DATA_FOLDER))
    if os.path.exists(DATA_FOLDER) is False:
        # directory does not exist - create it
        os.makedirs(DATA_FOLDER)
    else:
        # get a list of previous data files
        downloaded_filenames = glob.glob(os.path.join(DATA_FOLDER, 'export-*.csv'))
        if len(downloaded_filenames) > 0:
            # if we have at least 1 data file, get the latest, and parse the date
            latest_file = max(downloaded_filenames, key=os.path.getctime)
            directory_name, filename = os.path.split(latest_file)
            begin = datetime.datetime.strptime(filename, 'export-%Y-%m-%d_%H%M%S.csv').replace(tzinfo=utc)

    # begin the CSV export request
    csv_data = api.export(begin=begin)

    if csv_data is not None:
        # if there is new CSV data - save it to a file
        now = datetime.datetime.utcnow().replace(tzinfo=utc)
        filename = 'export-{}.csv'.format(now.strftime('%Y-%m-%d_%H%M%S'))
        outfilename = os.path.join(DATA_FOLDER, filename)
        with open(outfilename, 'w') as outFile:
            outFile.write(csv_data)