import json
import datetime
import os
import logging
import requests
import dateutil.parser
from pytz import utc
import multilog

VERSION = '1.0.0'

LOCAL_LOG_NAME = 'pelicans'
LOCAL_LOGS = '../logs/{}.log'.format(LOCAL_LOG_NAME)

SD_LOG_NAME = 'python.scheduled'
LABELS = {'file': os.path.basename(__file__), 'version': VERSION}
SD_LOGGING_KEY = '../../.keys/python-logging.json'
CLOUD_LOGGING = True

DEFAULT_BEGIN_DATE = datetime.datetime(2019,3,25).replace(tzinfo=utc)


class Config(object):
    """Configs for script inputs and outputs."""
    json_type_key = '__input_config__'

    def __init__(
        self,
        output_folder, 
        output_csv_prefix, 
        secrets_location):
        
        self.output_folder = output_folder
        self.output_csv_prefix = output_csv_prefix
        self.secrets_location = secrets_location
    
    @staticmethod
    def decode_config(dct):
        """Decode config from json."""
        if  Config.json_type_key in dct:
            configs = Config(
                dct['output_folder'],
                dct['output_csv_prefix'],
                dct['secrets_location'])
            return configs
        else:
            return dct
    
    @staticmethod
    def encode_config(config):
        """Encode config to json."""
        if isinstance(config, Config):
            field_dict = config.__dict__
            field_dict[Config.json_type_key] = ''
            return field_dict
        else:
            type_name = config.__class__.__name__
            raise TypeError('Object of type {} is not JSON serializable'.format(type_name))

class CttApiDownloadUtility:
    ApiEndpoint = 'https://account.celltracktech.com/api/v1/'

    def __init__(self, api_token):
        self.api_token = api_token
        self.units = self.getUnits()
        logger.multi_log(
            'there are {:,} units for this account'.format(len(self.units)),
            'INFO',
            CLOUD_LOGGING,
            {'action': 'unit count',
            'count': len(self.units)}
        )

    def _post(self, payload, parse_json_response=True):
        # post payload to the CTT Api Endpoint
        logger.multi_log(
            'posting payload: {:,} bytes'.format(len(payload)),
            'DEBUG')
        begin = datetime.datetime.now()
        response = requests.post(self.ApiEndpoint, data=payload)
        response.raise_for_status()
        if response.status_code == 200:
            request_duration = response.elapsed.total_seconds()
            charatcter_count = len(response.text)
            logger.multi_log(
                'received response: {:,} characters in {:,} seconds'.format(charatcter_count, round(request_duration,1)),
                'INFO',
                CLOUD_LOGGING,
                {'action': 'api response',
                 'characters': charatcter_count,
                 'time': request_duration})
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
        logger.multi_log(
            'building export request from {}'.format(begin),
            'DEBUG')
        export_meta = []
        for unit in self.units:
            if unit['lastData'] > begin:
                # this unit has data after the begin date - request data 
                logger.multi_log(
                    'adding unit {} to export list'.format(unit['unitId']),
                    'DEBUG')
                export_meta.append({
                    'unitId': unit['unitId'],
                    'startDt': begin.isoformat()
                })

        if len(export_meta) < 1:
            # if there are no units with new data - nothing to return
            logger.multi_log(
                'no data to export',
                'DEBUG')
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


def _get_config(config_location):
    """Get input and output configs from json."""
    with open(config_location, 'r') as json_file:
        configs = json.load(json_file, object_hook=Config.decode_config)

    return configs


def _get_secrets(secrets_location):
    """Get input and output secrets from json."""
    with open(secrets_location, 'r') as json_file:
        secrets= json.load(json_file, object_hook=Config.decode_config)

    return secrets


if __name__ == '__main__':
    import glob
    input_config_location = '../configs/pelican_input.json'

    try:
        global logger
        logger = multilog.MutliLogger(LOCAL_LOG_NAME, LOCAL_LOGS, SD_LOGGING_KEY, SD_LOG_NAME, LABELS)

        configs = _get_config(input_config_location)
        data_folder = configs.output_folder
        
        # get the CTT API Token from secrets
        secrets = _get_secrets(configs.secrets_location)
        token = secrets['api_key']
        logger.multi_log(
            'running pelican input',
            'INFO',
            CLOUD_LOGGING
        )
        api = CttApiDownloadUtility(api_token=token)

        # assume default begin date to start
        begin = DEFAULT_BEGIN_DATE
        logger.multi_log(
            'using data folder: {}'.format(data_folder),
            'DEBUG'
        )

        if os.path.exists(data_folder) is False:
            # directory does not exist - create it
            os.makedirs(data_folder)
        else:
            # get a list of previous data files
            downloaded_filenames = glob.glob(os.path.join(data_folder, 'export-*.csv'))
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
            outfilename = os.path.join(data_folder, filename)
            with open(outfilename, 'w', newline='\n') as outFile:
                outFile.write(csv_data)
    except Exception as e:
        logger.multi_log(
            e,
            'ERROR',
            CLOUD_LOGGING)
    finally:
        logging.shutdown()
