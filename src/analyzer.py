import multilog
import logging
import arcpy
import os

VERSION = '1.0.0'

LOCAL_LOG_NAME = 'analyzer'
LOCAL_LOGS = '../logs/{}.log'.format(LOCAL_LOG_NAME)

SD_LOG_NAME = 'python.scheduled'
LABELS = {'file': os.path.basename(__file__), 'version': VERSION}
SD_LOGGING_KEY = '../.secrets/python-logging.json'
CLOUD_LOGGING = True


if __name__ == "__main__":
    dir_path = os.path.dirname(os.path.realpath(__file__))
    os.chdir(dir_path)
    logger = multilog.MutliLogger(LOCAL_LOG_NAME, LOCAL_LOGS, SD_LOGGING_KEY, SD_LOG_NAME, LABELS)

    try:
        arcpy.env.workspace = 'C:\\ScheduledTasks\\dwr-tracking\\data\\Collar as CollarAdmin.sde'
        datasets = arcpy.ListFeatureClasses() + arcpy.ListTables()

        logger.multi_log('analyzing datasets', 'INFO', CLOUD_LOGGING)
        arcpy.management.AnalyzeDatasets(arcpy.env.workspace, in_datasets=datasets)
    
    except Exception as e:
        logger.multi_log(
            e,
            'ERROR',
            CLOUD_LOGGING)
    finally:
        logging.shutdown()