import logging
import logging.handlers
import google.cloud.logging
import sys
import traceback
from datetime import timedelta
from datetime import datetime as dt
import os


class MutliLogger(object):
    """Logging with multiple loggers."""
    
    def __init__(self, log_name, log_file_directory, sd_logging_key, sd_log_name, labels):
        if not os.path.exists(os.path.dirname(log_file_directory)):
            os.mkdir(os.path.dirname(log_file_directory))

        logging_name = self._setup_logging(log_name, log_file_directory)
        self.log_name = logging_name
        self.log = logging.getLogger(self.log_name)
        cloud_logger, sd_log_url = self._setup_stackdriver(sd_logging_key, sd_log_name)
        self.cloud_logger = cloud_logger
        self.sd_log_url = sd_log_url
        self.labels = labels
    
    def multi_log(self, msg, severity, cloud_logging=False, cloud_struct=None, use_global_loggers=True, log_name=None, cloud_logger=None):
        """Log to the Python logger and StackDriver struct logger."""
        time_stamp = dt.utcnow()
        
        l = self.log
        log_methods = {
            'DEBUG': l.debug,
            'INFO': l.info,
            'WARNING': l.warn,
            'ERROR': l.error,
            'CRITICAL': l.critical,
        }

        severity = severity.upper()
        if severity not in log_methods:
            severity = 'INFO'
        log_exec = False
        if isinstance(msg, Exception):
            log_exec = True
        log_methods[severity](msg, exc_info=log_exec)
        
        sd_serverities = {
            'DEBUG': 'DEBUG',
            'INFO': 'INFO',
            'WARNING': 'WARNING',
            'ERROR': 'ERROR',
            'CRITICAL': 'CRITICAL',
        }
        if cloud_logging:
            cl = self.cloud_logger

            struct = {'message': str(msg)}
            if cloud_struct:
                struct = {**struct, **cloud_struct}
            
            cl.log_struct(
                struct,
                log_name=self.sd_log_url,
                timestamp=time_stamp,
                labels=self.labels,
                severity=sd_serverities[severity])


    def _setup_logging(self, log_name, log_file_directory):
        """Setup local logging."""
        log_name = 'enricher'
        log = logging.getLogger(log_name)
        log.setLevel(logging.DEBUG)
        log_formatter = logging.Formatter(fmt='%(levelname)s: %(message)s')
        log.logThreads = 0
        log.logProcesses = 0

        file_handler = logging.handlers.RotatingFileHandler(log_file_directory, backupCount=7)
        file_handler.doRollover()
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(log_formatter)

        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(log_formatter)
        
        log.addHandler(console_handler)
        log.addHandler(file_handler)

        return log_name


    def _setup_stackdriver(self, sd_logging_key, sd_log_name):
        """Setup logging to stackdriver."""
        client = google.cloud.logging.Client.from_service_account_json(sd_logging_key)
        cloud_logger = client.logger(sd_log_name)
        log_name = 'projects/{project}/logs/{sd_name}'.format(
            project=client.project,
            sd_name=sd_log_name)

        return cloud_logger, log_name