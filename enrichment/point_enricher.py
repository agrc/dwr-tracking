import datetime
from datetime import timedelta
from datetime import datetime as dt
import time
import logging
import logging.handlers
import google.cloud.logging
import sys
import traceback
import arcpy
from os.path import basename
from os.path import join


arcpy.env.workspace = "in_memory"  
arcpy.env.overwriteOutput = True
arcpy.env.preserveGlobalIds = True


VERSION = '1.0.0'


SD_LOG_NAME = 'python.scheduled'
LABELS = {'file': basename(__file__), 'version': VERSION}
CLOUD_LOGGING = False

class EnrichmentData(object):

    def __init__(self, path, fields):
        self.path = path
        self.fields = fields



def get_querylayer_for_yesterday(workspace, table_name, keep_fields, date_field, today=None):
    """Create a query layer that includes only data for the previous day."""
    if today is None:
        yesterday = dt.now() - timedelta(days=1)
    else:
        yesterday = today - timedelta(days=1)

    start_of_day = dt(yesterday.year, yesterday.month, yesterday.day)
    start_day_string = dt.strftime(start_of_day, "%Y-%m-%d %H:%M:%S")
    end_of_day = start_of_day + timedelta(days=1)
    end_day_string = dt.strftime(end_of_day, "%Y-%m-%d %H:%M:%S")
    multi_log('Making query layer for {}. Date range: {} to {}'.format(
        table_name,
        start_day_string, 
        end_day_string),
        'INFO')
    #where clause for the time range
    where_clause = \
    """
    select {fields_list} from {table} 
    where 
    {field} >= '{start}'
    AND
    {field} < '{end}'
    """.format(
        fields_list=','.join(keep_fields) + ', SHAPE',
        table=table_name,
        field=date_field,
        start=start_day_string,
        end=end_day_string)
    
    ql_name = "date_query_result"
    ql_start_time = time.time()
    arcpy.MakeQueryLayer_management(
        workspace, ql_name, where_clause)
    ql_time = round(time.time() - ql_start_time, 4)
    multi_log('Query Layer creation time: {} seconds'.format(ql_time),
              'INFO',
              CLOUD_LOGGING,
              {'action': 'ql creation',
               'feature': table_name,
               'time': ql_time})
    
    return ql_name


def get_enriched_points(querylayer_points, enrichment_data, fields_to_keep):
    #defining features for the spatial join
    join_features = enrichment_data
    join_describe = arcpy.Describe(join_features)
    target_features = querylayer_points
    target_describe = arcpy.Describe(target_features)
    join_output = r"in_memory\spatial_join"
    temp_out_name = join_output
    n = 1
    while arcpy.Exists(temp_out_name):
        temp_out_name = join_output + str(n)
        n += 1
    join_output = temp_out_name


    multi_log(
        'Enriching {} with {}'.format(target_describe.name, join_describe.name),
        'INFO',
        CLOUD_LOGGING)
    multi_log('Keep fields: {}'.format(','.join(fields_to_keep)), 'DEBUG')

    if join_describe.spatialReference.name != target_describe.spatialReference.name:
        multi_log(
            'Spatial reference mismatch: join={}, target={}'.format(
                target_describe.spatialReference.name,
                join_describe.spatialReference.name),
            'WARNING',
            CLOUD_LOGGING,
            {'feature': basename(join_features)})

    #field map to determine which fields to keep
    fieldmappings = arcpy.FieldMappings()
    # Add all fields from inputs.
    fieldmappings.addTable(join_features)
    fieldmappings.addTable(target_features)

    keep_fields = set([f.lower() for f in fields_to_keep])
    # Check that keep fields are actually in these data
    mapped_field_names = set([f.name.lower() for f in fieldmappings.fields])
    field_intersect = keep_fields.intersection(mapped_field_names)
    if field_intersect != keep_fields:
        multi_log(
            'Keep fields not in either dataset: {}'.format(','.join(keep_fields - field_intersect)),
            'WARNING')

    for field in fieldmappings.fields:
        if field.name.lower() not in keep_fields:
            fieldmappings.removeFieldMap(
                fieldmappings.findFieldMapIndex(field.name))

    # Join datasets spatially
    join_start_time = time.time()
    arcpy.SpatialJoin_analysis(
        target_features, join_features, join_output,
        "JOIN_ONE_TO_ONE",
        "KEEP_ALL",
        fieldmappings)
    join_time = round(time.time() - join_start_time, 4)
    multi_log(
        'Join processing time: {} seconds'.format(join_time),
        'INFO',
        CLOUD_LOGGING,
        {'action': 'join',
         'feature': basename(join_features),
         'time': join_time,
         'message': 'Join processing'})

    #removing uneeded fields created from join
    arcpy.DeleteField_management(join_output, ["Join_Count", "TARGET_FID"])

    return join_output

def _report_field_name_duplication(paths, fields_to_keep):
    """Check for fields that exist in multiple features. Join process will only use first field."""
    field_names = []
    for feature in paths:
        field_names.extend([f.name.lower() for f in arcpy.ListFields(feature)])
    for field in fields_to_keep:
        f_count = field_names.count(field.lower())
        if f_count > 1:
            multi_log(
                'Field in multiple features: field={}, count={}'.format(field, f_count),
                'WARNING')
    

def mutliple_enrichment(querylayer_points, querylayer_fields, enrichment_features):
    """Enrich points with fields from list of feature classes."""
    paths = []
    fields_to_keep = []
    for feature in enrichment_features:
        paths.append(feature.path)
        fields_to_keep.extend(feature.fields)
    paths.append(querylayer_points)
    fields_to_keep.extend(querylayer_fields)
    _report_field_name_duplication(paths, fields_to_keep)
    
    enriched = querylayer_points
    accumulated_fields = list(querylayer_fields)
    for feature in enrichment_features:
        old_enriched = enriched
        accumulated_fields.extend(feature.fields)
        enriched = get_enriched_points(
            enriched,
            feature.path,
            accumulated_fields)
        arcpy.Delete_management(old_enriched)
    
    return enriched


def full_landowner_enrich():
    start_date = "2002-02-01 12:00:00"
    stop_date = "2002-03-01 12:00:00"
    ending = "2004-06-01 12:00:00"


    start = dt.strptime(start_date, "%Y-%m-%d %H:%M:%S")
    stop = dt.strptime(stop_date, "%Y-%m-%d %H:%M:%S")
    end = dt.strptime(ending, "%Y-%m-%d %H:%M:%S")
    
    while start < end:
        print('Start', start, 'End', stop)

        #where clause for the time range
        where_clause = "select * from Collar.COLLARADMIN.Collars where DateYearAndJulian >=" + \
            "'{}'".format(start) + " AND " + \
            "DateYearAndJulian <=" + "'{}'".format(stop)

        #query layer created from the clause
        arcpy.MakeQueryLayer_management(
            r"enrichedPoints\collar.agrc.utah.gov.sde", "date_query_result", where_clause)

        #defining features for the spatial join
        join_features = r"H:\enrichedPoints.gdb\SGID10_Landownership"
        target_features = r"date_query_result"

        #field map to determine which fields to keep
        fieldmappings = arcpy.FieldMappings()
        # Add all fields from inputs.
        fieldmappings.addTable(join_features)
        fieldmappings.addTable(target_features)

        fields_sequence = ["OWNER",
                        "ADMIN", "COUNTY", "GlobalID"]
        for field in fieldmappings.fields:
            if field.name not in fields_sequence:
                fieldmappings.removeFieldMap(
                    fieldmappings.findFieldMapIndex(field.name))

        #joining the query layer with landownership and writing to in_memory
        arcpy.SpatialJoin_analysis(target_features, join_features, r"in_memory\spatial_join", "JOIN_ONE_TO_ONE", "KEEP_ALL", fieldmappings)

        #removing uneeded fields created from join
        arcpy.DeleteField_management( r"spatial_join", ["Join_Count", "TARGET_FID"])

        #appending the spatial join output to the master table of enriched points
        arcpy.Append_management(r"spatial_join", r"H:\enrichedPoints.gdb\enrichedPoints", "NO_TEST")


        arcpy.Delete_management(r"in_memory\spatial_join")

        #adding time to the start and stop date to pickup where it left off
        start = stop + timedelta(minutes=1)
        stop = stop + timedelta(days=30)

def multi_log(msg, severity, cloud_logging=False, cloud_struct=None, use_global_loggers=True, log_name=None, cloud_logger=None):
    """Log to the Python logger and StackDriver struct logger."""
    time_stamp = dt.utcnow()
    
    l = None
    if use_global_loggers:
        l = log
    else:
        l = logging.getLogger('log_name')
   
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
        cl = None
        if use_global_loggers:
            cl = cloud_log
        else:
            cl = cloud_logger

        struct = {'message': str(msg)}
        if cloud_struct:
            struct = {**struct, **cloud_struct}
        
        cl.log_struct(
            struct,
            log_name=SD_LOG,
            timestamp=time_stamp,
            labels=LABELS,
            severity=sd_serverities[severity])


def _setup_logging():
    log_name = 'enricher'
    log = logging.getLogger(log_name)
    log.setLevel(logging.DEBUG)
    log_formatter = logging.Formatter(fmt='%(levelname)s: %(message)s')
    log.logThreads = 0
    log.logProcesses = 0

    file_handler = logging.handlers.RotatingFileHandler('logs/enricher.log', backupCount=7)
    file_handler.doRollover()
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(log_formatter)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(log_formatter)
    
    log.addHandler(console_handler)
    log.addHandler(file_handler)

    return log_name


def _setup_stackdriver():
    client = google.cloud.logging.Client.from_service_account_json('../.keys/python-logging.json')
    cloud_logger = client.logger(SD_LOG_NAME)
    log_name = 'projects/{project}/logs/{sd_name}'.format(
        project=client.project,
        sd_name=SD_LOG_NAME)

    return cloud_logger, log_name


if __name__ == '__main__':
    workspace = r'C:\Users\kwalker\AppData\Roaming\ESRI\Desktop10.4\ArcCatalog\CollarTest as CollarAdmin.sde'
    collars_table = 'CollarTest.COLLARADMIN.Collars'
    collars_keep_fields = ['CollarSerialNum']
    date_field = 'DateYearAndJulian'
    
    output_gdb = r'C:\giswork\temp\geotab_sample.gdb'
    output_feature = r'enriched_points'

    enrichment_features = [
        EnrichmentData(
            r'C:\Users\kwalker\AppData\Roaming\ESRI\Desktop10.4\ArcCatalog\Connection to sgid.agrc.utah.gov.sde\SGID10.CADASTRE.LandOwnership',
            ['AGENCY', 'OWNER']),
        EnrichmentData(
            r'C:\giswork\temp\testdata.gdb\landerownership_local',
            ['AGENCY_L'])
    ]

    log_name = _setup_logging()
    global log
    log = logging.getLogger(log_name)

    global cloud_log
    global SD_LOG
    cloud_log, SD_LOG = _setup_stackdriver()
    try:
        ql_name = get_querylayer_for_yesterday(
            workspace,
            collars_table,
            collars_keep_fields,
            date_field,
            dt.strptime('2017-07-18', "%Y-%m-%d"))
        ql_count = arcpy.management.GetCount(ql_name)[0]
        multi_log(
            'Query layer point count: {}'.format(ql_count),
            'INFO',
            CLOUD_LOGGING,
            {'action': 'ql count',
            'feature': collars_table,
            'count': ql_count})
        
        # Query Layers don't spatially join correctly and won't copy to in_memory
        points_feature = arcpy.management.CopyFeatures(ql_name, join(output_gdb, output_feature))[0]
        enriched = mutliple_enrichment(
            points_feature,
            collars_keep_fields,
            enrichment_features)
        
        arcpy.management.CopyFeatures(enriched, join(output_gdb, output_feature))
        arcpy.Delete_management(enriched)
        1/0
    except Exception as e:
        multi_log(
            e,
            'ERROR',
            CLOUD_LOGGING)
    finally:
        logging.shutdown()