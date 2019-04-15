import datetime
import json
from datetime import timedelta
from datetime import datetime as dt
import time
import logging
import logging.handlers
import google.cloud.logging
import sys
import traceback
import arcpy
import os
from os.path import basename
from os.path import join
import multilog


arcpy.env.workspace = "in_memory"  
arcpy.env.overwriteOutput = True
arcpy.env.preserveGlobalIds = True


VERSION = '1.0.0'

LOCAL_LOGS = '../logs/enricher.log'

SD_LOG_NAME = 'python.scheduled'
LABELS = {'file': basename(__file__), 'version': VERSION}
SD_LOGGING_KEY = '../.secrets/python-logging.json'
CLOUD_LOGGING = True

class Config(object):
    """Configs for script inputs and outputs."""
    json_type_key = '__input_config__'

    def __init__(
        self,
        workspace, 
        collars_table, 
        date_field,   
        globalid_field,
        related_guid_field,
        output_table):
        
        self.workspace = workspace
        self.collars_table = collars_table
        self.date_field = date_field
        self.globalid_field = globalid_field
        self.related_guid_field = related_guid_field
        self.output_table = output_table
    
    @staticmethod
    def decode_config(dct):
        """Decode config from json."""
        if  Config.json_type_key in dct:
            configs = Config(
                dct['workspace'], 
                dct['collars_table'], 
                dct['date_field'], 
                dct['globalid_field'],   
                dct['related_guid_field'], 
                dct['output_table'])
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


class EnrichmentData(object):
    """Features used for point enrichment."""
    def __init__(self, path, field_mappings):
        self.path = path
        self.field_mappings = field_mappings
    
    @staticmethod
    def decode_enrichment(dct):
        """Decode enrichment data feature from json."""
        if 'path' in dct and 'field_mappings' in dct:
            return EnrichmentData(dct['path'], dct['field_mappings'])
        else:
            return dct
    
    @staticmethod
    def encode_enrichment(enrichment):
        """Encode enrichment data feature to json."""
        if isinstance(enrichment, EnrichmentData):
            field_dict = enrichment.__dict__
            return field_dict
        else:
            type_name = enrichment.__class__.__name__
            raise TypeError('Object of type {} is not JSON serializable'.format(type_name))


def get_querylayer_for_yesterday(workspace, table_name, guid_field, date_field, today=None):
    """Create a query layer that includes only data for the previous day."""
    if today is None:
        yesterday = dt.now() - timedelta(days=1)
    else:
        yesterday = today - timedelta(days=1)

    start_of_day = dt(yesterday.year, yesterday.month, yesterday.day)
    start_day_string = dt.strftime(start_of_day, "%Y-%m-%d %H:%M:%S")
    end_of_day = start_of_day + timedelta(days=1)
    end_day_string = dt.strftime(end_of_day, "%Y-%m-%d %H:%M:%S")
    logger.multi_log('Making query layer for {}. Date range: {} to {}'.format(
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
        fields_list=guid_field + ', SHAPE',
        table=table_name,
        field=date_field,
        start=start_day_string,
        end=end_day_string)
    
    ql_name = "date_query_result"
    ql_start_time = time.time()
    arcpy.MakeQueryLayer_management(
        workspace, ql_name, where_clause)
    ql_time = round(time.time() - ql_start_time, 4)
    logger.multi_log('Query layer creation time: {} seconds'.format(ql_time),
              'INFO',
              CLOUD_LOGGING,
              {'action': 'ql creation',
               'feature': table_name,
               'time': ql_time})
    
    return ql_name


def get_enriched_points(querylayer_points, enrichment_data, fields_to_keep):
    """Join points to enrichment feature and keep specified fields"""
    #defining features for the spatial join
    join_features = enrichment_data
    join_describe = arcpy.Describe(join_features)
    target_features = querylayer_points
    target_describe = arcpy.Describe(target_features)
    join_output = 'in_memory\\spatial_join'
    temp_out_name = join_output
    n = 1
    while arcpy.Exists(temp_out_name):
        temp_out_name = join_output + str(n)
        n += 1
    join_output = temp_out_name

    logger.multi_log(
        'Enriching {} with {}'.format(target_describe.name, join_describe.name),
        'INFO',
        CLOUD_LOGGING)
    logger.multi_log('Keep fields: {}'.format(','.join(fields_to_keep)), 'DEBUG')

    if join_describe.spatialReference.name != target_describe.spatialReference.name:
        logger.multi_log(
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
        logger.multi_log(
            'Keep fields not in either dataset: {}'.format(','.join(keep_fields - field_intersect)),
            'WARNING')

    for field in fieldmappings.fields:
        if field.name.lower() not in keep_fields:
            fieldmappings.removeFieldMap(
                fieldmappings.findFieldMapIndex(field.name))

    # Join datasets spatially
    join_start_time = time.time()
    arcpy.analysis.SpatialJoin(
        target_features, join_features, join_output,
        "JOIN_ONE_TO_ONE",
        "KEEP_ALL",
        fieldmappings)
    join_time = round(time.time() - join_start_time, 4)
    logger.multi_log(
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


def mutliple_enrichment(querylayer_points, globalid_field, enrichment_features):
    """Enrich points with fields from list of feature classes."""
    enriched = querylayer_points
    accumulated_fields = [globalid_field]
    for feature in enrichment_features:
        old_enriched = enriched
        accumulated_fields.extend([f[0] for f in feature.field_mappings])
        enriched = get_enriched_points(
            enriched,
            feature.path,
            accumulated_fields)
        arcpy.Delete_management(old_enriched)
    
    return enriched


def _get_config(config_location):
    """Get input and output configs from json."""
    with open(config_location, 'r') as json_file:
        configs = json.load(json_file, object_hook=Config.decode_config)

    return configs


def _get_enrichment_data(config_location):
    enrichment_data = []
    with open(config_location, 'r') as json_file:
        enrichment_data = json.load(json_file, object_hook=EnrichmentData.decode_enrichment)

    return enrichment_data


def _load_data(enriched_points, destination_table, enrichment_features, globalid_field, related_guid_field):
    logger.multi_log('Loading data into: ' + destination_table, 'INFO', CLOUD_LOGGING)
    source_fields = []
    destination_fields = []
    for feature in enrichment_features:
        for source, dest in feature.field_mappings:
            source_fields.append(source)
            destination_fields.append(dest)
    source_fields.append(globalid_field)
    destination_fields.append(related_guid_field)

    with arcpy.da.SearchCursor(enriched_points, source_fields) as enriched_cursor, \
        arcpy.da.InsertCursor(destination_table, destination_fields) as destination_cursor:
        for row in enriched_cursor:
            destination_cursor.insertRow(row)


if __name__ == '__main__':
    # Set directory to the directory of this file.
    dir_path = os.path.dirname(os.path.realpath(__file__))
    os.chdir(dir_path)
    
    input_config_location = '../configs/point_enrichment_input.json'
    enrichment_config_location = '../configs/point_enrichment_data.json'

    configs = _get_config(input_config_location)

    enrichment_features = _get_enrichment_data(enrichment_config_location)

    logger = multilog.MutliLogger('enricher', LOCAL_LOGS, SD_LOGGING_KEY, SD_LOG_NAME, LABELS)
    
    try:
        ql_name = get_querylayer_for_yesterday(
            configs.workspace,
            configs.collars_table,
            configs.globalid_field,
            configs.date_field)
        ql_count = arcpy.management.GetCount(ql_name)[0]
        logger.multi_log(
            'Query layer point count: {}'.format(ql_count),
            'INFO',
            CLOUD_LOGGING,
            {'action': 'ql count',
            'feature': configs.collars_table,
            'count': ql_count})
        
        # Query Layers don't spatially join correctly and won't copy to in_memory
        output_gdb = join(dir_path, '..', 'data', 'enrichment_output.gdb')
        if not arcpy.Exists(output_gdb):
            arcpy.management.CreateFileGDB(os.path.dirname(output_gdb), os.path.basename(output_gdb))
        
        logger.multi_log('Copying query layer points locally', 'INFO', CLOUD_LOGGING)
        points_feature = arcpy.management.CopyFeatures(ql_name, join(output_gdb, 'points'))[0]

        enriched = mutliple_enrichment(
            points_feature,
            configs.globalid_field,
            enrichment_features)

        _load_data(
            enriched,
            join(configs.workspace, configs.output_table),
            enrichment_features,
            configs.globalid_field,
            configs.related_guid_field)

    except Exception as e:
        logger.multi_log(
            e,
            'ERROR',
            CLOUD_LOGGING)
    finally:
        logging.shutdown()