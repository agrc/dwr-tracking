from datetime import timedelta
from datetime import datetime
import arcpy


arcpy.env.workspace = "in_memory"  
arcpy.env.overwriteOutput = True
arcpy.env.preserveGlobalIds = True

start_date = "2002-02-01 12:00:00"
stop_date = "2002-03-01 12:00:00"
ending = "2004-06-01 12:00:00"

start = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")
stop = datetime.strptime(stop_date, "%Y-%m-%d %H:%M:%S")
end = datetime.strptime(ending, "%Y-%m-%d %H:%M:%S")


while start < end:
    print(start, stop)

    #where clause for the time range
    where_caluse = "select * from Collar.COLLARADMIN.Collars where DateYearAndJulian >=" + \
        "'{}'".format(start) + " AND " + \
        "DateYearAndJulian <=" + "'{}'".format(stop)

    #query layer created from the clause
    arcpy.MakeQueryLayer_management(
        r"enrichedPoints\collar.agrc.utah.gov.sde", "date_query_result", where_caluse)

    #defining features for the spatial join
    joinFeatures = r"H:\enrichedPoints.gdb\SGID10_Landownership"
    targetFeatures = r"date_query_result"

    #field map to determine which fields to keep
    fieldmappings = arcpy.FieldMappings()
    # Add all fields from inputs.
    fieldmappings.addTable(joinFeatures)
    fieldmappings.addTable(targetFeatures)

    fields_sequence = ["OWNER",
                       "ADMIN", "COUNTY", "GlobalID"]
    for field in fieldmappings.fields:
        if field.name not in fields_sequence:
            fieldmappings.removeFieldMap(
                fieldmappings.findFieldMapIndex(field.name))

    #joining the query layer with landownership and writing to in_memory
    arcpy.SpatialJoin_analysis(targetFeatures, joinFeatures, r"in_memory\spatial_join", "JOIN_ONE_TO_ONE", "KEEP_ALL", fieldmappings)

    #removing uneeded fields created from join
    arcpy.DeleteField_management( r"spatial_join", ["Join_Count", "TARGET_FID"])

    #appending the spatial join output to the master table of enriched points
    arcpy.Append_management(r"spatial_join", r"H:\enrichedPoints.gdb\enrichedPoints", "NO_TEST")


    arcpy.Delete_management(r"in_memory\spatial_join")

    #adding time to the start and stop date to pickup where it left off
    start = stop + timedelta(minutes=1)
    stop = stop + timedelta(days=30)


