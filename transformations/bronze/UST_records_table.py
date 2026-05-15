from pyspark import pipelines as dp

@dp.expect("valid_incident_number", "incident_number IS NOT NULL")
@dp.expect("valid_longitude", "longitude IS NOT NULL AND longitude BETWEEN -180 AND 180")
@dp.expect("valid_latitude", "latitude IS NOT NULL AND latitude BETWEEN -90 AND 90")
@dp.primary_key("incident_number")
@dp.table(name="seattle_bronze.seattle_ust_records", comment="Bronze table for Seattle UST records")
def bronze_UST_records():
    input_path = "/Volumes/seattle_data/seattle_landing/seattle_ust_records/"
    return (
        spark.readStream
        .format("delta")
        .load(input_path)
    )