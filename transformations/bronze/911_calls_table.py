from pyspark import pipelines as dp

@dp.expect("valid_incident_number", "incident_number IS NOT NULL")
@dp.expect("valid_longitude", "longitude IS NOT NULL AND longitude BETWEEN -180 AND 180")
@dp.expect("valid_latitude", "latitude IS NOT NULL AND latitude BETWEEN -90 AND 90")
@dp.table(name="seattle_bronze.seattle_fire_911_calls", comment="Bronze table for Seattle Fire 911 calls")
def bronze_911_calls():
    input_path = "/Volumes/seattle_data/seattle_landing/seattle_fire/911_calls/"
    return (
        spark.readStream
        .format("delta")
        .load(input_path)
    )