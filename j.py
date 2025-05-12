schema = types.StructType([
    types.StructField('Rental Id', types.IntegerType(), True), 
    types.StructField('Duration', types.IntegerType(), True), 
    types.StructField('Bike Id', types.IntegerType(), True), 
    types.StructField('End Date', types.TimestampType(), True), 
    types.StructField('EndStation Id', types.IntegerType(), True), 
    types.StructField('EndStation Name', types.StringType(), True), 
    types.StructField('Start Date', types.TimestampType(), True), 
    types.StructField('StartStation Id', types.IntegerType(), True), 
    types.StructField('StartStation Name', types.StringType(), True)
])
