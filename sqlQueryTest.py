import pyspark
from pyspark.sql import SQLContext

# Split data into test and training (TODO speedup with indexing query instead)
def trainingSplit(trainingRatio, fullData):
    count = fullData.count()
    train_count = int(round((count * trainingRatio)))
    training_data = fullData.limit(train_count)
    test_data = fullData.subtract(training_data)
    return training_data, test_data

# Split data into k folds (TODO speedup with indexing query instead)
def foldsSplit(k, fullData):
    count = fullData.count()
    pivot = int(round((count / k)))  
    folds = []
    for i in range(k):
        if(i == 0):
            folds.append(fullData.limit(pivot))
        else:
            for j in range(len(folds)):
                temp = fullData.subtract(folds[j])
            folds.append(temp.limit(pivot))         
    return folds            
            
# Create spark context and sparkSQL objects
sc = pyspark.SparkContext.getOrCreate()
spark = SQLContext(sc)

# Use sparkSQL to read in CSV
df = (spark.read
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .load("data/elec_demand_hourly.csv"))

# Register temporary table including DateTime representation of columns
df.registerTempTable("temp")
df = spark.sql("SELECT *, CAST(CONCAT( Year, '-', Month, '-', Day, ' ', Hour ) as timestamp) as TimeAndDate from temp ORDER BY TimeAndDate ASC")
df.registerTempTable("temp")


sc.stop()
