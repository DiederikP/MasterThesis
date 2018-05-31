from pyspark.sql import SparkSession
from pyspark.sql import Row

# Initializing and assigning the correct data

spark = SparkSession \
   .builder \
   .appName("Diederik Test") \
   .master('spark://192.168.105.201:7077') \
   .config("testvar", "testvalue") \
   .getOrCreate()

df1 = spark.read.json("/data/8028_keys.json")
df2 = spark.read.json("/data/8028.json")

df1.createOrReplaceTempView("keys")
df2.createOrReplaceTempView("data")

# Test 1:
# alle data uit buurt 00030000 en scenario 'MM'
t1 = spark.sql("""
    SELECT d.value, d.valueDate, k.keyId 
    FROM data d 
    INNER JOIN keys k 
        ON d.valueKey = k.sourceValueKey 
    WHERE k.Buurt = '00030000' AND Scenario = 'MM'
    """
).collect()

# Test 2:
# per datum de woonvoorraad in buurt 00030000
t2 = spark.sql("""
    SELECT d.valueDate, SUM(d.value) 
    FROM data d 
    INNER JOIN keys k 
        ON d.valueKey = k.sourceValueKey 
    WHERE k.Buurt = '00030000' AND Scenario = 'MM'
    GROUP BY d.valueDate
    """
).collect()

# Test 3:
# per datum en scenario de woonvoorraad in gemeente
t3 = spark.sql("""
    SELECT d.valueDate, SUM(d.value) 
    FROM data d 
    INNER JOIN keys k 
        ON d.valueKey = k.sourceValueKey 
    WHERE k.Gemeente = '1904' AND Scenario = 'MM' AND k.Buurt IS NOT NULL
    GROUP BY d.valueDate
    """
).collect()

# Test 4:
# per datum de grootste woonvoorraad per buurt in de gemeente '1904'
t4 = spark.sql("""
    SELECT d.valueDate, MAX(d.value) 
    FROM data d 
    INNER JOIN keys k 
        ON d.valueKey = k.sourceValueKey 
    WHERE k.Gemeente = '1904' AND Scenario = 'MM' AND k.Buurt IS NOT NULL
    GROUP BY d.valueDate
    """
).collect()


# Test 5:
# t3 met een extra restrictie. 
t5 = spark.sql("""
    SELECT d.valueDate, SUM(d.value) 
    FROM data d 
    INNER JOIN keys k 
        ON d.valueKey = k.sourceValueKey 
    WHERE k.Gemeente = '1900' AND Scenario = 'HM' AND Type ='MGW' AND k.Buurt IS NOT NULL
    GROUP BY d.valueDate
    """
).collect()