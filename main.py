import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, rand
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
import random
import datetime

CITIES = ['Москва', 'Санкт-Петербург', 'Новосибирск', 'Красноярск', 'Пермь']
LETTERS = 'abcdefghijklmnopqrstuvwxyz'
DOMAINS = ['ru', 'com']

def generate_name():
    return ''.join(random.choice(LETTERS) for _ in range(random.randint(5, 10)))

def generate_email(name):
    return f"{name}@{random.choice(DOMAINS)}.{random.choice(DOMAINS)}"

def generate_city():
    return random.choice(CITIES)

def generate_registration_date(age):
    current_year = datetime.datetime.now().year
    birth_year = current_year - age
    registration_year = birth_year + random.randint(18, age)
    return datetime.date(registration_year, random.randint(1, 12), random.randint(1, 28))

def generate_data(num_rows):
    return [
        (
            generate_name(),
            generate_email(name),
            generate_city(),
            random.randint(18, 95),
            random.randint(30000, 200000),
            generate_registration_date(age)
        )
        for _ in range(num_rows)
    ]

schema = StructType([
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("city", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("salary", IntegerType(), True),
    StructField("registration_date", DateType(), True)
])

if len(sys.argv) < 2:
    print("Usage: python main.py <num_rows>")
    sys.exit(1)

spark = SparkSession.builder.appName("Generate Synthetic Data").getOrCreate()

num_rows = int(sys.argv[1])

data = [
    (
        (name := generate_name()),
        generate_email(name),
        generate_city(),
        (age := random.randint(18, 95)),
        random.randint(30000, 200000),
        generate_registration_date(age)
    )
    for _ in range(num_rows)
]

df = spark.createDataFrame(data, schema)

df = df.repartition(20)

df.cache()

for col_name in df.columns:
    df = df.withColumn(col_name, when(rand() > 0.05, df[col_name]).otherwise(None))

output_file = f"{datetime.datetime.now().strftime('%Y-%m-%d')}-dev.csv"
df.write.csv(output_file, header=True, mode="overwrite")

spark.stop()
