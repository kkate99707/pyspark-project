from pyspark.sql import SparkSession
import os, sys
from pyspark.sql.types import StringType, DecimalType, DateType, IntegerType
from pyspark.sql.functions import when, col, rand, udf, monotonically_increasing_id, row_number
from decimal import Decimal
from random import randint, choice, uniform
from pyspark.sql.window import Window
from datetime import datetime, timedelta
import shutil
import glob
import string
import argparse

names = [
    'James Anderson', 'Michael Brown', 'Robert Johnson', 'David Williams', 'John Smith',
    'William Jones',
    'Richard Miller',
    'Joseph Davis',
    'Thomas Garcia',
    'Charles Wilson',
    'Emma Thompson',
    'Olivia Martinez',
    'Sophia Robinson',
    'Isabella Clark',
    'Mia Rodriguez',
    'Charlotte Lewis',
    'Amelia Walker',
    'Harper Hall',
    'Evelyn Young',
    'Abigail King'
]
cities = [
    'New York',
    'Los Angeles',
    'Chicago',
    'Houston',
    'Phoenix',
    'Philadelphia',
    'San Antonio',
    'San Diego',
    'Dallas',
    'Austin',
    'London',
    'Paris',
    'Berlin',
    'Madrid',
    'Rome',
    'Amsterdam',
    'Vienna',
    'Warsaw',
    'Prague',
    'Toronto'
]


def gen():
    v = ''
    nums = '1 2 3 4 5 6 7 8 9 0'.split()
    letters = list(string.ascii_letters)
    for j in range(randint(3, 7)):
        v += choice(letters + nums)
    return v


def generate():
    nums = '1 2 3 4 5 6 7 8 9 0'.split()
    letters = list(string.ascii_letters)
    punct = list('!@#$%^&*()?\'{ }[]:;".,/-_')
    s = {}
    s_s = set()
    for i in nums + letters + punct:
        g = gen()
        while g in s_s:
            g = gen()
        s[i] = g
        s_s.add(g)
    return s


def name() -> str:
    return choice(names)


def email(name) -> str:
    domen = ['gmail.com', 'mail.ru', 'yandex.by']
    return f'{name.lower().split()[0]}.{name.lower().split()[1]}{randint(1, 9999)}@{choice(domen)}'


def city() -> str:
    return choice(cities)


def age() -> int:
    return randint(18, 95)


def salary() -> Decimal:
    return Decimal(round(uniform(1000, 15000), 2))


def date_reg(age):
    end = datetime.now()
    d = age * 365
    return end - timedelta(randint(1, d))

def make_encrypt_udf(s):
    def encrypt(value):
        if value is None:
            return None
        value = str(value)
        return ''.join(s[i] for i in value)
    return udf(encrypt, StringType())


get_name = udf(name, StringType())
get_email = udf(email, StringType())
get_city = udf(city, StringType())
get_age = udf(age, IntegerType())
get_salary = udf(salary, DecimalType(10, 2))
get_date_registration = udf(date_reg, DateType())


# Настройка сессии Spark
def session():
    spark = SparkSession.builder \
        .appName("MyApp") \
        .master("spark://spark-master:7077") \
        .getOrCreate()
    return spark

# Генерация сегодняшнего отчета
def data_csv_today(spark, n, s):
    w = Window.orderBy(monotonically_increasing_id())
    df = spark.range(1, n + 1) \
        .withColumn('id', row_number().over(w)) \
        .withColumn('name', get_name()) \
        .withColumn('email', get_email(col('name'))) \
        .withColumn('city', get_city()) \
        .withColumn('age', get_age()) \
        .withColumn('salary', get_salary()) \
        .withColumn('registration_date', get_date_registration(col('age')))

    encrypt_udf = make_encrypt_udf(s)
    for c in df.columns:
        df = df.withColumn(c, encrypt_udf(col(c)))

    for c in df.columns:
        df = df.withColumn(c, when(rand() < 0.05, None).otherwise(col(c)))

    if not os.path.exists('data'):
        os.makedirs('data')

    df.coalesce(1).write.csv("data/output", header=True, mode='overwrite')

    name_file_old = glob.glob('data/output/*.csv')[0]
    name_file_new = f'data/{datetime.now().date()}-dev.csv'
    shutil.copy(name_file_old, name_file_new)

    shutil.rmtree('data/output')


# Генерация отчетов за 2 месяца
def data_csv_two_month(spark):
    t_m = timedelta(60)
    data = datetime.now() - t_m
    while data <= datetime.now():
        if data.day % 2 == 1:
            w = Window.orderBy(monotonically_increasing_id())
            df = spark.range(1, randint(100, 1000)) \
                .withColumn('id', row_number().over(w)) \
                .withColumn('name', get_name()) \
                .withColumn('email', get_email(col('name'))) \
                .withColumn('city', get_city()) \
                .withColumn('age', get_age()) \
                .withColumn('salary', get_salary()) \
                .withColumn('registration_date', get_date_registration(col('age')))

            columns_to_nullify = ['city', 'salary', 'email']
            for c in columns_to_nullify:
                df = df.withColumn(c, when(rand() < 0.05, None).otherwise(col(c)))

            if not os.path.exists('data'):
                os.makedirs('data')

            df.coalesce(1).write.csv("data/output", header=True, mode='overwrite')

            name_file_old = glob.glob('data/output/*.csv')[0]
            name_file_new = f'data/{data.date()}-dev.csv'
            shutil.copy(name_file_old, name_file_new)
        data += timedelta(1)

    shutil.rmtree('data/output')


def stop(spark):
    spark.stop()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--n', type=int, default=100)
    args = parser.parse_args()
    spark = session()
    s = generate()
    data_csv_today(spark, args.n, s)
    stop(spark)

main()