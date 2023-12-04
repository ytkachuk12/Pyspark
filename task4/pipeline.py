"""All pipeline steps is here"""

from task4.task_4 import DataFrames

# Create SparkSession obj
df = DataFrames()
df.session = "task4"

df.start()
df.save()
