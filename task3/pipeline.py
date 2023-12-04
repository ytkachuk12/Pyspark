"""All pipeline steps is here"""

from task3.task_3 import DataFrames

# Create SparkSession obj
df = DataFrames()
df.session = "task3"

df.start()
df.save()
