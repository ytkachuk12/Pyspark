"""All pipeline steps is here"""

from task5.task_5 import DataFrames

# Create SparkSession obj
df = DataFrames()
df.session = "task5"

df.start()
df.save()
