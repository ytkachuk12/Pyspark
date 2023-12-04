"""All pipeline steps is here"""

from task2.task_2 import DataFrames

# Create SparkSession obj
df = DataFrames()
df.session = "task2"

df.start()
df.save()
