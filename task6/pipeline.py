"""All pipeline steps is here"""

from task6.task_6 import DataFrames

# Create SparkSession obj
df = DataFrames()
df.session = "task6"

df.start()
df.save()
