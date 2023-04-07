# Importing pandas as pd
import pandas as pd

# Creating the first Dataframe using dictionary
df1 = df = pd.DataFrame({"a": [1, 2, 3, 4],
                         "b": [5, 6, 7, 8]})

# Creating the Second Dataframe using dictionary
df2 = pd.DataFrame({"a": [1, 2, 3],
                    "b": [5, 6, 7]})

# Print df1
print(df1, "\n")
print("---------------")
# to append df2 at the end of df1 dataframe
c = pd.concat([df1, df2], axis=0)
print(c)
