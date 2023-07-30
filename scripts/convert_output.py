#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# The purpose of the python script is to convert the output from average_time_between_pickups.txt into one table


# In[2]:


import pandas as pd
import re

# Step 1: Read the text from the file
with open("average_time_between_pickups.txt", "r") as file:
    text_data = file.read()

# Step 2: Parse the text and extract the zone names and average times
pattern = r"\│\s*(.*?)\s*\│\s*(.*?)\s*\│"
matches = re.findall(pattern, text_data, re.DOTALL)

# Step 3: Filter out rows that are not divisible by 3
data_list = [matches[i] for i in range(len(matches)) if i % 3 == 2]

# Step 4: Extract the zone names and average times from filtered rows
data_list = [{"Zone": match[0].strip(), "average_time_between_pickups": float(match[1])} for match in data_list]

# Step 5: Create a DataFrame from the extracted data
df = pd.DataFrame(data_list)

# Step 6: Display the DataFrame
df


# In[4]:


df.to_csv('average_time_between_pickups_py.txt', sep=',', index=True)

