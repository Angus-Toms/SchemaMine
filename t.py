# Read in datasets/flights.csv

# Cut the last 3 columns
# Save the result to a new file

import pandas as pd

# Conert nulls/blanks to 0
flights = pd.read_csv('datasets/flights.csv', na_values=['NA', ' '])
flights = flights.iloc[:, :-3]

# Save nulls as 0
flights.fillna(0, inplace=True)
flights.to_csv('datasets/flights_cut.csv', index=False)