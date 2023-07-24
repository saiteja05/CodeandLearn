from sklearn.datasets import fetch_california_housing
import pandas as pd
import seaborn as sn
# Load the boston dataset from sklearn.datasets
cali_data = fetch_california_housing()
# print(cali_data['feature_names'])
# Enter the boston data into a dataframe
df = pd.DataFrame(cali_data['data'], columns=cali_data['feature_names'])

# Print the first 5 rows to confirm ran correctly
print(df.head())
print(df.size)
df.info()
# sn.set(style='ticks',palette='Set1')
# sn.scatterplot(x='AveRooms',y='AveOccup',data=df)
# sn.barplot(x='AveRooms',y='AveOccup',data=df)
print(df.query('Population==322.0'))