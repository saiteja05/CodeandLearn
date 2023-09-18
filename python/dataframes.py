import pandas as pd
import spark as sp

def square(x):return x*x

print("working")
df=pd.read_csv('/Users/batman/Downloads/fd_data_engineer_test/data/housing.csv')
# to get schema details
print(df.info())
#to get mean ,avg,max,count for numeric rows
# print(df.describe())
#to select a column

# print(df['housing_median_age'].head())

#filtering and aggreagating

# print(df['households'][(df['housing_median_age'] < 20) & (df['total_bedrooms'] > 2)].count())


#group by - multi select single cloumn

# print(df.groupby(['total_bedrooms','total_rooms'],as_index=False)[['housing_median_age']].mean())

# visualizing groups
# df2 = df.groupby(['total_bedrooms'])
# print(df2.get_group(5));
# print(df2.groups[5])

# print(df.groupby(['housing_median_age','median_house_value'],as_index=False)
#       .agg({"total_rooms":["mean","max","min"],"total_bedrooms":["mean","max","min"]})
#       .head(10)
#       )
#
# # aliasing group by
#
# print(df.groupby(['housing_median_age','median_house_value'],as_index=False)
#       .agg(tr_mean=("total_rooms","mean"),tbr_mean=("total_bedrooms","mean"))
#       .sort_values(by="housing_median_age",ascending=False)
#       .head(10)
#       )

# Pivot
# data: The name of the DataFrame
# values: The column that contains the values to be aggregated in the pivot table
# index: The rows of the pivot table
# columns: The columns of the pivot table
# aggfunc: The function used to aggregate the values

print(pd.pivot_table(data=df , index=[] ,
                     values=['total_rooms','median_income'] ,
                     columns= ['total_bedrooms'] ,
                     aggfunc={'total_rooms':'mean','median_income':['max','min'] }

                     )
      )
#union

uniondf = pd.concat([df.head(10),df.tail(10)])


print(uniondf)
print(pd.concat([df.head(10),df.tail(10)],ignore_index=True))
pd.cut( df['latitude'],bins=3)

#join ( we can join and concat on axis as well , default is y axis (axis 0))

print(df.head(10).merge(df.tail(10), how= 'outer' ,on='latitude'))

#where
df["median_income"].where(
    df["total_bedrooms"] >2 ,
    other = df["median_income"] * 1.1 , inplace=True)


#window functiions

#rolling window

# let us take one column 20

target_col = df["median_income"].head(20);
print(target_col.info())
print(target_col.rolling(7).mean())
print(target_col)
print(target_col.rolling(1).apply(square))


# you can give rank target_col.rank()\
# lag using shift(1)
#lead using shift(-1)
# https://sparkbyexamples.com/pandas/pandas-window-functions/

# explading sum

print(target_col.expanding(3).sum())
