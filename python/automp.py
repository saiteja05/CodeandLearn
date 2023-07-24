import pandas as pd
# 1. mpg:           continuous
#     2. cylinders:     multi-valued discrete
#     3. displacement:  continuous
#     4. horsepower:    continuous
#     5. weight:        continuous
#     6. acceleration:  continuous
#     7. model year:    multi-valued discrete
#     8. origin:        multi-valued discrete
#     9. car name:      string (unique for each instance)


# df = pd.DataFrame({
#
# name':['Jane','John','Ashley','Mike','Emily','Jack','Catlin'],
# 'ctg':['A','A','C','B','B','C','B'],
# 'val':np.random.random(7).round(2),
# 'val2':np.random.randint(1,10, size=7)
#
# })
cols=["mpg","cylinders","displacement","horsepower" ,"weight","acceleration","model_year","origin","car_name"]
df=pd.read_csv(filepath_or_buffer='/Users/batman/Downloads/auto+mpg/auto-mpg.data',delim_whitespace=True,header=None,names=cols)
# print(df['horsepower'].unique())
df['horsepower']=df['horsepower'].replace('?','0')
df['horsepower'] = pd.to_numeric(df['horsepower'])
print(df.pivot_table(values='horsepower',columns=["model_year"],index="car_name",dropna=True, fill_value=0))

print(df.query('model_year == 75')[df.car_name.str.contains('ford')])

# print(df.shape)
# print(df.describe())
# print(df['mpg'].value_counts()," KB")
# x=df.info()
# print(x)
# print(df.head(255))

# print(len(df.columns))
# for i in df.columns:
    # print(i)
#
# print(df.groupby(["mpg","weight"]).aggregate({"model year":["min","max"],"horsepower":["mean"]}))

# print(df[['weight' , 'horsepower','acceleration','cylinders']][df.horsepower>80 ].corr())

# print(df['model year'][df.horsepower>80])