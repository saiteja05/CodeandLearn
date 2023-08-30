import pandas.util.testing as tm; tm.N = 3
import numpy as np
import pandas as pd


# Create long dataframe
def unpivot(frame):
    print(frame.index,frame.columns)
    N, K = frame.shape
    print(np.tile(np.asarray(frame.index), K))
    print(np.asarray(frame.columns).repeat(N))
    data = {'value' : frame.values.ravel('F'),
            'variable' : np.asarray(frame.columns).repeat(N),
            'date' : np.tile(np.asarray(frame.index), K)}
    return pd.DataFrame(data, columns=['date', 'variable', 'value'])





df = pd.DataFrame(

    {
        "books":["lotr","HP","DND","GOT","lotr","HP","DND","GOT","lotr","HP","DND","GOT"],
         "years":[1998,1998,1998,1998,1995,1995,1995,1995,1991,1991,1991,1991],
        "author":["a","b","c","d","a","b","c","d","a","b","c","d"],
        "sales":[22,33,45,21,35,67,12,33,22,444,1,202],
        "price":[100,200,300,400,200,200,200,200,150,150,150,150]

    })




# Convert to wide format
df_pivot = df.pivot(index='date', columns='variable', values='value')

# Convert back to long format
print(df_pivot.unstack())




# print(df)
print(pd.pivot_table(df,values=['sales','price'],index=['years','author'],columns=['books'], aggfunc={'sales':'sum','price':'min'},fill_value=0,dropna=True).unstack())