import pandas as pd
import seaborn as sn
import numpy as np
import unittest as ut
import unittest.mock as mockerR

x1= pd.DataFrame({ "hello":[1,2,3,4,5]})
x1.info()
np = np.array([[1,2,3,4,4],[2,3,4,5,6]])
print(np[1,1:4])
sn.histplot(data=x1,x='hello')
print(pd.__dict__)
