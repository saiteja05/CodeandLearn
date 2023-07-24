import pandas as pd
import numpy as np
from faker import  Faker
# from faker_events import
names = ['age', 'workclass', 'fnlwgt', 'education', 'educationnum', 'maritalstatus', 'occupation', 'relationship', 'race',
        'sex', 'capitalgain', 'capitalloss', 'hoursperweek', 'nativecountry', 'label']
data = pd.read_csv('/Users/batman/Downloads/adult/adult.data',delimiter=',',header=None,names=names)
# print(data)
# fake=Faker()
# print(fake.name())
# fakerevents=fe()

# print(data['hoursperweek'])

bbox = data['hoursperweek'].plot(kind="box")
