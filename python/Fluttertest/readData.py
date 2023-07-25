import csv


def transform_median(data,rounding_factor):
    for row in data:
        row['median_income']= round(float(row['median_income']),rounding_factor)
        # print(row['median_income'])

def  createIndex(data): #
    lc=0
    for row in data:
        row['index']=lc
        lc=lc+1

#O(n*M)
def removeColulms(datafile : list[dict], collist:list):
    for colname in collist:
        for d in range(len(datafile)):
           del datafile[d][colname]

def removeEmptyRows(datafile: list[dict]):
    for i in range(len(datafile)):
        for key in datafile[i]:
            if(datafile[i][key]==" "):
                datafile.pop(i)


with open(file ='/Users/batman/Downloads/fd_data_engineer_test/data/housing.csv',mode='r') as datafile:
    csvfile=csv.DictReader(datafile)
    print(type(csvfile))
    datalist = []
    for r in csvfile:
        datalist.append((r))
    removeEmptyRows(datalist)
    removeColulms(datalist,['longitude','latitude'])
    transform_median(datalist,2)
    createIndex(datalist)

    print(datalist[0])



#
# class HousingDashboards:
#     dataD
#     __init__
#
#     write
#
#
# class HousingTransformation
#
#     convert_dict()
#
#     transformation()
#     {
#
#     }
#
#
#     head()
#
# class DatabaseConnector
#
#     connector
#     table











