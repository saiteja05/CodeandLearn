import csv
import db
import re


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

    bound=len(datafile)
    # print('oldlen= ',bound)
    i=0
    while(i<bound):
        for key in datafile[i]:
            if(datafile[i][key]==" " or len(datafile[i][key])==0 or not(datafile[i][key] != '' ) or re.search(r'^.+$',datafile[i][key]) is None ):
                # print(datafile[i])
                datafile.pop(i)
                bound=bound-1
                break

        i=i+1
    # print('newlen= ', len(datafile))




def connect_db(dbname):
    connect = db.Dbconnector(dbname)
    cur = connect.getCursor()
    return connect,cur

def createtable(connect,cur,table_name,columns):
    ctbl = """create table if not exists {}({} int,{} int,{} int,{} int,{} int,{} double precision,{} int,{} varchar,{} int);""".format(
        table_name, *columns)
    print(ctbl)
    cur.execute(ctbl)
    connect.commit()

def writedata(connect,cur,datafile : list[dict]):
    try:
        for r in range(len(datafile)):
            runnable=cast(datafile[r])
            if(runnable):
                # print('we are good')
                isql = "insert into housingdata values{} ;".format(tuple(datafile[r].values()))
                # print(isql)
                cur.execute(isql)
            else:
                print('invalid data',datafile[r])
        # connect.commit()
    except Exception as exc:
        print('error :',exc)



def readdata(connect,cur,colname,bound):
    results=[]
    try:
        sql='select * from housingdata where {} >= 30000 and {} <= 35000 ;'.format(colname,colname)
        cur.execute(sql,bound)
        results=cur.fetchall()
        print(type(results))
        cols=[]
        for i in cur.description:
            cols.append(i.name)
        results.append(tuple(cols))
        print(results)
        del cols
    except Exception as exc:
        print('error :', exc)
    return results
def cast(row : dict):
    isfaulty = True
    for col in row:
        if(type(row[col]) is str):
            if(row[col] != '' ):
                # print('casting')
            # print(type(col))
            # print(col,row[col])
                if(re.search(r'^[<0-9A-Za-z ]+$',row[col])):
                # print(str)
                # print(row[col])
                    row[col]=row[col]
                    continue
                elif(re.search(r'^[-+]?\d*\.[1-9]\d+$',row[col])):
                # print(float)
                    row[col]=float(row[col])
                # print(row[col])
                    continue
                else:
                # print(int)
                    row[col]=int(float(row[col]))
            else:
                isfaulty=False


    return isfaulty


def readCsv(filepath : str):
    with open(file= filepath, mode='r') as datafile:
        csvfile = csv.DictReader(datafile)
        # print(type(csvfile))
        datalist = []
        for r in csvfile:
            datalist.append((r))
        return datalist



if __name__ == '__main__':

    path='/Users/batman/Downloads/fd_data_engineer_test/data/'
    datalist=readCsv(filepath=path+'housing.csv')

    print(len(datalist))
    removeEmptyRows(datalist)
    removeColulms(datalist,['longitude','latitude'])
    transform_median(datalist,2)
    createIndex(datalist)
    print(len(datalist))
    print(datalist[0])
    # print(tuple(datalist[0].keys()))
    # print(tuple(datalist[0].values()))
    connect,cur=connect_db(dbname='batman')
    createtable(connect,cur,"housingdata",tuple(datalist[0].keys()))
    writedata(connect,cur,datalist)
    results=readdata(connect,cur,colname='median_house_value',bound=[30000,35000])

    # with open(path+'output.csv', mode="w", newline="") as csvfile:
    #     csvwriter = csv.writer(csvfile)
    #     print(results)
    #     i=len(results)-1
    #     while(i>=0):
    #         csvwriter.writerows(results[i])
    #         i=i-1

    connect.closeAll()








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











