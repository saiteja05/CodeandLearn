# https://www.psycopg.org/docs/usage.html#passing-parameters-to-sql-queries
import psycopg2 as db

class Dbconnector:

    def __init__(self, duser):
        self.dbcon = db.connect("dbname=postgres user="+duser)
    def getCursor(self):
        self.cur = self.dbcon.cursor()
        return self.cur

    def commit(self):
        self.dbcon.commit()


    def closeAll(self):

        self.cur.close()
        self.dbcon.close()



# test

if __name__ == "__main__":
    connect = Dbconnector('batman')
    cur=connect.getCursor()
    cur.execute("select * from pg_user")
    print(cur.fetchall())
    connect.closeAll()
    del connect
    print(" %s "%('sai'))
#
# # cur.execute("CREATE TABLE test (id serial PRIMARY KEY, num integer, data varchar);")
# # cur.execute("INSERT INTO test (num, data) VALUES (%s, %s)",(100,"a,b")) #psycopg2 will handle this and avoid sql injection
#
