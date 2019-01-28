#!/usr/bin/env python3

import cx_Oracle
import threading

class Oracle:

    def __init__(self,username,password,host,port,service):
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.service = service

    def connet(self):

        try:
            self.db = cx_Oracle.connect(self.username,self.password,self.host + ':' + self.port + '/' + self.service)

        except cx_Oracle.DatabaseError as E:
            print("Error in db connection: {}".format(E))

        self.cursor = self.db.cursor()

        return self.cursor

    def execute(self,sql,buildvars=None,dml=None,commit=None):

        try:
            self.cursor.execute(sql,buildvars)
            print(self.cursor.fetchall())

        except cx_Oracle.DatabaseError as E:
            print("Error in db connection: {}".format(E))
            if dml:
                self.db.rollback()

        if commit:
            self.db.commit()

    def disconnect(self):
        try:
            self.cursor.close()
            self.db.close()

        except cx_Oracle.DatabaseError as E:
            print("Error while disconnecting db {}".format(E))

def database1(lock,DBS,SQLS):
    D = DBS['DB1']
    sql = SQLS['DB1']

    lock.acquire()
    Db1obj = Oracle(D['user'], D['pwd'], D['host'], D['port'], D['service'])
    oracle = Db1obj.connet()

    try:
        buildvars = {}
        commit = False
        dml = False
        Db1obj.execute(sql, buildvars,dml,commit)
    finally:
        Db1obj.disconnect()
    lock.release()

def database2(lock,DBS,SQLS):
    D = DBS['DB2']
    sql = SQLS['DB2']

    lock.acquire()
    Db2obj = Oracle(D['user'], D['pwd'], D['host'], D['port'], D['service'])
    oracle = Db2obj.connet()

    try:
        buildvars = {}
        commit = False
        Db2obj.execute(sql, buildvars, commit)
    finally:
        Db2obj.disconnect()
    lock.release()

def main_task():

    lock = threading.Lock()

    DBS = {'DB1': {'user': "******", 'pwd': '*******', 'host': '*********', 'port': "*****",
                   'service': '****'},
           'DB2': {'user': "******", 'pwd': '******', 'host': '******', 'port': "****",
                   'service': '******'}}

    SQLS = {'DB1': "select * from Persons", 'DB2': "select * from KNTA_USERS"}

    T1 = threading.Thread(target=database1,args=(lock,DBS,SQLS))
    T2 = threading.Thread(target=database2,args=(lock,DBS,SQLS))

    T1.start()
    T2.start()

    T1.join()
    T2.join()

if __name__ == '__main__':

    main_task()
