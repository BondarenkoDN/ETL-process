import os
#print(os.listdir('.\data'))
#print([i for i in os.listdir('.\data') if i[:12]=='transactions'])
#if i[:11]=='transactions'
import sqlite3 
import pandas as pd
import sys
import ddl as ddl
#import os
conn=sqlite3.connect('sber.db')
cursor=conn.cursor()

# считываем из файлов данные по транзакциям и терминалам
def xsl2sql(filePath, tableName,conn):
    df=pd.read_excel(filePath)
    df.to_sql(tableName, con=conn, if_exists='replace', index=False)
    df.to_excel('backup_'+filePath[5:], sheet_name='sheet_name1', index=None)

# экспорт из базы в эксель
def sql2xsl(filePath,columns,tableName, conn):
    df=pd.read_sql(f'select {columns} from {tableName}',con=conn)
    df.to_excel(filePath,sheet_name='sheet_name1',index=None)

#for i in [j for j in os.listdir('.\data') if j[:12]=='transactions']:
   # print(f'data/{i}')
    #xsl2sql(f'data/{i}','table_trans',conn)
#print('data/passports_blacklist_{%s}.xlsx')
 #xsl2sql('data/passports_blacklist_.xlsx','black_list',conn)
 #print(sys.argv[1])
 print(sys.argv)