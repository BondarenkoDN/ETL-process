# процесс формирования отчета
import sqlite3 
import pandas as pd
import sys
import ddl as ddl
import os
conn=sqlite3.connect('sber.db')
cursor=conn.cursor()


# экспорт из базы в эксель
def sql2xsl(filePath,columns,tableName, conn):
    df=pd.read_sql(f'select {columns} from {tableName}',con=conn)
    df.to_excel(filePath,sheet_name='sheet_name1',index=None)


# считываем из файлов данные по транзакциям и терминалам
def xsl2sql(filePath, tableName,conn):
    df=pd.read_excel(filePath)
    df.to_sql(tableName, con=conn, if_exists='replace', index=False)
    df.to_excel('backup_'+filePath[5:], sheet_name='sheet_name1', index=None)

xsl2sql(f'data/transactions_{sys.argv[:]}','table_trans',conn)
xsl2sql(f'data/passports_blacklist_{sys.argv[:]}.xlsx','black_list',conn)


#-----------------------------
#Заполняем таблицу clients
#-------------------------

def insertClients():
    cursor.execute('''
        --sql
        INSERT INTO clients(
            client_id,
            last_name,
            first_name,
            patrinimyc,
            date_of_birth,
            passport_num,
            passport_valis_to,
            phone
        )
        SELECT DISTINCT
            client_id,
            last_name,
            first_name,
            patrinimyc,
            date_of_birth,
            passport_num,
            passport_valis_to,
            phone
        FROM clientsNew
    ''')

    #формирование изменений в таблице при инкрементальной нагрузке
    cursor.execute('''
        UPDATE clients
        SET effective_to = datetime(current_timestamp,'-1 second')
        WHERE client_id in (
            SELECT 
                client_id
            FROM clientsUpdate)
        AND effective_to = datetime('2999-12-31 23:59:59');
    ''')
    cursor.execute('''
        --sql
        INSERT INTO clients(
            client_id,
            last_name,
            first_name,
            patrinimyc,
            date_of_birth,
            passport_num,
            passport_valis_to,
            phone
        )
        SELECT DISTINCT
            client_id,
            last_name,
            first_name,
            patrinimyc,
            date_of_birth,
            passport_num,
            passport_valis_to,
            phone
        FROM clientsUpdate
    ''')
    cursor.execute('''
        UPDATE clients
        SET effective_to = current_timestamp 
        AND deleted_flg = 1
        WHERE client_id in (
            SELECT 
                client_id
            FROM clientsDeleted)
        AND effective_to = datetime('2999-12-31 23:59:59');
    ''')
    
    conn.commit()

    #-------------------------
    #Заполнение таблицы account 
    #--------------------------

def insertAccounts():
    cursor.execute('''
    --sql
        INSERT INTO accounts(
            account_num,
            valid_to,
            client   
        )
        SELECT DISTINCT
            account_num,
            valid_to,
            client  
        FROM accountsNew
    ''')
    cursor.execute('''
        UPDATE accounts
        SET effective_to = datetime(current_timestamp,'-1 second')
        WHERE client_id in (
            SELECT 
                client
            FROM accountsUpdate)
        AND effective_to = datetime('2999-12-31 23:59:59');
    ''')
    cursor.execute('''
        --sql
        INSERT INTO accounts(
            account_num,
            valid_to,
            client   
        )
        SELECT DISTINCT
            account_num,
            valid_to,
            client  
        FROM accountsUpdate
    ''')
    cursor.execute('''
        UPDATE accounts
        SET effective_to = datetime(current_timestamp,'-1 second')
        AND deleted_flg=1
        WHERE client_id in (
            SELECT 
                client
            FROM accountsDeleted)
        AND effective_to = datetime('2999-12-31 23:59:59');
    ''')

    conn.commit()



    #-------------------------
    #Заполнение таблицы cards 
    #--------------------------

def insertCards():
    cursor.execute('''
    --sql
        INSERT INTO cards(
             card_num,
             account_num   
        )
        SELECT DISTINCT
             card_num,
             account_num  
        FROM cardsNew
    ''')
    cursor.execute('''
        UPDATE cards
        SET effective_to = datetime(current_timestamp,'-1 second')
        WHERE account_num in (
            SELECT 
                account_num
            FROM cardsUpdate)
        AND effective_to = datetime('2999-12-31 23:59:59');
    ''')
    cursor.execute('''
        --sql
        INSERT INTO cards(
             card_num,
             account_num    
        )
        SELECT DISTINCT
             card_num,
             account_num   
        FROM cardsUpdate
    ''')

    cursor.execute('''
        UPDATE cards
        SET effective_to = datetime(current_timestamp,'-1 second')
        AND deleted_flg=1
        WHERE account_num in (
            SELECT 
                account_num
            FROM cardsDeleted)
        AND effective_to = datetime('2999-12-31 23:59:59');
    ''')
    conn.commit()





    #-------------------------
    #Заполнение таблицы passport_blacklist 
    #--------------------------
def insertBlackList():
    cursor.execute('''
        --sql
        INSERT INTO passport_blacklist(
            passport_num ,
            entry_dt)
            SELECT
                t1.passport,
                t1.start_dt
            FROM black_list t1
            LEFT JOIN passport_blacklist t2
            ON t1.passport=t2.passport_num
            WHERE t2.passport_num is null    
    ''')

    conn.commit()

    #-------------------------
    #Заполнение таблицы report 
    #--------------------------

def insertReport():
    cursor.execute('''
        INSERT INTO report(
            event_dt ,
            passport ,
            fio ,
            phone ,
            event_type ,
            report_dt 
            )
            SELECT
                event_dt ,
                passport ,
                fio ,
                phone ,
                event_type ,
                report_dt 
            FROM fraud
            WHERE event_dt in(
                SELECT 
                    max(event_dt)
                FROM fraud
                GROUP BY passport_num)
    ''')
    conn.commit()

#-------------------------
#Добавление новых паспортов
#--------------------------

def insertBlackListNew():
    cursor.execute('''
        INSERT INTO passport_blacklist(
            passport_num ,
            entry_dt)
            SELECT
                t1.passport,
                t1.report_dt
            FROM report t1
            LEFT JOIN passport_blacklist t2
            ON t1.passport=t2.passport_num
            WHERE t2.passport_num is null 
    ''')
    conn.commit()

 
ddl.clearTmpTables()

ddl.CreateTableClients()
ddl.createTableClients00()
ddl.createTableClientsNew()
ddl.createTableClientsUpdate()
ddl.createTableClientsDeleted()
insertClients()

ddl.createTableAccounts()
ddl.createTableAccounts00()
ddl.createTableAccountsNew()
ddl.createTableAccountsUpdate()
ddl.createTableAccountsDeleted()
insertAccounts()

ddl.createTableCards()
ddl.createTableCards00()
ddl.createTableCardsNew()
ddl.createTableCardsUpdate()
ddl.createTableCardsDeleted()
insertCards()

ddl.createTableTransactions()
ddl.createTableTerminals()
ddl.createTableBlackList()
insertBlackList()

cursor.execute('drop table if exists fraud')
ddl.createTableReport()
ddl.createTableFraud()
insertReport()

insertBlackListNew()

sql2xsl('report.xlsx','*','report',conn)



