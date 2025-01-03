#загрузка данных из файлов в базу
import sqlite3

conn=sqlite3.connect('bank.db')
cursor=conn.cursor()


#-------------------------------------
# Создаем таблицы по заданию, добавляя в таблицы
# clients,accounts,cards поля времени с какого по какой
# добавляем флаг об удалении
# это делается чтобы далее можно было создать из этих таблиц побочные, 
# содержащие новые, удаленные и измененные записи
# формируем таблицы с актуальным срезом
#-------------------------------------

def CreateTableClients():
    cursor.execute('''
        --sql
         CREATE TABLE if NOT EXISTS clients(          
            client_id varchar(128),
            last_name varchar(128),
            first_name varchar(128),
            patrinimyc varchar(128),
            date_of_birth datetime,
            passport_num varchar(128),
            passport_valid_to datetime,
            phone varchar(128),
            effective_from datetime default current_timestamp,
            effective_to datetime default (datetime('2999-12-31 23:59:59')),
            deleted_flg char(1) default 0
            )
    ''')
    cursor.execute('''
        CREATE VIEW IF NOT EXISTS v_clients as
            SELECT DISTINCT
                client_id,
                last_name,
                first_name,
                patrinimyc,
                date_of_birth,
                passport_num,
                passport_valis_to,
                phone
            FROM clients
            WHERE current_timestamp between effective_from and effective_to
    ''')

#формируем таблицу accounts  и актуальное на данный момент отображение
def CreateTableaccounts():
    cursor.execute('''
         CREATE TABLE if NOT EXISTS accounts(          
            account_num varchar(128),
            valid_to datetime,
            client varchar(128),
            effective_from datetime default current_timestamp,
            effective_to datetime default (datetime('2999-12-31 23:59:59')),
            deleted_flg char(1) default 0
            )
    ''')
    cursor.execute('''
        CREATE VIEW IF NOT EXISTS v_accounts as
            SELECT DISTINCT
            account_num,
            valid_to,
            client    
            FROM accounts
            WHERE current_timestamp between effective_from and effective_to
    ''')

#формируем таблицу cards  и  актуальное на данный момент отображение
def CreateTableCards():
    cursor.execute('''
         CREATE TABLE if NOT EXISTS cards(          
            card_num varchar(128),
            account_num varchar(128),
            effective_from datetime default current_timestamp,
            effective_to datetime default (datetime('2999-12-31 23:59:59')),
            deleted_flg char(1) default 0
            )
    ''')
    cursor.execute('''
        CREATE VIEW IF NOT EXISTS v_cards as
            SELECT DISTINCT
                card_num,
                account_num
            FROM cards
            WHERE current_timestamp between effective_from and effective_to
    ''')

#------------------------------------------------------------
# формируем таблицы transaction и  terminals из выгрузки excel
#------------------------------------------------------------

def CreateTableTransactions():
    cursor.execute('''
         CREATE TABLE if NOT EXISTS transactions as          
            SELECT
                trans_id ,
                trans_date ,
                card_num ,
                oper_type ,
                amt,
                oper_result ,
                terminal 
            FROM table_trans
    ''')

def CreateTableTerminals():
    cursor.execute('''
         CREATE TABLE if NOT EXISTS terminals as
            SELECT        
                ternimal_id,
                terminal_type ,
                terminal_cite ,
                terminal_address
            FROM table_trans
        
    ''')

def CreateTablePassport_blacklist():
    cursor.execute('''
         CREATE TABLE if NOT EXISTS passport_blacklist (          
                passport_num ,
                entry_dt
         )
    ''')



#-----------------------------------------------------
# реализуем икрементальную загрузку. 
# создаем временные таблицы для clients,accounts,cards
#----------------------------------------------------

# создаем временные таблицы для clients
def createTableClients00():
    cursor.execute('''
        CREATE TABLE clients_00 as
            SELECT DISTINCT
                client as client_id ,
                last_name ,
                first_name ,
                patrinimyc ,
                date_of_birth,
                passport as passport_num,
                passport_valid_to,
                phone
            FROM table_trans
    ''')
def createTableClientsNew():
     cursor.execute('''
        CREATE TABLE clientsNew as
            SELECT
                t1.*
            FROM clients_00 t1
            LEFT JOIN v_clients t2
            ON t1.client_id=t2.client_id
            WHERE t2.client_id is null
     ''')
def createTableClientsUpdate():
    cursor.execute('''
        CREATE TABLE clientsUpdate as
            SELECT 
                t1.* 
            FROM  clients_00 t1
            INNER JOIN v_clients t2
            ON t1.client_id=t2.client_id
            AND ( t1.last_name            <>        t2.last_name  
                OR t1.first_name          <>        t2.first_name    
                OR t1.patrinimyc          <>        t2.patrinimyc    
                OR t1.date_of_birth       <>        t2.date_of_birth  
                OR t1.passport_num        <>        t2.passport_num  
                OR t1.passport_valid_to   <>        t2.passport_valid_to  
                OR t1.phone               <>        t2.phone  
            )
    ''')
def createTableClientsDeleted():
     cursor.execute('''
        CREATE TABLE clientsDeleted as
            SELECT
                t1.*
            FROM v_clients t1
            LEFT JOIN clients_00 t2
            ON t1.client_id=t2.client_id
            WHERE t2.client_id is null
     ''')


#------------------------------------------
# создаем временные таблицы для accounts
#-----------------------------------------
def createTableAccounts00():
    cursor.execute('''
        CREATE TABLE accounts_00 as
            SELECT DISTINCT
                account as account_num, 
                account_valid as valid_to, 
                client
            FROM table_trans
    ''')
def createTableAccountsNew():
     cursor.execute('''
        CREATE TABLE accountsNew as
            SELECT
                t1.*
            FROM accounts_00 t1
            LEFT JOIN v_accounts t2
            ON t1.client=t2.client
            WHERE t2.client is null
     ''')
def createTableAccountsUpdate():
    cursor.execute('''
        CREATE TABLE accountsUpdate as
            SELECT 
                t1.* 
            FROM  accounts_00 t1
            INNER JOIN v_accounts t2
            ON t1.client=t2.client
            AND ( t1.account_num    <>        t2.account_num  
                OR t1.valid_to      <>        t2.valid_to    
            )
    ''')
def createTableAccountsDeleted():
     cursor.execute('''
        CREATE TABLE accountsDeleted as
            SELECT
                t1.*
            FROM v_accounts t1
            LEFT JOIN accounts_00 t2
            ON t1.client=t2.client
            WHERE t2.client is null
     ''')

#------------------------------------------
# создаем временные таблицы для cards
#-----------------------------------------
def createTableCards00():
    cursor.execute('''
        CREATE TABLE cards_00 as
            SELECT DISTINCT
                card as card_num, 
                account as account_num
            FROM table_trans
    ''')
def createTableCardsNew():
     cursor.execute('''
        CREATE TABLE cardsNew as
            SELECT
                t1.*
            FROM cards_00 t1
            LEFT JOIN v_cards t2
            ON t1.account_num=t2.account_num
            WHERE t2.account_num is null
     ''')
def createTableCardsUpdate():
    cursor.execute('''
        CREATE TABLE cardsUpdate as
            SELECT 
                t1.* 
            FROM  cards_00 t1
            INNER JOIN v_cards t2
            ON t1.account_num=t2.account_num
            AND t1.card_num <> t2.card_num  # вот тут было написано account_num
    ''')
def createTableCardsDeleted():
     cursor.execute('''
        CREATE TABLE cardsDeleted as
            SELECT
                t1.*
            FROM v_cards t1
            LEFT JOIN cards_00 t2
            ON t1.account_num=t2.account_num
            WHERE t2.account_num is null
     ''')


#-----------------------------------------
#функция удаления временных таблиц
#-----------------------------------

def clearTmpTables():
    cursor.execute('DROP TABLE IF EXISTS accounts_00')
    cursor.execute('DROP TABLE IF EXISTS accountsNew')
    cursor.execute('DROP TABLE IF EXISTS accountsUpdate')
    cursor.execute('DROP TABLE IF EXISTS accountsDeleted')

    cursor.execute('DROP TABLE IF EXISTS clients_00')
    cursor.execute('DROP TABLE IF EXISTS clientsNew')
    cursor.execute('DROP TABLE IF EXISTS clientsUpdate')
    cursor.execute('DROP TABLE IF EXISTS clientsDeleted')

    cursor.execute('DROP TABLE IF EXISTS cards_00')
    cursor.execute('DROP TABLE IF EXISTS cardsNew')
    cursor.execute('DROP TABLE IF EXISTS cardsUpdate')
    cursor.execute('DROP TABLE IF EXISTS cardsDeleted')

#---------------------------------------------------
#Создание таблицы Report
#---------------------------------------------------
def createTableReport():
    cursor.execute('''
       CREATE TABLE IF NOT EXISTS report(
            event_dt datetime,
            passport varchar(128),
            fio varchar(128),
            phone varchar(128),
            event_type varchar(128),
            report_dt datetime
       ) 
    ''')

#----------------------------------
# Создание таблицы Fraud которая составляет новый отчет
#-------------------------
def createTableFraud():
    cursor.execute('''
        CREATE TABLE fraud AS
            SELECT * from(
                SELECT DISTINCT
                    date as event_dt,
                    passport as passport_num,
                    last_name ||' '|| first_name||' '||patronymic AS fio,
                    phone,
                    CASE
                        WHEN passport_valid_to<date
                            THEN 'passport_block'
                        WHEN account_valid_to<date
                            THEN 'account_block'
                        WHEN city <> lag(city) OVER(
                        PARTITION BY card ORDER BY trans_id)
                        AND strftime('%s',date)/60-strftime('%s',lag(date) over (
                        PARTITION BY card ORDER BY trans_id))/60<60
                            THEN 'location_fraud'
                        WHEN
                            oper_result='Успешно'
                            AND lag(oper_result)   over (PARTITION BY card ORDER BY trans_id)='Отказ'
                            AND lag(oper_result,2) over (PARTITION BY card ORDER BY trans_id)='Отказ'
                            AND amount<lag(amount) over (PARTITION BY card ORDER BY trans_id)
                            AND lag(amount)        over (PARTITION BY card ORDER BY trans_id)<
                                lag(amount,2)      over (PARTITION BY card ORDER BY trans_id)
                            AND strftime('%s',date)/60-strftime('%s',lag(date) over (
                            PARTITION BY card ORDER BY trans_id))<20
                                THEN 'selection_fraud'
                    END AS event_type,
                    datetime(current_timestep) as report_dt
                FROM table_trans
                WHERE passport NOT IN (SELECT passport_num FROM passport_blacklist)) t1
                WHERE event_type IS NOT null
            )
    ''')
