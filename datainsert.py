import os
import glob
import errno
import psycopg2
# source :   https://pynative.com/python-postgresql-tutorial/

"THIS CODE IS NOT COMPLETE.FOR NOW IT ONLY INSERTS ONE ROW OF DATA WITH ONLY THE COMMON FIELDS(65 FIELDS) IN THE SQL TABLE." \
"SO GIVING MULTIPLE ROWS OR ROWS MORE THAN COLUMNS WILL RESULT IN AN ERROR"

connection = psycopg2.connect(user="postgres",
                                  password="pass",    # CHANGE TO YOUR PASSWORD BEFORE RUNNING THE CODE
                                  host="127.0.0.1",
                                  port="5432",
                                  database="postgres"
                                  )

curr = connection.cursor()

def set_data(filepath):
    with open(filePath, 'r') as file:
         return file.readline().split("|")

def get_columns(c):
    if c=="voice":
        return ['CDR_ID',
                    'CDR_SUB_ID' ,
                    'CDR_TYPE'      ,
                    'SPLIT_CDR_REASON' ,
                    'CDR_BATCH_ID'   ,
                    'SRC_REC_LINE_NO'  ,
                    'SRC_CDR_ID'     ,
                    'SRC_CDR_NO'       ,
                    'STATUS'          ,
                    'RE_RATING_TIMES'  ,
                    'CREATE_DATE'       ,
                    'START_DATE'       ,
                    'END_DATE'       ,
                    'CUST_LOCAL_START_DATE' ,
                    'CUST_LOCAL_END_DATE'  ,
                    'STD_EVT_TYPE_ID'  ,
                    'EVT_SOURCE_CATEGORY' ,
                    'OBJ_TYPE'           ,
                    'OBJ_ID'           ,
                    'OWNER_CUST_ID'     ,
                    'DEFAULT_ACCT_ID'   ,
                    'PRI_IDENTITY'      ,
                    'BILL_CYCLE_ID'     ,
                    'SERVICE_CATEGORY'  ,
                    'USAGE_SERVICE_TYPE' ,
                    'SESSION_ID'        ,
                    'RESULT_CODE'      ,
                    'RESULT_REASON'      ,
                    'BE_ID'             ,
                    'HOT_SEQ'           ,
                    'CP_ID'              ,
                    'RECEPIENT_NUMBER'   ,
                    'USAGE_MEASURE_ID'   ,
                    'ACTUAL_USAGE'       ,
                    'RATE_USAGE'         ,
                    'SERVICE_UNIT_TYPE'  ,
                    'USAGE_MEASURE_ID2'  ,
                    'ACTUAL_USAGE2'    ,
                    'RATE_USAGE2'       ,
                    'SERVICE_UNIT_TYPE2' ,
                    'DEBIT_AMOUNT'         ,
                    'Reserved'             ,
                    'DEBIT_FROM_PREPAID'  ,
                    'DEBIT_FROM_ADVANCE_PREPAID' ,
                    'DEBIT_FROM_POSTPAID' ,
                    'DEBIT_FROM_ADVANCE_POSTPAID' ,
                    'DEBIT_FROM_CREDIT_POSTPAID' ,
                    'TOTAL_TAX'            ,
                    'FREE_UNIT_AMOUNT_OF_TIMES' ,
                    'FREE_UNIT_AMOUNT_OF_DURATION' ,
                    'FREE_UNIT_AMOUNT_OF_FLUX' ,
                    'ACCT_ID'              ,
                    'ACCT_BALANCE_ID'    ,
                    'BALANCE_TYPE'        ,
                    'CUR_BALANCE'         ,
                    'CHG_BALANCE'         ,
                    'CURRENCY_ID'          ,
                    'OPER_TYPE'          ,
                    'CUR_EXPIRE_TIME'    ,
                    'FU_OWN_TYPE'         ,
                    'FU_OWN_ID'          ,
                    'FREE_UNIT_TYPE'      ,
                    'FREE_UNIT_ID'       ,
                    'BONUS_AMOUNT'     ,
                    'CURRENT_AMOUNT'      ,
                    'FU_MEASURE_ID']

def set_columns_for_sql(c):
    columns = get_columns(c)
    return " ("+(','.join(columns))+")"


def set_data_for_sql(filePath):
    with open(filePath, 'r') as file:
         data = file.readline().split("|")
         for n,i in enumerate(data):
             if i == '':
                 data[n]="0"
    s="("+(','.join(data)+")")
    if s[len(s)-2]==',':
        s[len(s)-2]=''
    return (s)


def insert_data(filePath,table,c):
    with open(filePath, 'r') as file:
         data = file.readline().split("|")
    fields = get_columns(c)
    sql=("INSERT INTO "+ table + set_columns_for_sql(c) +
         " VALUES " + set_data_for_sql(filePath))
    print("sql------------", sql)
    curr.execute(sql)
    connection.commit()
    print("Data loaded to database from text file")
    curr.close()



def main():
    path = input("Enter Your data File Directory: ")    # path of the unl file
    table = input("Enter  Database Table Name: ")       # name of table schemas HERE IT IS "voice_table"!!!!!
    category = input("Enter category of data:")         # here just type 'voice' as we are dealing with only voice data

    filenames = glob.glob(path)
    for filename in filenames:
        print("-file------+++++++++--", filename)
        insert_data(filename,table,category)




main()
