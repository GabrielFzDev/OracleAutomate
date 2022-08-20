import cx_Oracle
import os
import csv


def connect():
    oracle_pass = os.environ.get('ORACLEPASS')
    oracle_user = os.environ.get('ORACLEUSER')
    conn = cx_Oracle.connect(f'{oracle_user}/{oracle_pass}@tacservicesdb_high')
    return conn.cursor()

#Gera o nome das colunas e coloca de forma linear
def generateColumns(tableName):
    cursor = connect()
    cursor.execute(f'select * from {tableName}')

    for i,column in enumerate(cursor.description):
        if i == 0:
            columns = column[0]
        else:
            columns = columns + ', '+column[0]
    return columns,len(cursor.description)

# Pega o nome das colunas e quantas colunas sao para fazer um comando SQL de insert completo
def generateSqlInsert(tableName):
    values  = ''
    colunas = generateColumns(tableName)
    for i in range(0,colunas[1]):
        if i == 0:
            values = values + f"nvl(:{i},'')" #NVL para se for null a informação colocar ''
        else:
            values = values + f", nvl(:{i},'')" # retorna o comando insert completo
    format = 'INSERT INTO {tableName} ({colunas[0]}) values({values})'

#Ler o Arquivo e printar a primeira Coluna que normalment eh o cabeça~lho
def readArchive(path,sep,tablename):
    with open(path,'r', encoding='UTF-8') as csv_file:
        csv_reader = csv.reader(csv_file,delimiter=sep)
        for line in csv_reader:
            return line

#readArchive(path = r'',sep='|',)

