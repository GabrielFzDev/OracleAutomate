import cx_Oracle
import os
import csv
import datetime

archiveLines = []
listColunms = []

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
        listColunms.append(column[0])
        if i == 0:
            columns = column[0]
        else:
            columns = columns + ', '+column[0]
    return columns,len(cursor.description), listColunms

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
def readArchiveSnapshot(path,sep,tablename):
    tableColunms = generateColumns(tablename) #pegar numero das Colunas
    with open(path,'r', encoding='UTF-8') as csv_file:
        csv_reader = csv.reader(csv_file,delimiter=sep)
        for line in csv_reader:
            temp = []
            header = line
            regexColunms(header,tableColunms[2])
            archiveColunms = len(line) + 1 # Abrir o arquivo e ver numero de Colunas + snapshot
            if not archiveColunms == tableColunms[1]:
                print('Number of colunms isnt the same!') # e nao for igual para o programa
                return archiveColunms, tableColunms[1]
            else: # se for igual colocar uma lista de listas
                temp.append(datetime.datetime.strftime(datetime.datetime.now(),'%d/%m/%Y'))
                for data in line:
                    temp.append(data)
                archiveLines.append(tuple(temp))
                break
    return tuple(archiveLines) # retornar como tuplas de tuplas

def regexColunms(header, colunms):
    print(header,colunms)
#Colunas com o mesmo nome

#readArchive(path = r'',sep='|',)

