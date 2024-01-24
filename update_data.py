
#importando Bibliotecas
import requests
from bs4 import BeautifulSoup
import pandas as pd
import pandas.io.sql as sqlio
import psycopg2 as ps
from sqlalchemy import text
from sqlalchemy import create_engine
from datetime import date

###Objetivo: Atualizar a base IPEA existente


def update_date():
#consulta da base existente com o objetivo de ler a ultima data preenchida.

     engine = create_engine("postgresql://postgres:Fiap123@localhost:5432/ipea")
     conn = engine.connect()

     sql_consulta = """select * from ipea_dados;"""

     dfconsulta_base = sqlio.read_sql_query(sql_consulta, conn)
     
     #fim da consulta da base


     #verificar a ultima data do DF que contém as informações existentes na base
     ultima_data = dfconsulta_base["Data"].max()
  
     #fim da consulta da ultima data- reserva
     


     ################# baixar o arquivo aqui para comparar com a ultima data da base
     url = "http://www.ipeadata.gov.br/ExibeSerie.aspx?module=m&serid=1650971490&oper=view"
     response = requests.get(url)

     #se a conexão der certo, faz o webscrapping
     if response.status_code == 200:
          
          soup = BeautifulSoup(response.text, 'html.parser')
               #lendo a tabela do site que contém os dados
          table = soup.find('table', {'id': 'grd_DXMainTable'})
               #lendo a tabela do site com o pandas e transformando em um Dataframe
          df_novo = pd.read_html(str(table),skiprows=1)[0]
               #alterando o nome das colunas para facilitar a manipulação e visualização
          df_novo.columns=['Data', 'Preco']
               #alterando o tipo da coluna Data para datetime para já fcilitar o insert na base
          df_novo['Data'] = pd.to_datetime(df_novo['Data'], format='%d/%m/%Y')
          #df_novo['Data']= pd.Timestamp(df_novo['Data'])
          #df_novo['Data'] = df_novo['Data'].date()

               #alterando o tipo da coluna Preco para numerico
          df_novo['Preco']=df_novo['Preco'].round(2)
          df_novo['Preco'] = df_novo['Preco'] / 100.0 
          df_novo['Preco']=df_novo['Preco'].apply(lambda x: '{:.2f}'.format(x))
     
               #ordenando o arquivo
          df_novo=df_novo.sort_values(by='Data', ascending=True).reset_index(drop=True)
          print("Dados consultados com sucesso!")
     else:
          print("Falha ao acessar o site. Verifique a URL e a conexão com a internet.")
     ############## fim da consulta do novo arquivo 

     #consulta a ultimo data do novo arquivo consultado do Site IPEA         
     nova_data = df_novo['Data']. max()
     nova_data = nova_data.date()
     #commpara a ultima data do dataframe novo com a ultima data da base de dados e incrementa na base
     if nova_data > ultima_data:
          engine = create_engine("postgresql://postgres:Fiap123@localhost:5432/ipea")  #não precisa
          df_novo.to_sql('ipea_dados', con=engine, if_exists='append', index=False)
          print('Dados atualizados na base de dados  com sucesso')
     else:
          print('Não há dados a serem atualizados.')


      ##### Fim da Função. 