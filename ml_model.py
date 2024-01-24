#importando as bibliotecas
import pandas as pd
from sklearn.model_selection import train_test_split
#from xgboost import XGBRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error
from sklearn.ensemble import GradientBoostingRegressor
import pandas.io.sql as sqlio
from sqlalchemy import text
from sqlalchemy import create_engine
import pyarrow as pa
import pyarrow.parquet as pq



#funções

################## 0 - separa em treino e teste

def separar_treino_teste ():
 
#ler a tabela da base de dados com informações do IPEA
 engine = create_engine("postgresql://postgres:Fiap123@localhost:5432/ipea")
 conn = engine.connect()
 sql_consulta = """select * from ipea_dados;"""
 dfconsulta_base = sqlio.read_sql_query(sql_consulta, conn)

#Criar lags
 for lag in range(1,5):
  dfconsulta_base[f'Preco_lag_{lag}']=dfconsulta_base['Preco'].shift(lag)
  dfconsulta_base = dfconsulta_base.dropna()

#Preparar treino e teste
 x = dfconsulta_base[['Preco_lag_1','Preco_lag_2','Preco_lag_3','Preco_lag_4']].values 
 y = dfconsulta_base['Preco'].values

#Dividir conjunto de valores em x train e x test
 x_train, x_test, y_train, y_test = train_test_split(x,y,test_size=0.3,shuffle=False)
 
 return  x_train, x_test, y_train, y_test




##################  1 -  treinar modelo

def train_model(x_train, y_train):

#Treinar com GradientBoostingRegressor
 model = GradientBoostingRegressor(n_estimators=100, learning_rate=0.1, max_depth=5, random_state = 0, loss='squared_error')
 model.fit(x_train, y_train)

#Treinar com XgBoost 
 #modelA = XGBRegressor(objective='reg:squarederror', n_estimators=100, learning_rate=0.1, max_depth=5, random_state = 0)
 #modelA.fit(x_train, y_train)

 return model 

 #model = GradientBoostingRegressor(n_estimators=100, learning_rate=0.1, max_depth=5, random_state = 0, loss='squared_error')

################## 2 - prever valores 

def predict_future_value(x_test,x_train,y_train):
  model = GradientBoostingRegressor(n_estimators=100, learning_rate=0.1, max_depth=5, random_state = 0, loss='squared_error')
  model.fit(x_train, y_train)

#Prever com GradientBoostingRegressor
  predictions = model.predict(x_test)

#Fazer Previsões com XgBoost --> no curso da alura, a predição tá na api
 # predictionsXG = modelA.predict(x_test)
  return predictions



###################  3 - medir  modelo   --> fazer essa?

def measure_model(predictions, y_test):

#Avaliar o GradientBoostingRegressor
 mse=mean_squared_error(y_test, predictions)
 mae=mean_absolute_error(y_test, predictions)
 print('MSE:', mse)
 print('MAE:', mae)

#Avaliar o Modelo XgBoost -- apagar aqui por enquanto
 #msexg=mean_squared_error(y_test, predictionsXG)
 #maexg=mean_absolute_error(y_test, predictionsXG)
 #print('MSE_xg:', msexg)
 #print('MAE_xg:', maexg)


###################  3 - grava arquivo   -->

##### visualiza a previsao e grava em um parquet

def cria_parquet (predictions, data_inicial, num_dias):
 #receber datas inicial e número de dias futuros por parametro
 data_inicial = pd.to_datetime(data_inicial)
 num_datas_futuras = num_dias

 #criando uma data inicial para inicir o range
 #data_inicial = pd.to_datetime('2004/08/01')
 #determinando o numero de datas futuras 
 #num_datas_futuras = 1825

# Gerar datas futuras
 datas_futuras = pd.date_range(start=data_inicial, periods=num_datas_futuras + 1, freq='D')[1:]

 #Grava no array a previsão futura do período fornecido
 previsao_futura2 = predictions[0:num_dias] 
 #Grava no Dataframe a previsão com colunas data e preço 
 previsao_futura3 = pd.DataFrame({'Data': datas_futuras, 'Preco': previsao_futura2})
 print("Criando o Arquivo")

#definindo o nome do arquivo .parquet
 nome_arquivo_parquet2 = 'ipea_futuro_novo.parquet'
#salvando o DataFrame em um arquivo Parquet
 previsao_futura3.to_parquet(nome_arquivo_parquet2, index=False)
#fim da criação do parquet 
 return predictions 



