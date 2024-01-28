
# app streamlit com Graficos


import pandas as pd
import pandas.io.sql as sqlio
import matplotlib.pyplot as plt
import numpy as np
import streamlit as st
import openpyxl as op
from sqlalchemy import text
from sqlalchemy import create_engine
from ml_model import train_model, measure_model, predict_future_value, separar_treino_teste, cria_parquet
from update_data import update_date
from sklearn.ensemble import GradientBoostingRegressor
import pyarrow as pa
import pyarrow.parquet as pq
import datetime
import psycopg2






# .streamlit/secrets.toml
dialect = "postgresql"
host = "localhost"
port = "5432"
database = "postgres"
username = "postgres"
password = "Fiap123"



engine = create_engine("postgresql://postgres:Fiap123@localhost:5432/postgres")
conn = engine.connect()
sql_consulta = """select * from ipea_dados;"""
dfconsulta_base = sqlio.read_sql_query(sql_consulta, conn)
print(dfconsulta_base)


## todos os graficos serão feitos com base no dataframe dfconsulta_base e vou filtrar os anos de análíse por período. 

#título da página 
st.title("Tech Challenge 4")
st.subheader('By Luciane dos Santos Reis')

#layout da página inteira
tab0, tab1 = st.tabs(['   ', ' '])


with tab0:
    st.write('')



# Gráfico Visão Geral 

with tab0:
    st.markdown('<h1 style="text-align: center;">Análise econômica - Preço por barril do petróleo</h1>', unsafe_allow_html=True)

st.markdown("---")

fig, axis = plt.subplots(figsize=(15, 6))
cores = ['seagreen','lightseagreen', 'green']
grafico_vl = dfconsulta_base.plot( grid='gray',y='Preco', x='Data', marker='o', kind='line', color=cores, title='Preço por Barril de Petróleo', fontsize=16)
fig1 = grafico_vl.get_figure()
fig1.set_size_inches(16, 6)  
grafico_vl.set_title('Visão Geral - 1987 a 2024 \n', fontsize=26)
plt.xticks(rotation=45)
plt.xlabel('')
st.pyplot(fig1)
st.markdown("    ")
st.markdown("    ")
st.markdown("---")
st.markdown('<p style="text-align: center;font-size:23px;">Este é o gráfico completo da base de dados do IPEA, De 1987 a 2024.  </p>', unsafe_allow_html=True)
st.markdown('<p style="text-align: center;font-size:23px;">O preço do Barril de Petróleo, a longo prazo, tem uma tendência de crescimento, devido ao aumento do crescimento da atividade econômica, porém há momentos de aumento de preço mais acentuado ainda que geralmente ocorrem em momentos de crise, onde há uma redução da produção de petróleo (oferta) como guerras ou crises nas regiões produtoras de petróleo.</p>', unsafe_allow_html=True)
st.markdown('<p style="text-align: center;font-size:23px;">Há momentos de crises globais que geraram redução de demanda como a crise bancária de 2009, a crise gerada pela pandemia do Covid-19 e nesses momentos, há uma forte redução da demanda que leva a uma queda no preço a curto prazo, no entanto como esse mercado tem poucos produtores mundiais, logo após essas crisees, os produtores reduzem a produção diminuindo a oferta e aumentando novamente os preços. </p>', unsafe_allow_html=True)
st.markdown('<p style="text-align: center;font-size:23px;">A seguir iremos analisar os maiores picos demonstrados no gráfico. </p>', unsafe_allow_html=True)
st.markdown("---")



##Gráfico 1990 A guerra do Golfo - o Preço aumentou 

# Convertendo as strings para datetime.date
data_inicio = pd.to_datetime('1990-01-01').date()
data_fim = pd.to_datetime('1991-12-31').date()
#filtro do período de 1990 e 1991
df_1990_to_1991 = dfconsulta_base.loc[(dfconsulta_base['Data'] >= data_inicio) & (dfconsulta_base['Data'] <= data_fim)]

grafico_v3 = df_1990_to_1991.plot(grid='gray',y='Preco', x='Data', kind='line', color=cores, title=' ',fontsize=16)
fig3 = grafico_v3.get_figure()
fig3.set_size_inches(16, 6)  
 

grafico_v3.set_title('1990 - 1991 - A guerra do Golfo\n', fontsize=26)
plt.xticks(rotation=45)
plt.xlabel('')

st.pyplot(fig3)
st.markdown("---")
st.markdown('<p style="text-align: center;font-size:23px;">Um dos primeiros picos apresentados é o período da Guerra do Golfo, o preço do petrólo aumentou consideravelmente. Uma das estratégias de defesa do Iraque no período da guerra foi derramar petróleo no Golfo Pérsico para evitar o pouso na água pelos Estados Unidos. Isso foi considerado um terrorismo ambiental e é considerado um dos maiores derramamentos de petróleo da história. O Golfo Pérsico é considerado uma das zonas costeiras mais ricas em Petróleo e quando o Iraque invadiu o Kuwait durante a Guerra, em Agosto de 1990, houve a interrupção da produção nesta região. É possível ver esse pico no gráfico no período de Agosto a Outubro. Após essa invasão, houve uma aliança entre diferentes nações gerando temor e uma incerteza econômica, afetando o preço do petróleo. É possíbel ver no gráfico que após Outubro o preço vai diminuindo, tem algumas variações e vai se mantendo baixo.</p>', unsafe_allow_html=True)
st.markdown("---")


#Gráfico 2001 a 2002 - Ataque as torres Gêmeas e Guerra no afeganistão

data_inicio1 = pd.to_datetime('2001-01-01').date()
data_fim1 = pd.to_datetime('2002-12-31').date()
#filtro do período de 2001 a 2002
df_2001_to_2002 = dfconsulta_base.loc[(dfconsulta_base['Data'] >= data_inicio1) & (dfconsulta_base['Data'] <= data_fim1)]

grafico_v3 = df_2001_to_2002.plot(grid='gray',y='Preco', x='Data', kind='line', color=cores, title=' ',fontsize=16)
fig3 = grafico_v3.get_figure()
fig3.set_size_inches(16, 6)  

grafico_v3.set_title('2001 a 2002 - Ataque as torres Gêmeas e Guerra no afeganistão\n', fontsize=26)
plt.xticks(rotation=45)
plt.xlabel('')

st.pyplot(fig3)
st.markdown("---")
st.markdown('<p style="text-align: center;font-size:23px;">O ataque as torres gêmeas em 11 de setembro de 2001 gerou impacto no mercado financeiro e no mercado de petróleo. É possível ver nitidamente a grande queda no preço do Barril neste período. Devido a paralização das atividades econômicas nos Estados Unidos como operações comerciais, demanda por energia por exemplo, contribuiram para a queda do preço do barril de petróleo. A instabilidade econômica projetou uma desaceleração economica, que refletiu na demanda por petróleo. Neste período também houve uma queda no dólar. Logo os preços começaram a subir, e em 2002 devido a guerra no Afeganistão (2001-2002), decorrente dos ataques de 11 de Setembro, há registros de picos que só começamm a aumentar a partir de Abril de 2002 e vai se mantendo em crescimento até o ano de 2003.</p>', unsafe_allow_html=True)
st.markdown("---")


#Gráfico 2007 a 2009 - Crise Bancária, Crise Financeira e Recessão
data_inicio2 = pd.to_datetime('2007-01-01').date()
data_fim2 = pd.to_datetime('2009-12-31').date()
#filtro do período de 2007 a 2009 
df_2007_to_2009 = dfconsulta_base.loc[(dfconsulta_base['Data'] >= data_inicio2) & (dfconsulta_base['Data'] <= data_fim2)]

grafico_v3 = df_2007_to_2009.plot(grid='gray', y='Preco', x='Data', kind='line', color=cores, title=' ',fontsize=16)
fig3 = grafico_v3.get_figure()
fig3.set_size_inches(16, 6)  

grafico_v3.set_title('2007 a 2009 - Crise Bancária, Crise Financeira e Recessão\n', fontsize=26)
plt.xticks(rotation=45)
plt.xlabel('')

st.pyplot(fig3)
st.markdown("---")
st.markdown('<p style="text-align: center;font-size:23px;"> Os anos de 2007, 2008 e 2009 foram marcados pela crise bancária nos Estados Unidos, Crise Financeira que atingiu todo o mundo e um período de Grande Recessão. É possível notar o crescimento do preço do barril no ano de 2007 com a crise bancária, chegando a um pico em Maio de 2008. No entanto, no ano de 2008 enquanto ocorria a crise financeira houve uma grande queda no preço do barril, que foi diminuindo até 2009, o ano da recessão. No ano de 2009 é possível notar que o preço do barril vem crescendo até atingir outros picos a partir de 2010. Novamente a desaceleração econômica global reduzindo a demanda por petróleo. A crise finaneira que afetou atividades industriais, a demanda por combustível diminuiu. E a desvalorização do dólar americano influenciou no preço do petróleo, já que ele é cotado em dólares.</p>', unsafe_allow_html=True)
st.markdown("---")

#Gráfico 2016 - A queda do dólar
data_inicio3 = pd.to_datetime('2016-01-01').date()
data_fim3 = pd.to_datetime('2016-12-31').date()
#filtro do ano de 2016
df_2016 = dfconsulta_base.loc[(dfconsulta_base['Data'] >= data_inicio3) & (dfconsulta_base['Data'] <= data_fim3)]

grafico_v3 = df_2016.plot(grid='gray', y='Preco', x='Data', kind='line', color=cores, title=' ',fontsize=16)
fig3 = grafico_v3.get_figure()
fig3.set_size_inches(16, 6)  

grafico_v3.set_title('2016 - A queda do dólar\n', fontsize=26)
plt.xticks(rotation=45)
plt.xlabel('')

st.pyplot(fig3)
st.markdown("---")
st.markdown('<p style="text-align: center;font-size:23px;">Uma série de fatores contribuiram com a queda do dólar, como a saída do Reino Unido da União Européia, o crescimento econômico moderado, as Eleições Presidenciais nos Estados Unidos, tudo isso causando uma grande insegurança nas condições econômicas mundiais e consequentemente o preço do barril no ano de 2016 baixou. Essa relação ocorre porque o preço do barril é cotado em dólares e quando há uma queda no dólar, há uma queda no preço do barril do petróleo.</p>', unsafe_allow_html=True)
st.markdown("---")


#Gráfico 2020 - Pandemia - COVID19 - O preço do Barril
data_inicio4 = pd.to_datetime('2020-01-01').date()
data_fim4 = pd.to_datetime('2020-12-31').date()
#filtro do ano de 2020
df_2020 = dfconsulta_base.loc[(dfconsulta_base['Data'] >= data_inicio4) & (dfconsulta_base['Data'] <= data_fim4)]

grafico_v3 = df_2020.plot(grid='gray', y='Preco', x='Data', kind='line', color=cores, title=' ',fontsize=16)
fig3 = grafico_v3.get_figure()
fig3.set_size_inches(16, 6)  

grafico_v3.set_title('2020 - Pandemia - COVID19 \n', fontsize=26)
plt.xticks(rotation=45)
plt.xlabel('')

st.pyplot(fig3)
st.markdown("---")
st.markdown('<p style="text-align: center;font-size:23px;">A pandemia de 2020 causou um terrível impacto no preço do barril de petróleo. As medidas mundiais de lockdown e distanciamento social causaram uma bruta queda na demanda por petróleo. No período de Março a Maio de 2020, é possível ver no gráfico o registro dessa queda e no período de Maio a Julho o preço volta a subir mas se mantém estável até o final de 2020.</p>', unsafe_allow_html=True)
st.markdown("---")



#previsoes Futuras


df_parquet = pd.read_parquet('ipea_futuro_GB.parquet')

#print(df_parquet)

#st.write("### Tabela de Dados")
#st.write(df_parquet)




####################################
# Criar gráfico Reral x Previsto
data_inicio5 = pd.to_datetime('2004-01-01').date()
data_fim5 = pd.to_datetime('2024-12-31').date()

plt.figure(figsize=(20, 6))
dfconsulta_base_real = dfconsulta_base.loc[(dfconsulta_base['Data'] >= data_inicio5) & (dfconsulta_base['Data'] <= data_fim5)]
df_parquet =  df_parquet.loc[(df_parquet['Data'] >= '2004-01-01') & (df_parquet['Data'] <= '2024-12-31')]
# Plotar dados reais
plt.plot(dfconsulta_base_real['Data'], dfconsulta_base_real['Preco'], label='Real', color='green')

# Plotar dados previstos
plt.plot(df_parquet['Data'], df_parquet['Preco'], label='Previsto', color='orange')

# Configurações do gráfico
plt.title('Real x Previsto', fontsize=26)
plt.xlabel('')
plt.xticks(rotation=45,fontsize=22)
plt.grid(True)
plt.legend(fontsize=16)
plt.yticks(fontsize=22)  # Tamanho da fonte do eixo y

# Mostrar o gráfico no Streamlit
st.pyplot(plt.gcf())



#################################### Tabela de Dados

#esse é o arquivo gerado em 21-01-2024 ao reescrever o ML
df_parquet_futuro = pd.read_parquet('ipea_futuro.parquet')
print(df_parquet_futuro)
df_parquet_futuro = df_parquet_futuro.sort_values(by='Data', ascending = True)


tamanho_pagina = 10
# Número total de registros
total_registros = len(df_parquet_futuro)

# Número total de páginas
total_paginas = (total_registros // tamanho_pagina) + 1

# Número da página atual (aqui, definido como a primeira página)
pagina_atual = st.number_input("Selecione a página", 1, total_paginas, 1)

    # Calcula os índices de início e fim para exibição na página atual
inicio = (pagina_atual - 1) * tamanho_pagina
fim = pagina_atual * tamanho_pagina



def estilo_linha(linha):
    cores = ['lightgreen', 'lightgray']  # Cores alternadas
    return [f'background-color: {cores[i % 2]}' for i in range(len(linha))]

# Aplica o estilo às linhas da tabela
st.write("### Previsão Futura - Tabela")
estilo_tabela = df_parquet_futuro.iloc[inicio:fim].style.apply(estilo_linha, axis=0)

estilo_centro = '''
    <style>
        table {
            margin-left: auto;
            margin-right: auto;
        }
    </style>
'''
st.markdown(estilo_centro, unsafe_allow_html=True)
st.table(estilo_tabela)


##### grafico da previsão futura 



grafico_v9 = df_parquet_futuro.plot(grid='gray', y='Preco', x='Data', kind='line', color=cores, title=' ',fontsize=16)
fig9 = grafico_v9.get_figure()
fig9.set_size_inches(16, 6)  

grafico_v9.set_title('Previsao Futura \n', fontsize=26)
plt.xticks(rotation=45)
plt.xlabel('')
st.pyplot(fig9)



#Texto Conclusivo

st.markdown("---")
st.markdown('<p style="text-align: center;font-size:26px;"><strong>Previsão</strong></p>', unsafe_allow_html=True)
st.markdown('<p style="text-align: center;font-size:23px;">Com as Ferramentas de Machine Leaning, especificamente com a Biblioteca GradientBoostingRegressor, fiz uma previsão do Preço do Barril de Petróleo.</p>', unsafe_allow_html=True)
st.markdown('<p style="text-align: center;font-size:23px;">Ao observar a evolução do preço do barril de petróleo, nota-se que, apesar das fortes oscilações de curto prazo, por conta das crises globais (forte aumento ou diminuição do preço, devido a oferta e demanda) é difícil ser preciso no preço, pois seria necessário ter previsibilidade desses grandes eventos, mas ao observar o longo prazo é possível ver uma tendência sobre o preço e projetá-la. Estou apresentando uma projeção de 5 anos, para que possamos enxergar essa tendência.</p>', unsafe_allow_html=True)
st.markdown('<p style="text-align: center;font-size:23px;"> ', unsafe_allow_html=True)

st.markdown("---")


######Interação com Usuário
# Botão para atualizar IPEA


st.markdown('<p style="text-align: center;font-size:30px;"><strong>Novas Projeções</strong></p>', unsafe_allow_html=True)
st.markdown('<p style="text-align: center;font-size:18px;">Neste espaço é possível simular novas projeções do preço do barril de petróleo para o período desejado.</p>', unsafe_allow_html=True)

st.markdown("---")



#Titulo
st.subheader("Atualize os Dados")

update_ipea = st.button("Atualizar_IPEA")
if update_ipea:
    update_date()
    today = datetime.date.today()
    st.success(f"Base atualizada com sucesso em {today}.")

st.markdown('<p style="font-size:15px;">Clique aqui para atualizar a os dados do IPEA em nossa base.</strong></p>', unsafe_allow_html=True)
st.markdown('<p style=";font-size:15px;">Fonte: http://www.ipeadata.gov.br/ExibeSerie.aspx?module=m&serid=1650971490&oper=view.</strong></p>', unsafe_allow_html=True)
st.markdown("---")




#botao -  Refazendo a previsao 


st.subheader("Selecione um período.")

selected_date_start = st.date_input("Início")
selected_date_end = st.date_input("Fim")
#calula a quantidade de dias 
if selected_date_start and selected_date_end:
        diferenca_dias = (selected_date_end - selected_date_start).days

        # Imprime a mensagem com a diferença em dias
        st.success(f"Vamos calular a predição de  {diferenca_dias} dias.")
st.markdown("---")

st.subheader("Execute a Previsão")

# Botão para Executar tudo 
mchine_learning = st.button("Executar_ML")


if mchine_learning:
    print('Separando treino e teste')
    treino_teste_result = separar_treino_teste()
    print(treino_teste_result)
    st.success("Base separada com sucesso!")


    print('treinando o modelo')
    x_train, x_test, y_train, y_test =  treino_teste_result 
    modelo_treinado = GradientBoostingRegressor(n_estimators=100, learning_rate=0.1, max_depth=5, random_state = 0, loss='squared_error')
    modelo_treinado.fit(x_train, y_train)
    print('imprimindo modelo treinado')
    print(modelo_treinado)
    st.success("Modelo treinado com sucesso!")



        
    print('Prevendo valores')
    future_value = predict_future_value(x_test,x_train,y_train)
    
          
    print('Criando o arquivo')        
    cria_parquet (future_value, selected_date_start,diferenca_dias)
    print('Arquivo Criado')        
    
    
    df_parquet_futuro_new = pd.read_parquet('ipea_futuro_novo.parquet')
  
    st.write("### Resultado da Previsão")
    st.table(df_parquet_futuro_new) 

###### Nova Tabela de Dados ########
    


st.markdown("---")





