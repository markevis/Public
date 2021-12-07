#!/usr/bin/env python
# coding: utf-8

# In[1]:


#importar bibliotecas
import pandas as pd
from sklearn.tree import DecisionTreeClassifier


# In[2]:


#importar base de dados e adicionar a coluna 'Age' ao DataFrame existente e explorar colunas necessárias

df = pd.read_csv('ml_project1_data.csv', sep=',', encoding='cp1252', usecols=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10 ,11, 12, 13])


# In[3]:


#importar base de dados e adicionar a coluna 'Age' ao DataFrame existente
now = pd.to_datetime('now')

age = (now.year - df['Year_Birth'])

new_df = pd.DataFrame(df)

new_df['Age'] = age

new_df.head()


# In[4]:


# adicionar a coluna 'Dt_Last_Purchase' ao DataFrame existente
import numpy as np

df['Dt_Customer'] = pd.to_datetime(df['Dt_Customer'])

temp = df['Recency'].apply(np.ceil).apply(lambda x: pd.Timedelta(x, unit='D'))

last_purchase = df['Dt_Customer'] + temp

new_df['Dt_Last_Purchase'] = last_purchase

new_df.head()


# In[5]:


# Verificando a data de compra do último cliente
data_ref = new_df.Dt_Last_Purchase.max()
print(data_ref)


# In[6]:


# Analise exploratória dos clientes baseada nas features | 'DtCustomer' & 'Recency' | 
# Desde a data de inscrição do cliente na empresa e numero de dias desde a última compra
import seaborn as sns
sns.set_palette("rocket")

ax = sns.boxplot(data = new_df, x = 'Age' )
ax.set_xlabel("Idade das pessoas que compraram desde agosto de 2012 até {}".format(data_ref.strftime('%d de %B de %Y')),
              fontsize = 20)
ax.figure.set_size_inches(20,8)
sns.set(font_scale = 1.8)


# In[7]:


######
ax = sns.histplot(new_df.Age, kde = True,
                  bins = [i*5 for(i) in range (25)])
ax.set_xlabel("Idade das pessoas", 
              fontsize = 20, position=(0.5,0.5))
ax.set_ylabel("Quantidade de pessoas", 
              fontsize = 20, position=(0.5,0.5))
ax.grid(None)

ax.set_title('Distribuição do número de compras desde julho de 2012 até {}'.format(data_ref.strftime('%d de %B de %Y')))

ax.figure.set_size_inches(20,9)


# In[8]:


#new_df = new_df.sort_values(by= 'Age')

dados_filtrados = pd.DataFrame(new_df)

categories = ['De 21 a 30 anos',
                              'De 31 a 40 anos','De 41 a 50 anos','De 51 a 60 anos',
                              'De 61 a 70 anos','De 71 a 80 anos','De 81 a 90 anos','Mais de 90 anos']
faixa_idade = [20 , 30 , 40 , 50 , 60 , 
               70 , 80 , 90, 130]
dados_filtrados['faixa'] = pd.cut(dados_filtrados.Age,
                                  bins= faixa_idade,labels = categories, 
                                  include_lowest= True)


# In[9]:


### 
dados_filtrados.index = pd.to_datetime(dados_filtrados['Dt_Last_Purchase'])
dados_filtrados.faixa.unique();


# In[10]:


####
dados_data = dados_filtrados.groupby(pd.Grouper(freq= 'M'))


# In[11]:


###
dados_data.groups;


# In[12]:


####
df1208 = dados_data.get_group('2012-08-31')
df1209 = dados_data.get_group('2012-09-30')
df1210 = dados_data.get_group('2012-10-31')
df1211 = dados_data.get_group('2012-11-30')
df1212 = dados_data.get_group('2012-12-31')
df1301 = dados_data.get_group('2013-01-31')
df1302 = dados_data.get_group('2013-02-28')
df1303 = dados_data.get_group('2013-03-31')
df1304 = dados_data.get_group('2013-04-30')
df1305 = dados_data.get_group('2013-05-31')
df1306 = dados_data.get_group('2013-06-30')
df1307 = dados_data.get_group('2013-07-31')
df1308 = dados_data.get_group('2013-08-31')
df1309 = dados_data.get_group('2013-09-30')
df1310 = dados_data.get_group('2013-10-31')
df1311 = dados_data.get_group('2013-11-30')
df1312 = dados_data.get_group('2013-12-31')
df1401 = dados_data.get_group('2014-01-31')
df1402 = dados_data.get_group('2014-02-28')
df1403 = dados_data.get_group('2014-03-31')
df1404 = dados_data.get_group('2014-04-30')
df1405 = dados_data.get_group('2014-05-31')
df1406 = dados_data.get_group('2014-06-30')
df1407 = dados_data.get_group('2014-07-31')
df1408 = dados_data.get_group('2014-08-31')
df1409 = dados_data.get_group('2014-09-30')
df1410 = dados_data.get_group('2014-10-31')


# In[13]:


#####
todos_meses = [df1208 ,df1209 ,df1210 ,df1211 ,df1212 ,
               df1301 ,df1302 ,df1303 ,df1304 ,df1305 ,
               df1306 ,df1307 ,df1308 ,df1309 ,df1310 ,
               df1311 ,df1312 ,df1401 ,df1402 ,df1403 ,
               df1404 ,df1405 ,df1406 ,df1407 ,df1408 , 
               df1409 ,df1410]
 
for i in range(0,len(todos_meses)):
    #print(i)
    todos_meses[i] = todos_meses[i].groupby('faixa').Age.count()


# In[14]:


for i in range(0,len(todos_meses)):
    #print(i)
    if i+8 <= 12 :
        todos_meses[i].rename('2012 - '+"%02d" % (i+8),inplace= True)
    else:
        todos_meses[i].rename('2013 - '+"%02d" % (i-4),inplace= True)


# In[15]:


####
de1208_ate1410 = pd.DataFrame()

for i in range(0,len(todos_meses)):
    de1208_ate1410 = de1208_ate1410.append(todos_meses[i])


# In[16]:


####
de1208_ate1410 = de1208_ate1410.T


# In[17]:


###
de1208_ate1410


# In[18]:


####
de1208_ate1410_porcentagem = pd.DataFrame()

for mes in de1208_ate1410:
    de1208_ate1410_porcentagem[str(mes)] = (de1208_ate1410[mes].values/de1208_ate1410[mes].values.sum())

de1208_ate1410_porcentagem.index = de1208_ate1410.index


# In[19]:


###
###import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
import matplotlib.ticker as mticker

mpl.rcParams['font.size'] = 6.5

x = np.array(range(100, 500, 50))
y = 37*x

fig, [ax1, ax2, ax3] = plt.subplots(1,3)

ax1.plot(x,y, linewidth=5, color='green')
ax2.plot(x,y, linewidth=5, color='red')
ax3.plot(x,y, linewidth=5, color='blue')

label_format = '{:,.0f}'

sns.set_palette("Paired")

ax = de1208_ate1410_porcentagem.T.plot(kind= 'bar' , stacked= True, figsize= (20,8),fontsize =20)
ax.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.5, fontsize = 20)
# fixing yticks with "set_yticks"
ticks_loc = ax2.get_yticks().tolist()
ax2.set_yticks(ax1.get_yticks().tolist())
ax2.set_yticklabels([label_format.format(x) for x in ticks_loc])
#ax.set_yticklabels(['0%' , '20%', '40%' , '60%', '80%', '100%'])
ax.set_ylabel("Proporção",fontsize=25)
ax.set_title("Proporção de COMPRAS de cada faixa etária por CLIENTE", fontsize = 30)
ax.set_xticklabels(de1208_ate1410.columns, rotation=90)
ax.set_xlabel("Ano-mês",fontsize=25)


# In[ ]:




