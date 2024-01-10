df = pd.read_csv("example.csv")


import psycopg2 as ps
import pandas as pd

df = pd.read_csv("C:/Users/admin/Desktop/тест/Tablica_LS.csv")
df.head()

загрузим и изучим таблицу

df.info()

проверим таблицу на дубликаты

df.duplicated(keep=False)

дубликатов не обнаружено. Однако данные в столбце "дата начала услуги" нужно привести в удобный формат

df['Дата начала услуги'] = pd.to_datetime(df['Дата начала услуги'],format='%d.%m.%Y')

df.info()

df.to_csv('table_LS_new_6.csv',date_format='%d.%m.%Y',index=False)

посмотрим на таблицу платежей

df = pd.read_csv("C:/Users/admin/Desktop/тест/Tablica_plat.csv")
df.head()

df.info()



данные в "сумме платежа" и в "дате загрузки" нужно привести к удобному формату

но предварительно нужно именить запятую на точку в "Сумме платежа"

df['Сумма платежа'] = df['Сумма платежа'].str.replace(',', '.')

df['Сумма платежа'] = pd.to_numeric(df['Сумма платежа'])

df['Дата загрузки'] = pd.to_datetime(df['Дата загрузки'],  format='%m/%d/%Y')

df.info()

Рассчитаем для каждого ЛС сумму всех поступивших платежей с учетом возвратов, но исключая списания

sum_by_account = df[df['Сумма платежа'] > 0].groupby('ЛС')['Сумма платежа'].sum()
print(sum_by_account)

Рассчитаем для каждого ЛС плановую выручку с учетом дат начала услуг на ЛС и ставки тарифа

new_table = pd.read_csv('C:/Users/admin/table_LS_new_6.csv')

new_table.head()

import datetime
current_date = datetime.datetime.now() # получить текущую дату и время

df['Плановая выручка'] = df.apply(lambda row: row['Ставка тарифа'] * (current_date - row['Дата начала услуги']).days, axis=1)

df.info()

df.head()


df.to_csv('table_LS_new_7.csv',index=False)

df = pd.read_csv('C:/Users/admin/table_LS_new_7.csv')

df.head()

Рассчитаем для каждого ЛС сальдо: плановая выручка (п. 3) минус сумма платежей (из п. 2).    

Предваритаельно нужно привести данные в столбцах к числовому формату

df_plat = pd.read_csv("C:/Users/admin/Desktop/тест/Tablica_plat.csv")
df_ls_new = pd.read_csv('C:/Users/admin/table_LS_new_7.csv')
df_plat['Сумма платежа'] = pd.to_numeric(df_plat['Сумма платежа'], errors='coerce')
df_ls_new['Плановая выручка'] = pd.to_numeric(df_ls_new['Плановая выручка'], errors='coerce')

Теперь можно рассчитать сальдо

df_merged = pd.merge(df_ls_new, df_plat, left_on='Лицевой счет (ЛС)', right_on='ЛС', how='left')

df_merged['Сальдо'] = df_merged['Плановая выручка'] - df_merged['Сумма платежа']

print (df_merged)

посмотрим на новвю таблицу

df_merged.to_csv('C:/Users/admin/saldo_table.csv', index=False)

df = pd.read_csv('C:/Users/admin/saldo_table.csv')

df.head()

Чтобы рассчитать для каждой квартиры частоту платежей и количество оплаченных тарифов а календарный год нужно еще раз подготовить данные.

df_1 = pd.read_csv('C:/Users/admin/table_LS_new_7.csv')

df_1['Дата начала услуги'] = pd.to_datetime(df_1['Дата начала услуги'])

для дальнейшей работы переименую столбец ЛС в "Лицевой счет" в таблице Лицевых счетов

df_1.rename(columns={'Лицевой счет (ЛС)': 'Лицевой счет'}, inplace=True)

Приводим данные в столбцах 'Дата начала услуги' и 'Дата загрузки' к формату datetime

df_2 = pd.read_csv("C:/Users/admin/Desktop/тест/Tablica_plat.csv")

df_2['Дата загрузки'] = pd.to_datetime(df_2['Дата загрузки'])

и для дальнейшей работы нужно переименовать столбец ЛС в "Лицевой счет" в этой таблице

df_2.rename(columns={'ЛС': 'Лицевой счет'}, inplace=True)

Приводим данные в столбце 'Сумма платежа' к числовому формату и заменяем запятую

df_2['Сумма платежа'] = df_2['Сумма платежа'].str.replace(',', '.')

df_2['Сумма платежа'] = pd.to_numeric(df_2['Сумма платежа'])

Объединяем таблицы по столбцу 'Лицевой счет'

merged_df = pd.merge(df_1, df_2, on='Лицевой счет')

Сгруппируем данные по 'ИД квартиры' и считаем частоту платежей и количество оплаченных тарифов за календарный год

result = merged_df.groupby('ИД квартиры').agg({'Сумма платежа': 'count', 'Дата начала услуги': 'nunique'})

print(result)

В качестве других метрик для анализа платежной дисциплины абонентов за услуги я предлагаю разделить абонентов
на тех кто платит в начале месяца и в конце месяца.
Эта метрика может указывать на дисциплинированность абонетов. Можно применить такой код


df = pd.read_csv("C:/Users/admin/Desktop/тест/Tablica_plat.csv")

# Создать новый столбец "Категория оплаты"
df['Категория оплаты'] = ''
# Используя формулу или скрипт в этом новом столбце, определить категорию оплаты для каждой строки, основываясь на датах загрузки.
#соответственно приведя даты к нужному формату
df['Дата загрузки'] = pd.to_datetime(df['Дата загрузки'])
df.loc[df['Дата загрузки'].dt.day >= 25, 'Категория оплаты'] = 'Конец месяца'
df.loc[df['Дата загрузки'].dt.day <= 5, 'Категория оплаты'] = 'Начало месяца'
#далее сгруппировать абонентов по по категориям "конец месяца" и "начало месяца".
grouped_df = df.groupby('Категория оплаты').agg({'ЛС': 'count'})


print(df.head())

print(grouped_df)

Визуализируем метрику сальдо

data = pd.read_csv('C:/Users/admin/saldo_table.csv')

import matplotlib.pyplot as plt

data['Дата загрузки'] = pd.to_datetime(data['Дата загрузки'])
data['Сальдо'] = data['Плановая выручка'] - data['Сумма платежа']
daily_saldo = data.groupby('Дата загрузки')['Сальдо'].sum()
plt.figure(figsize=(10,6))
plt.plot(daily_saldo.index, daily_saldo.values)
plt.xlabel('Дата загрузки')
plt.ylabel('Сальдо')
plt.title('Динамика сальдо по дате загрузки')


plt.show()

