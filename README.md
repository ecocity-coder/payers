Анализ платежной дисциплины.

Был поставлена задача исследовать платежную дисциплину и выявить перспективных плательщиков и направление по улучшению дисциплины. 
Было проведено аналитическое исследование платежной дисциплины, включая:
- проверка данных, устранение пропусков, дубликатов, аномалий, артефактов и дефектов
- расчет сумм всех поступивших платежей каждого ЛС с учетом возвратов
- расчет плановой выручки каждого ЛС по ставкам тарифов
- расчет частоты платежей для каждой квартиры
- выявление дат оплат относительно начала и конца расчетного периода
  
Были выявлены плательщики, которые оплачивают услуги полностью, ранее остальных, в самом начале расчетного периода. На этих плательщиков было рекомендовано сделать маркетинговые акценты. Были так же выявлены плательщики, оплачивающие услуги в самом конце расчетного периода - было рекомендовано стимулировать их бонусами/льготами или другими средствами.
Создать новый столбец "Категория оплаты"
df['Категория оплаты'] = ''
# Используя формулу или скрипт в этом новом столбце, определить категорию оплаты для каждой строки, основываясь на датах загрузки.
#соответственно приведя даты к нужному формату
df['Дата загрузки'] = pd.to_datetime(df['Дата загрузки'])
df.loc[df['Дата загрузки'].dt.day >= 25, 'Категория оплаты'] = 'Конец месяца'
df.loc[df['Дата загрузки'].dt.day <= 5, 'Категория оплаты'] = 'Начало месяца'
#далее сгруппировать абонентов по по категориям "конец месяца" и "начало месяца".
grouped_df = df.groupby('Категория оплаты').agg({'ЛС': 'count'})

