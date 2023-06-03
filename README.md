# Test

Создать DAG в Airflow который будет запускаться ежедневно

задачи dag:
1 задача:
1. спарсить данные по коронавирусу по странам с url "https://www.worldometers.info/coronavirus/"
2. Предобработать данные - убрать символы '\n' и ':' c текстовых значений, а цифры привести в формат int
3. Сохранить три таблицы с колонками ['Country,Other', 'TotalCases', 'NewCases', 'TotalDeaths', 'NewDeaths', 'TotalRecovered',  'NewRecovered', 'ActiveCases'] в соответствующие таблицы в базе данных Postgres

2 задача:
1. Сделать запрос (query) к одной из таблиц (любой из предыдущих трех созданных) и отсортировать таблицу по отношению 'TotalRecovered' к 'TotalCases' (от меньшего к большему).
2. Сохранить первые десять строк с колонками ['Country','TotalRecovered','TotalCases'] в новую таблицу Postgres
