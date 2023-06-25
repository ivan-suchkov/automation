import telegram
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import seaborn as sns
import io
import pandas as pd
import pandahouse as ph
from datetime import datetime, timedelta
from datetime import date
from io import StringIO
import requests
from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

# дефолтные аргументы дага
default_args = {
                'owner': 'i-suchkov',
                'depends_on_past': False,
                'retries': 3,
                'retry_delay': timedelta(minutes=2),
                'start_date': datetime(2023, 4, 24)
                }

# откуда получим данные
connection = {
            'host': ###,
            'database': ###,
            'user': ###, 
            'password': ###
                    }

# расписание дага - каждые 15 минут
schedule_interval = '0/15 * * * *' 

# обозначаем бота
my_token =  ###
bot = telegram.Bot(token=my_token) 

alert_chat_id = ### ## общий чат, куда будем отправлять отчеты
chat_id = ### ## мой персональный айди для проверки

# фунцкия чтения SQL запроса
def query(query):
    """
    Функция принимает SQL запрос, сохраненный в переменную query и данные о подключении, сохраненные в переменную connection.
    Функция возвращает датафрейм пандас с таблицей, полученной в результате запроса. 
    """
    return ph.read_clickhouse(query, connection=connection)


def check_anomaly_quantile(df, metric, a=1, n=1):
    '''
    Функция check_anomaly_quantile предлагает алгоритм проверки значения на аномальность посредством
    поиска выбросов по статистическому методу, основанному на вычислении межквартильного размаха.
    Функция принимает в качестве аргументов датафрейм со значениями метрик по 15-минуткам, название метрики,
    коэффициент а и значение n как количество 15-минуток
    '''
    df['q25'] = df[metric].shift(1).rolling(n).quantile(0.25)
    df['q75'] = df[metric].shift(1).rolling(n).quantile(0.75)
    df['iqr'] = df['q75'] - df['q25'] 
    df['top'] = df['q75'] + a * df['iqr']
    df['bottom'] = df['q25'] - a * df['iqr']
    df['median'] = df[metric].shift(1).rolling(n).quantile(0.50)

    df['top'] = df['top'].rolling(n, center=True, min_periods=1).mean()
    df['bottom'] = df['bottom'].rolling(n, center=True, min_periods=1).mean()
    
    current_value = df[metric].iloc[-1]
    median_value = df['median'].iloc[-1]

    if df[metric].iloc[-1] < df['bottom'].iloc[-1] or df[metric].iloc[-1] > df['top'].iloc[-1]:
        is_alert = 1
    else:
        is_alert = 0
        
    if current_value <= median_value:
        diff = round(abs(current_value / median_value - 1), 3)
    else:
        diff = round(abs(median_value / current_value - 1), 3)

    return is_alert, current_value, diff


def check_anomaly_pd(df, metric, threshold=0.3):
    '''
    Функция check_anomaly_pd проверяет значение метрик на аномальность на основе сравнения с предыдущим днем. 
    Функция подсветит аномалию, если разница в значении метрики будет больше установленного порога.
    Функция принимает в качестве аргументов датафрейм со значениями метрик по 15-минуткам, навзание метрики
    и значение порога для разницы метрик.
    '''
    current_ts = df['ts'].max()  
    day_ago_ts = current_ts - pd.DateOffset(days=1) 

    current_value = df[df['ts'] == current_ts][metric].iloc[-1] 
    day_ago_value = df[df['ts'] == day_ago_ts][metric].iloc[0]

    if current_value <= day_ago_value:
        diff = round(abs(current_value / day_ago_value - 1), 3)
    else:
        diff = round(abs(day_ago_value / current_value - 1), 3)

    if diff > threshold:
        is_alert = 1
    else:
        is_alert = 0

    return is_alert, current_value, diff



# начало кода DAG
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def i_suchkov_alert_dag():
    
    # получаем данные ленты
    @task
    def get_feed():
        q = '''
            SELECT
              toStartOfFifteenMinutes(time) as ts,
              toDate(ts) as date,
              formatDateTime(ts, '%R') as hm,
              uniqExact(user_id) as feed_users,
              countIf(user_id, action = 'view') as views,
              countIf(user_id, action = 'like') as likes,
              ROUND(
                countIf(user_id, action = 'like') / countIf(user_id, action = 'view'),
                3
              ) AS CTR
            FROM
              {db}.feed_actions
            WHERE
              ts >= today() - 1
              and ts < toStartOfFifteenMinutes(now())
            GROUP BY
              ts,
              date,
              hm
            ORDER BY
              ts
            '''
        return query(q)

    # получаем данные сообщений
    @task
    def get_messages():
        q = '''
            SELECT
              toStartOfFifteenMinutes(time) as ts,
              toDate(ts) as date,
              formatDateTime(ts, '%R') as hm,
              uniqExact(user_id) as message_users,
              count(user_id) as messages
            FROM
              simulator_20230320.message_actions
            WHERE
              ts >= today() - 1
              and ts < toStartOfFifteenMinutes(now())
            GROUP BY
              ts,
              date,
              hm
            ORDER BY
              ts
              '''
        return query(q)
    
    
    # отправка алерта
    @task
    def run_alert(df, metric, chat_id=None, a_a=None, n_a=None, th=None):
        
        context = get_current_context()
        ds = context['ds']
        
        if metric == 'CTR': 
            flag, value, diff = check_anomaly_pd(df, metric, threshold=th)
            if flag == 1:
                m = '[CTR]'
                alert = (f'ВНИМАНИЕ, АНОМАЛИЯ В МЕТРИКЕ {m}:\n\
текущее значение: {value}\n\
отклонение: {diff}%\n\
ссылка на график: http://superset.lab.karpov.courses/r/3551\n\
ссылка на дашборд: http://superset.lab.karpov.courses/r/3559\n\
@ivan_suchkov, обрати, пожалуйста, внимание')
                bot.sendMessage(chat_id=chat_id, text=alert)
            else: pass
        
        elif metric == 'feed_users':
            flag, value, diff = check_anomaly_quantile(df, metric, a=a_a, n=n_a)
            if flag == 1:
                m = '[количество пользователей ленты]'
                alert = (f'ВНИМАНИЕ, АНОМАЛИЯ В МЕТРИКЕ {m}:\n\
текущее значение: {value}\n\
отклонение: {diff}%\n\
ссылка на график: http://superset.lab.karpov.courses/r/3552\n\
ссылка на дашборд: http://superset.lab.karpov.courses/r/3559\n\
@ivan_suchkov, обрати, пожалуйста, внимание')
                bot.sendMessage(chat_id=chat_id, text=alert)
            else: pass
        
        elif metric == 'views':
            flag, value, diff = check_anomaly_quantile(df, metric, a=a_a, n=n_a)
            if flag == 1:
                m = '[количество просмотров]'
                alert = (f'ВНИМАНИЕ, АНОМАЛИЯ В МЕТРИКЕ {m}:\n\
текущее значение: {value}\n\
отклонение: {diff}%\n\
ссылка на график: http://superset.lab.karpov.courses/r/3553\n\
ссылка на дашборд: http://superset.lab.karpov.courses/r/3559\n\
@ivan_suchkov, обрати, пожалуйста, внимание')
                bot.sendMessage(chat_id=chat_id, text=alert)
            else: pass
        
        elif metric == 'likes':
            flag, value, diff = check_anomaly_quantile(df, metric, a=a_a, n=n_a)
            if flag == 1:
                m = '[количество лайков]'
                alert = (f'ВНИМАНИЕ, АНОМАЛИЯ В МЕТРИКЕ {m}:\n\
текущее значение: {value}\n\
отклонение: {diff}%\n\
ссылка на график: http://superset.lab.karpov.courses/r/3554\n\
ссылка на дашборд: http://superset.lab.karpov.courses/r/3559\n\
@ivan_suchkov, обрати, пожалуйста, внимание')
                bot.sendMessage(chat_id=chat_id, text=alert)
            else: pass        
        
        elif metric == 'message_users':
            flag, value, diff = check_anomaly_quantile(df, metric, a=a_a, n=n_a)
            if flag == 1:
                m = '[количество пользователей сообщений]'
                alert = (f'ВНИМАНИЕ, АНОМАЛИЯ В МЕТРИКЕ {m}:\n\
текущее значение: {value}\n\
отклонение: {diff}%\n\
ссылка на график: http://superset.lab.karpov.courses/r/3555\n\
ссылка на дашборд: http://superset.lab.karpov.courses/r/3559\n\
@ivan_suchkov, обрати, пожалуйста, внимание')
                bot.sendMessage(chat_id=chat_id, text=alert)
            else: pass     
        
        elif metric == 'messages':
            flag, value, diff = check_anomaly_quantile(df, metric, a=a_a, n=n_a)
            if flag == 1:
                m = '[количество сообщений]'
                alert = (f'ВНИМАНИЕ, АНОМАЛИЯ В МЕТРИКЕ {m}:\n\
текущее значение: {value}\n\
отклонение: {diff}%\n\
ссылка на график: http://superset.lab.karpov.courses/r/3558\n\
ссылка на дашборд: http://superset.lab.karpov.courses/r/3559\n\
@ivan_suchkov, обрати, пожалуйста, внимание')
                bot.sendMessage(chat_id=chat_id, text=alert)
            else: pass  
                
        return
    
    
    # соединяем функции
    feed = get_feed()
    messages = get_messages()
    run_alert(feed, 'CTR', chat_id=alert_chat_id, a_a=None, n_a=None, th=0.3)
    run_alert(feed, 'feed_users', chat_id=alert_chat_id, a_a=3, n_a=5, th=None)
    run_alert(feed, 'views', chat_id=alert_chat_id, a_a=3, n_a=5, th=None)
    run_alert(feed, 'likes', chat_id=alert_chat_id, a_a=3, n_a=5, th=None)
    run_alert(messages, 'message_users', chat_id=alert_chat_id, a_a=4, n_a=5, th=None)
    run_alert(messages, 'messages', chat_id=alert_chat_id, a_a=4, n_a=5, th=None)

# включаем даг    
i_suchkov_alert_dag = i_suchkov_alert_dag()