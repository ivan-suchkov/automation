# coding=utf-8

from datetime import datetime, timedelta
import pandas as pd
import pandahouse as ph
from io import StringIO
import requests
from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


# дефолтные аргументы
default_args = {
                'owner': 'i-suchkov',
                'depends_on_past': False,
                'retries': 2,
                'retry_delay': timedelta(minutes=5),
                'start_date': datetime(2023, 4, 18), # во вторник ночью получаем данные за прошлый день (понедельник)
                }

# откуда получим данные
input_connection = {
                    'host': ###, #здесь было имя хоста
                    'database':###, #здесь было название бд
                    'user':###, #здесь было имя юзера
                    'password': ###, #здесь был пароль
                    }

# куда загрузим данные
output_connection = {
                     'host': ###, #здесь было имя хоста
                     'database': ###, #здесь было название бд
                     'user': ###, #здесь было имя юзера
                     'password': ###, #здесь был пароль
                    }

# расписание работы - каждый день в четыре часа ночи (учитывая, что в airflow время 3- от Мск)
schedule_interval = '0 4 * * *' 

# фунцкия чтения SQL запроса
def query(query, connection):
    """
    Функция принимает SQL запрос, сохраненный в переменную query и данные о подключении, сохраненные в переменную connection.
    Функция возвращает датафрейм пандас с таблицей, полученной в результате запроса. 
    """
    return ph.read_clickhouse(query, connection=connection)



# начало кода DAG
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def i_suchkov_dag():
    
    
    # в feed_actions для каждого юзера посчитаем число просмотров и лайков контента за вчера
    @task()
    def get_feed_actions(): 
        
        q = '''
            SELECT  user_id, 
                    age, 
                    gender, 
                    os, 
                    countIf(action='like') likes, 
                    countIf(action='view') views, 
                    toString(toDate(time)) event_date
            FROM {db}.feed_actions
            WHERE   toDate(time) = yesterday()
            GROUP BY user_id, event_date, age, gender, os
            '''
        df_feed_actions = query(q, input_connection)
        return df_feed_actions

    
    # В message_actions для каждого юзера считаем, сколько он получает и отсылает сообщений, скольким людям он пишет, сколько людей пишут ему
    @task()
    def get_message_actions():
        q2 = '''
             WITH 
                 sent as(
                         SELECT  user_id, 
                                 age, 
                                 gender, 
                                 os, 
                                 COUNT(reciever_id) messages_sent, 
                                 COUNT(DISTINCT reciever_id) users_sent, 
                                 toString(toDate(time)) event_date
                         FROM {db}.message_actions
                         WHERE   toDate(time) = yesterday()
                         GROUP BY user_id, event_date, age, gender, os
                         ),

                 recieved as(
                         SELECT  reciever_id receiver_id, 
                                 COUNT(user_id) messages_received, 
                                 COUNT(DISTINCT user_id) users_received
                         FROM {db}.message_actions
                         WHERE   toDate(time) = yesterday()
                         GROUP BY receiver_id
                         )

             SELECT * EXCEPT receiver_id 
             FROM sent s JOIN recieved r ON s.user_id = r.receiver_id
             '''
        df_message_actions = query(q2, input_connection)
        return df_message_actions
    
    
    # объединяем две таблицы в одну
    @task()
    def merge_feed_message(feed_actions, message_actions):
        df_merged = (
                    feed_actions
                    .merge(
                            message_actions,
                            how='outer',
                            on = ['user_id', 'age', 'gender', 'os', 'event_date']
                              )
                    ).fillna(0)
        return df_merged
    
    
    # далее считаем метрики в разрезе по полу, возрасту и ос. Делаем три разных таска на каждый срез.
    
    
    # срез по возрасту    
    @task()
    def age(df_merged):
        df_age = (
            df_merged
            .pivot_table(
                index=['age','event_date'],
                values=[
                    'likes',
                    'views',
                    'messages_received',
                    'messages_sent',
                    'users_sent',
                    'users_received'
                        ],
                aggfunc='sum'
                        )
            .reset_index()
        )

        def categorize_age(age): 
            '''
            Функция поможет категоризировать возраст
            '''
            if age < 18: 
                return '14-18'
            elif 18 <= age <= 25: 
                return '18-25'
            elif 26 <= age <= 30:
                return '26-30'
            elif 31 <= age <= 35: 
                return '31-35'
            elif 36 <= age <= 40: 
                return '36-40'
            elif 41 <= age <= 45: 
                return '41-45'
            elif 46 <= age <= 50: 
                return '46-50'
            elif 51 <= age <= 55:
                return '51-55'
            elif 56 <= age <= 65: 
                return '56-65'
            elif age > 65: 
                return '>65'

        df_age['age'] = df_age['age'].apply(categorize_age)

        # считаем внутри возрастных категорий
        df_age = (
            df_age
            .groupby(['age', 'event_date'])
            .sum()
            .reset_index()
            .rename(columns={'age':'dimension_value'})
        )
        df_age['dimension'] = 'age'

        return df_age
    
    
    # срез по полу
    @task()
    def gender(df_merged):
        df_gender = (
                        df_merged
                        .pivot_table(
                            index=['gender', 'event_date'],
                            values=[
                                'likes',
                                'views',
                                'messages_received',
                                'messages_sent',
                                'users_sent',
                                'users_received'
                                    ],
                            aggfunc='sum'
                                    )
                        .reset_index()
                        .rename(columns={'gender': 'dimension_value'})
                    )

        df_gender['dimension'] = 'gender'
        return df_gender
    
    
    # срез по ос
    @task()
    def os(df_merged):
        df_os = (
                    df_merged
                    .pivot_table(
                        index=['os', 'event_date'],
                        values=[
                            'likes',
                            'views',
                            'messages_received',
                            'messages_sent',
                            'users_sent',
                            'users_received'
                                ],
                        aggfunc='sum'
                                )
                    .reset_index()
                    .rename(columns={'os': 'dimension_value'})
                )

        df_os['dimension'] = 'os'
        return df_os
    
    
    # cобираем все срезы в одну таблицу 
    @task()
    def df_concat(df_age, df_gender, df_os):
        df_concatenated = pd.concat([df_age, df_gender, df_os])
        df_concatenated = (
            df_concatenated
            .astype({'likes':'int32',
                     'messages_received':'int32',
                     'messages_sent':'int32',
                     'users_received':'int32',
                     'users_sent':'int32',
                     'views':'int32'})
        )
        return df_concatenated
    
    
    # выгружаем данные в ClickHouse
    @task()
    def load(df_concatenated): 
        context = get_current_context()
        ds = context['ds']
        q3 = '''
            CREATE TABLE IF NOT EXISTS test.ivan_suchkov_2(
                event_date String,
                dimension String,
                dimension_value String,
                views Int32,
                likes Int32,
                messages_sent Int32,
                users_sent Int32,
                messages_received Int32,
                users_received Int32)
            ENGINE = MergeTree()
            ORDER BY event_date
             '''
        ph.execute(query=q3, connection=output_connection)
        ph.to_clickhouse(df=df_concatenated, table='ivan_suchkov_2', connection=output_connection, index=False)

    
    
    # подключаем таски
    feed_actions = get_feed_actions()
    message_actions = get_message_actions()
    df_merged = merge_feed_message(feed_actions, message_actions)
    df_age = age(df_merged)
    df_gender = gender(df_merged)
    df_os = os(df_merged)
    df_concatenated = df_concat(df_age, df_gender, df_os)
    load(df_concatenated)
    
    
    
# подключаем даг    
i_suchkov_dag = i_suchkov_dag()    