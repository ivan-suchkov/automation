import telegram
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import seaborn as sns
import io
import pandas as pd
import pandahouse as ph
from datetime import datetime, timedelta
from io import StringIO
import requests
from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
# from read_db.CH import Getch

# обозначаем бота
my_token = ###
bot = telegram.Bot(token=my_token) 

chat_id = ### общий чат, куда будем отправлять отчеты
# chat_id = ### мой персональный айди для проверки


# подключение к CH
connection = {'host': ###
              'database': ###,
              'user': ###, 
              'password': ###
                     }


# фунцкия чтения SQL запроса
def query(q):
    """
    Функция принимает SQL запрос, сохраненный в переменную query и данные о подключении, сохраненные в переменную connection.
    Функция возвращает датафрейм пандас с таблицей, полученной в результате запроса. 
    """
    return ph.read_clickhouse(q, connection=connection)


# дефолтные аргументы для дага
default_args = {
                'owner': 'i-suchkov',
                'depends_on_past': False,
                'retries': 2,
                'retry_delay': timedelta(minutes=5),
                'start_date': datetime(2023, 4, 21)
                }

# расписание работы для дага - каждый день в 11:00
schedule_interval = '0 11 * * *'


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def i_suchkov_dag_bot():
    
    
    # формируем текстовый отчет
    # текст с информацией о значениях ключевых метрик за предыдущий день:
    # DAU, просмотры, лайки CTR
    @task()
    def create_text_report():
        q0 = '''
        SELECT toString(yesterday()) ystd
             '''
        day = query(q0)['ystd'][0]

        q =  '''
        SELECT
          COUNT(DISTINCT user_id) DAU
        FROM
          simulator_20230320.feed_actions
        WHERE
          toDate(time) = yesterday()
        '''
        dau = query(q)['DAU'][0]

        q2 = '''
        SELECT
          countIf(user_id, action='like') likes
        FROM
          simulator_20230320.feed_actions
        WHERE
          toDate(time) = yesterday()
             '''
        likes = query(q2)['likes'][0]

        q3 = '''
        SELECT
          countIf(user_id, action='view') views
        FROM
          simulator_20230320.feed_actions
        WHERE
          toDate(time) = yesterday()
             '''
        views = query(q3)['views'][0]

        ctr = round(likes / views, 3)

        return (f'[Лента новостей]\n\
отчет за {day}:\n\
\n\
DAU: {dau}\n\
лайки: {likes}\n\
просмотры: {views}\n\
CTR: {ctr}')
 
    
    
    # формируем отчет для графиков
    # график с значениями метрик за предыдущие 7 дней:
    # DAU, просмотры, лайки CTR
    @task()
    def seven_days_metrics():
        q1w = '''
            SELECT
              COUNT(DISTINCT user_id) DAU,
              toDate(time) date
            FROM
              simulator_20230320.feed_actions
            WHERE
              toDate(time) BETWEEN (yesterday() - 6) AND yesterday()
            GROUP BY date
            '''
        dau_w = query(q1w)

        q2w = '''
            SELECT
              countIf(user_id, action='like') likes,
              toDate(time) date
            FROM
              simulator_20230320.feed_actions
            WHERE
              toDate(time) BETWEEN (yesterday() - 6) AND yesterday()
            GROUP BY date
            '''
        likes_w = query(q2w)

        q3w = '''
            SELECT
              countIf(user_id, action='view') views,
              toDate(time) date
            FROM
              simulator_20230320.feed_actions
            WHERE
              toDate(time) BETWEEN (yesterday() - 6) AND yesterday()
            GROUP BY date
            '''
        views_w = query(q3w)

        ctr_w = likes_w.merge(views_w, on='date')
        ctr_w['ctr'] = round(ctr_w['likes']/ctr_w['views'], 3)
        metrics_w = dau_w.merge(likes_w, on='date')
        metrics_w = metrics_w.merge(views_w, on='date')
        metrics_w['ctr'] = round(metrics_w['likes']/metrics_w['views'], 3)
        
        return metrics_w
        
    # отправляем в чат график и текстовый репорт в одном сообщении
    @task()
    def send_graph_text_report(metrics_w, text_report):
        
        context = get_current_context()
        ds = context['ds']

        fig = plt.figure(figsize=(16, 10))

        ax1 = plt.subplot(2, 2, 3)
        sns.lineplot(data=metrics_w, x="date", y="DAU", color='navy')
        plt.ylabel(' ')
        plt.xlabel(' ')
        plt.title('DAU')
        plt.xticks(rotation = 0)
        plt.ylim(15000, 20000)
        plt.grid(alpha=0.2)
        plt.tight_layout()

        ax2 = plt.subplot(2, 2, 1)
        sns.lineplot(data=metrics_w, x="date", y="views", color='teal')
        plt.ylabel(' ')
        plt.xlabel(' ')
        plt.title('просмотры')
        plt.xticks(rotation = 0)
        plt.ylim(0, 1000000)
        plt.grid(alpha=0.2)
        ax2.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{:,.0f}'.format(x/1000) + 'k'))
        plt.tight_layout()

        ax3 = plt.subplot(2, 2, 2)
        sns.lineplot(data=metrics_w, x="date", y="likes", color='coral')
        plt.ylabel(' ')
        plt.xlabel(' ')
        plt.title('лайки')
        plt.xticks(rotation = 0)
        plt.ylim(0, 500000)
        plt.grid(alpha=0.2)
        ax3.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{:,.0f}'.format(x/1000) + 'k'))
        plt.tight_layout()

        ax4 = plt.subplot(2, 2, 4)
        sns.lineplot(data=metrics_w, x="date", y="ctr", color='crimson')
        plt.ylabel(' ')
        plt.xlabel(' ')
        plt.title('CTR')
        plt.xticks(rotation = 0)
        plt.ylim(0, 0.3)
        plt.grid(alpha=0.2)
        plt.tight_layout()

        plt.subplots_adjust(top=0.9)
        fig.suptitle('динамика по ключевым метрикам за предыдущие 7 дней', y=0.98, fontsize=20)

        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = '7_days_metrics.png'
        plt.close()

        bot.sendPhoto(chat_id=chat_id, photo=plot_object, caption=text_report)
    
        return
    
    
    # подключаем таски
    text_report = create_text_report()
    metrics_w = seven_days_metrics()
    send_graph_text_report(metrics_w, text_report)


# подключаем даг
i_suchkov_dag_bot = i_suchkov_dag_bot()