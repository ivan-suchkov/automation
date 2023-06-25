import telegram
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import seaborn as sns
import io
import pandas as pd
import pandahouse as ph
from datetime import datetime, timedelta, date
from io import StringIO
import requests
from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

# обозначаем бота
my_token = ###
bot = telegram.Bot(token=my_token) 

chat_id = ### ## общий чат, куда будем отправлять отчеты
# chat_id = ### ## мой персональный айди для проверки


# подключение к CH
connection = {'host': ###,
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
def i_suchkov_dag_bot_2_2():
    
    
    # формируем текстовый отчет
    # текст с информацией о значениях ключевых метрик за предыдущий день:
    # DAU, просмотры, лайки CTR
    @task()
    def create_text_report():
        q0 = '''
        SELECT toString(yesterday()) yesterday
        '''
        day = query(q0)['yesterday'][0]

        q = '''
        SELECT * FROM (
        SELECT
          toDate(time) as date,
          uniqExact(user_id) as feed_users,
          countIf(user_id, action = 'view') as views,
          countIf(user_id, action = 'like') as likes,
          ROUND(
            countIf(user_id, action = 'like') / countIf(user_id, action = 'view'),
            3
          ) AS CTR,
          ROUND((likes + views) / feed_users, 2) as events_per_user,

          ROUND(feed_users / neighbor(feed_users, -1), 3)*100 as vs_prev_day_users,
          ROUND(views / neighbor(views, -1), 3)*100 as vs_prev_day_views,
          ROUND(likes / neighbor(likes, -1), 3)*100 as vs_prev_day_likes,
          ROUND(CTR / neighbor(CTR, -1), 3)*100 as vs_prev_day_CTR,
          ROUND(events_per_user / neighbor(events_per_user, -1), 3)*100 as vs_prev_day_events_per_user,

          ROUND(feed_users / neighbor(feed_users, -7), 2)*100 as vs_prev_week_users,
          ROUND(views / neighbor(views, -7), 3)*100 as vs_prev_week_views,
          ROUND(likes / neighbor(likes, -7), 3)*100 as vs_prev_week_likes,
          ROUND(CTR / neighbor(CTR, -7), 3)*100 as vs_prev_week_CTR,
          ROUND(events_per_user / neighbor(events_per_user, -7), 3)*100 as vs_prev_week_events_per_user,

          ROUND(feed_users / neighbor(feed_users, -30), 3)*100 as vs_prev_month_users,
          ROUND(views / neighbor(views, -30), 3)*100 as vs_prev_month_views,
          ROUND(likes / neighbor(likes, -30), 3)*100 as vs_prev_month_likes,
          ROUND(CTR / neighbor(CTR, -30), 3)*100 as vs_prev_month_CTR,
          ROUND(events_per_user / neighbor(events_per_user, -30), 3)*100 as vs_prev_month_events_per_user

        FROM
          simulator_20230320.feed_actions
        WHERE
          date < today()
        GROUP BY
          date
        ORDER BY
          date)
        WHERE date = yesterday()
        '''
        feed_metrics = query(q)

        q3 = '''
        SELECT * FROM(
        SELECT first_date,
               count(users) new_users, 
               ROUND(new_users / neighbor(new_users, -1), 3)*100 as vs_prev_day_new_users,
               ROUND(new_users / neighbor(new_users, -7), 3)*100 as vs_prev_week_new_users,
               ROUND(new_users / neighbor(new_users, -30), 3)*100 as vs_prev_month_new_users
        FROM (
        SELECT DISTINCT user_id users, MIN(toDate(time)) OVER (PARTITION BY user_id) first_date
        FROM simulator_20230320.feed_actions)
        GROUP BY first_date
        ORDER BY first_date)
        WHERE first_date = yesterday()
        '''
        new_users = query(q3)

        q4 = '''
        SELECT * FROM (
        SELECT toDate(time) date,
               count(user_id) messages, 
               ROUND(messages / neighbor(messages, -1), 3)*100 as vs_prev_day_messages,
               ROUND(messages / neighbor(messages, -7), 3)*100 as vs_prev_week_messages,
               ROUND(messages / neighbor(messages, -30), 3)*100 as vs_prev_month_messages,
               count(distinct user_id) users,
               ROUND(users / neighbor(users, -1), 3)*100 as vs_prev_day_users,
               ROUND(users / neighbor(users, -7), 3)*100 as vs_prev_week_users,
               ROUND(users / neighbor(users, -30), 3)*100 as vs_prev_month_users,
               ROUND(messages / users, 1) as messages_per_user,
               ROUND(messages_per_user / neighbor(messages_per_user, -1), 3)*100 as vs_prev_day_messages_per_user,
               ROUND(messages_per_user / neighbor(messages_per_user, -7), 3)*100 as vs_prev_week_messages_per_user,
               ROUND(messages_per_user / neighbor(messages_per_user, -30), 3)*100 as vs_prev_month_messages_per_user
        FROM simulator_20230320.message_actions
        GROUP BY date)
        WHERE date = yesterday()
        '''
        message_users = query(q4)

        q6 = '''
        SELECT
          *
        FROM
          (
            SELECT
              date,
              total_users,
              ROUND(total_users / neighbor(total_users, -1), 3) * 100 as vs_prev_day_total_users,
              ROUND(total_users / neighbor(total_users, -7), 3) * 100 as vs_prev_week_total_users,
              ROUND(total_users / neighbor(total_users, -30), 3) * 100 as vs_prev_month_total_users
            FROM
              (
                SELECT
                  date,
                  COUNT(user_id) total_users
                FROM
                  (
                    SELECT
                      user_id,
                      toDate(time) date
                    FROM
                      simulator_20230320.feed_actions
                    UNION
                      DISTINCT
                    SELECT
                      user_id,
                      toDate(time) date
                    FROM
                      simulator_20230320.message_actions
                  )
                GROUP BY
                  date
              )
          )
        WHERE
          date = yesterday()
          '''
        total_users = query(q6)

        return (f"[APP REPORT FOR {day}]\n\
    \n\
    total users: {total_users['total_users'][0]} (vs prev day: {total_users['vs_prev_day_total_users'][0]}%, vs prev week: {total_users['vs_prev_week_total_users'][0]}%, vs prev month: {total_users['vs_prev_month_total_users'][0]}%)\n\
    new users: {new_users['new_users'][0]} (vs prev day: {new_users['vs_prev_day_new_users'][0]}%, vs prev week: {new_users['vs_prev_week_new_users'][0]}%, vs prev month: {new_users['vs_prev_month_new_users'][0]}%)\n\
    _______\n\
    [FEED]:\n\
    DAU: {feed_metrics['feed_users'][0]} (vs prev day: {feed_metrics['vs_prev_day_users'][0]}%, vs prev week: {feed_metrics['vs_prev_week_users'][0]}%, vs prev month: {feed_metrics['vs_prev_month_users'][0]}%)\n\
    views: {feed_metrics['views'][0]} (vs prev day: {feed_metrics['vs_prev_day_views'][0]}%, vs prev week: {feed_metrics['vs_prev_week_views'][0]}%, vs prev month: {feed_metrics['vs_prev_month_views'][0]}%)\n\
    likes: {feed_metrics['likes'][0]} (vs prev day: {feed_metrics['vs_prev_day_likes'][0]}%, vs prev week: {feed_metrics['vs_prev_week_likes'][0]}%, vs prev month: {feed_metrics['vs_prev_month_likes'][0]}%)\n\
    CTR: {feed_metrics['CTR'][0]} (vs prev day: {feed_metrics['vs_prev_day_CTR'][0]}%, vs prev week: {feed_metrics['vs_prev_week_CTR'][0]}%, vs prev month: {feed_metrics['vs_prev_month_CTR'][0]}%)\n\
    events per users: {feed_metrics['events_per_user'][0]} (vs prev day: {feed_metrics['vs_prev_day_events_per_user'][0]}%, vs prev week: {feed_metrics['vs_prev_week_events_per_user'][0]}%, vs prev month: {feed_metrics['vs_prev_month_events_per_user'][0]}%)\n\
    ___________\n\
    [MESSAGES]:\n\
    DAU: {message_users['users'][0]} (vs prev day: {message_users['vs_prev_day_users'][0]}%, vs prev week: {message_users['vs_prev_week_users'][0]}%, vs prev month: {message_users['vs_prev_month_users'][0]}%)\n\
    number of messages: {message_users['messages'][0]} (vs prev day: {message_users['vs_prev_day_messages'][0]}%, vs prev week: {message_users['vs_prev_week_messages'][0]}%, vs prev month: {message_users['vs_prev_month_messages'][0]}%)\n\
    messages per user: {message_users['messages_per_user'][0]} (vs prev day: {message_users['vs_prev_day_messages_per_user'][0]}%, vs prev week: {message_users['vs_prev_week_messages_per_user'][0]}%, vs prev month: {message_users['vs_prev_month_messages_per_user'][0]}%)")
    
        
    @task()
    def create_graph_df():
        q2 = '''
        SELECT
          toStartOfHour(time) as ts,
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
          simulator_20230320.feed_actions
        WHERE
          date >= yesterday() - 1 and date < today()
        GROUP BY
          ts,
          date,
          hm
        ORDER BY
          ts
          '''
        feed_hours = query(q2)
        
        q5 = '''
        SELECT
          toStartOfHour(time) as ts,
          toDate(ts) as date,
          formatDateTime(ts, '%R') as hm,
          uniqExact(user_id) as users,
          COUNT(user_id) as messages
        FROM
          simulator_20230320.message_actions
        WHERE
          date >= yesterday() - 1 and date < today()
        GROUP BY
          ts,
          date,
          hm
        ORDER BY
          ts
          '''
        messages_hours = query(q5)
        
        graph_df = feed_hours.merge(messages_hours[['ts', 'users', 'messages']], on='ts')
        
        return graph_df

    
    # отправляем в чат график и текстовый репорт в одном сообщении
    @task()
    def send_graph_text_report(metrics, text_report):

        context = get_current_context()
        ds = context['ds']

        yesterday = date.today() - timedelta(days = 1)
        day_before = date.today() - timedelta(days = 2)

        fig = plt.figure(figsize=(14, 16))

        ax1 = plt.subplot(3, 2, 1)
        sns.lineplot(data=metrics.query('date == @yesterday'), 
                     x="hm", 
                     y="feed_users", 
                     color='navy', 
                     label=yesterday.strftime('%Y-%m-%d'))
        sns.lineplot(data=metrics.query('date == @day_before'), 
                     x="hm", 
                     y="feed_users", 
                     color='navy', 
                     label=day_before.strftime('%Y-%m-%d'), 
                     alpha=0.5, 
                     linestyle='--')
        plt.ylabel(' ')
        plt.xlabel(' ')
        plt.title('users (feed)')
        plt.xticks(rotation = 0)
        plt.ylim(0, 3000)
        plt.grid(alpha=0.2)
        plt.legend()
        plt.tight_layout()
        for ind, label in enumerate(ax1.get_xticklabels()):
            if ind % 2 == 0:
                label.set_visible(True)
            else:
                label.set_visible(False)

        ax2 = plt.subplot(3, 2, 2)
        sns.lineplot(data=metrics.query('date == @yesterday'), 
                     x="hm", 
                     y="views", 
                     color='teal', 
                     label=yesterday.strftime('%Y-%m-%d'))
        sns.lineplot(data=metrics.query('date == @day_before'), 
                     x="hm", 
                     y="views", 
                     color='teal', 
                     label=day_before.strftime('%Y-%m-%d'), 
                     alpha=0.5, 
                     linestyle='--')
        plt.ylabel(' ')
        plt.xlabel(' ')
        plt.title('views')
        plt.xticks(rotation = 0)
        plt.ylim(5000, 50000)
        plt.grid(alpha=0.2)
        plt.legend()
        plt.tight_layout()
        for ind, label in enumerate(ax2.get_xticklabels()):
            if ind % 2 == 0:
                label.set_visible(True)
            else:
                label.set_visible(False)

        ax3 = plt.subplot(3, 2, 3)
        sns.lineplot(data=metrics.query('date == @yesterday'), 
                     x="hm", 
                     y="CTR", 
                     color='coral', 
                     label=yesterday.strftime('%Y-%m-%d'))
        sns.lineplot(data=metrics.query('date == @day_before'), 
                     x="hm", 
                     y="CTR", 
                     color='coral', 
                     label=day_before.strftime('%Y-%m-%d'), 
                     alpha=0.5, 
                     linestyle='--')
        plt.ylabel(' ')
        plt.xlabel(' ')
        plt.title('CTR')
        plt.xticks(rotation = 0)
        plt.ylim(0.1, 0.3)
        plt.grid(alpha=0.2)
        plt.legend()
        plt.tight_layout()
        for ind, label in enumerate(ax3.get_xticklabels()):
            if ind % 2 == 0:
                label.set_visible(True)
            else:
                label.set_visible(False)

        ax4 = plt.subplot(3, 2, 4)
        sns.lineplot(data=metrics.query('date == @yesterday'), 
                     x="hm", 
                     y="likes", 
                     color='crimson', 
                     label=yesterday.strftime('%Y-%m-%d'))
        sns.lineplot(data=metrics.query('date == @day_before'), 
                     x="hm", 
                     y="likes", 
                     color='crimson', 
                     label=day_before.strftime('%Y-%m-%d'), 
                     alpha=0.5, 
                     linestyle='--')
        plt.ylabel(' ')
        plt.xlabel(' ')
        plt.title('likes')
        plt.xticks(rotation = 0)
        plt.ylim(0, 15000)
        plt.grid(alpha=0.2)
        plt.legend()
        plt.tight_layout()
        for ind, label in enumerate(ax4.get_xticklabels()):
            if ind % 2 == 0:
                label.set_visible(True)
            else:
                label.set_visible(False)    

        ax5 = plt.subplot(3, 2, 5)
        sns.lineplot(data=metrics.query('date == @yesterday'), 
                     x="hm", 
                     y="users", 
                     color='grey', 
                     label=yesterday.strftime('%Y-%m-%d'))
        sns.lineplot(data=metrics.query('date == @day_before'), 
                     x="hm", 
                     y="users", 
                     color='grey', 
                     label=day_before.strftime('%Y-%m-%d'), 
                     alpha=0.8, 
                     linestyle='--')
        plt.ylabel(' ')
        plt.xlabel(' ')
        plt.title('users (messages)')
        plt.xticks(rotation = 0)
        plt.ylim(0, 500)
        plt.grid(alpha=0.2)
        plt.legend()
        plt.tight_layout()
        for ind, label in enumerate(ax5.get_xticklabels()):
            if ind % 2 == 0:
                label.set_visible(True)
            else:
                label.set_visible(False) 


        ax6 = plt.subplot(3, 2, 6)
        sns.lineplot(data=metrics.query('date == @yesterday'), 
                     x="hm", 
                     y="messages", 
                     color='green', 
                     label=yesterday.strftime('%Y-%m-%d'))
        sns.lineplot(data=metrics.query('date == @day_before'), 
                     x="hm", 
                     y="users", 
                     color='green', 
                     label=day_before.strftime('%Y-%m-%d'), 
                     alpha=0.5, 
                     linestyle='--')
        plt.ylabel(' ')
        plt.xlabel(' ')
        plt.title('sent messages')
        plt.xticks(rotation = 0)
        plt.ylim(0, 1000)
        plt.grid(alpha=0.2)
        plt.legend()
        plt.tight_layout()
        for ind, label in enumerate(ax6.get_xticklabels()):
            if ind % 2 == 0:
                label.set_visible(True)
            else:
                label.set_visible(False) 

        plt.subplots_adjust(top=0.9)
        fig.suptitle(f'динамика по ключевым метрикам за {yesterday}', y=0.97, fontsize=24)

        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'prev_day_metrics.png'
        plt.close()

        bot.sendPhoto(chat_id=chat_id, photo=plot_object, caption=text_report)
        return
    
    
    @task()
    def send_top_posts_csv():   
        q6 = '''
        SELECT post_id, 
               countIf(user_id, action = 'view') as views,
               countIf(user_id, action = 'like') as likes,
               ROUND(countIf(user_id, action = 'like') / countIf(user_id, action = 'view'), 3) AS CTR,
               count(DISTINCT user_id) reach
        FROM simulator_20230320.feed_actions
        WHERE toDate(time) = yesterday()
        GROUP BY post_id
        ORDER BY views DESC
        LIMIT 50
        '''
        q0 = '''
        SELECT toString(yesterday()) yesterday
        '''
        day = query(q0)['yesterday'][0]

        top_50_posts = query(q6)
        file_object = io.StringIO()
        top_50_posts.to_csv(file_object)
        file_object.name = f"top_50_posts({day}).csv"
        file_object.seek(0)
        bot.sendDocument(chat_id=chat_id, document=file_object, caption=f'⬆️ top-50 posts of {day} ⬆️')
        return
    
    @task()
    def send_top_users_csv():   
        q7 = '''
        SELECT *, (views+likes+messages_sent) as total_actions FROM 
        (SELECT DISTINCT user_id,
               countIf(user_id, action = 'view') as views,
               countIf(user_id, action = 'like') as likes,
               ROUND(countIf(user_id, action = 'like') / countIf(user_id, action = 'view'), 3) AS CTR
        FROM simulator_20230320.feed_actions
        WHERE toDate(time) = yesterday()
        GROUP by user_id) t1
        JOIN
        (SELECT DISTINCT user_id,
               COUNT(user_id) messages_sent
        FROM simulator_20230320.message_actions
        WHERE toDate(time) = yesterday()
        GROUP by user_id) t2 
        USING user_id
        ORDER BY total_actions DESC
        LIMIT 50
        '''
        top_50_users = query(q7)

        q0 = '''
        SELECT toString(yesterday()) yesterday
        '''
        day = query(q0)['yesterday'][0]

        file_object = io.StringIO()
        top_50_users.to_csv(file_object)
        file_object.name = f"top_50_users({day}).csv"
        file_object.seek(0)
        bot.sendDocument(chat_id=chat_id, document=file_object, caption=f'⬆️ top-50 users of {day} ⬆️')
        return
    
    
    # подключаем таски
    text_report = create_text_report()
    graph_df = create_graph_df()
    send_graph_text_report(graph_df, text_report)
    send_top_posts_csv()
    send_top_users_csv()


# подключаем даг
i_suchkov_dag_bot_2_2 = i_suchkov_dag_bot_2_2()