import requests
import logging
import psycopg2

logging.basicConfig(format='%(levelname)s: %(message)s',
                    level=logging.WARNING,
                    filename='output.log',
                    filemode='w')


def add_note(user_id, order_id, count):
    env = "dev"
    body = {
    }
    url = get_url(env) + '/api/{}/{}'.format(user_id, order_id)
    resp = requests.post(url, json=body, headers=get_header(env))
    if resp.status_code != 200:
        logging.error('--{}, failed to add note: {}, {}, status_code: {}, response content: {}'
                      .format(count, user_id, order_id, resp.status_code, resp.content))
    else:
        logging.warning('--{}, successfully add note: {}, {}'.format(count, user_id, order_id))


def get_header(env_key):
    header_map = {'dev': {
        'Content-Type': 'application/json',
        'X-Session-User': 'x',
        'X-Session-Key': 'x'
    }, 'prod': {
        'Content-Type': 'application/json',
        'X-Session-User': 'x',
        'X-Session-Key': 'x'
    }}
    return header_map[env_key]


def get_url(env_key):
    url_map = {'dev': 'https://dev.xx.com',
               'prod': 'https://prod.xx.com'
               }
    return url_map[env_key]


def operate():
    conn = psycopg2.connect(host="127.0.0.1", port="3433", database="x", user="x", password="x")

    # Create a cursor object
    cur = conn.cursor()

    cur.execute("""select id, owner from orders o 
               where o.placed_at > 1575129600000 --2019-12-01 
               and (o.status = 'MERCHANT_PROCESSING' or o.status = 'SHIPPED')
               and o.error_details = 'OK' 
                limit 20000 offset 70000; """)

    # 这种遍历表的操作不应该写在脚本中, 正确的做法应该是 dump 到文件中 , 脚本直接读取文件
    query_results = cur.fetchall()
    count = 0
    for i in query_results:
        count = count + 1
        add_note(i[1], i[0], count)
    cur.close()
    conn.close()


if __name__ == '__main__':
    operate()
