import requests
import logging
import time
import threading

logging.basicConfig(format='%(levelname)s: %(message)s',
                    level=logging.WARNING,
                    filename='output3.log',
                    filemode='w')

# 利用 threading.Semaphore 控制并发数量, 和 Java 中的并发工具 Semaphore 功能一样.
semaphore = threading.Semaphore(8)


def add_note(user_id, order_id, count):
    env = "prod"
    body = {
    }

    url = get_url(env) + '/api/{}/{}'.format(user_id, order_id)
    resp = requests.post(url, json=body, headers=get_header(env))
    if resp.status_code != 200:
        logging.error('--{}, failed to add note: {}, {}, status_code: {}, response content: {}'
                      .format(count, user_id, order_id, resp.status_code, resp.content))
    else:
        logging.warning('--{}, successfully add note: {}, {}'.format(count, user_id, order_id))
    # 接口请求结束, 释放 permit
    semaphore.release()


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
    count = 0
    with open("masterorders.csv", "r") as f:
        for line in f:
            count = count + 1
            line_array = line.split(",")
            order_id = line_array[0].replace('"', '')
            user_id = line_array[1].replace('"', '')
            print(count, order_id, user_id)
            # 限制 8 个线程一起执行
            if semaphore.acquire():
                threading.Thread(target=add_note, args=(user_id, order_id, count,)).start()


if __name__ == '__main__':
    start_ticks = round(time.time() * 1000)
    print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
    operate()
    print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
    print(str((round(time.time() * 1000) - start_ticks) / 1000) + "秒")
