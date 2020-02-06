import requests
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

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
    count = 0
    obj_list = []
    with ThreadPoolExecutor(max_workers=3) as executor:
        with open("/Users/pc/Pictures/testo.csv", "r") as f:
            for line in f:
                count = count + 1
                line_array = line.split(",")
                order_id = line_array[0].replace('"', '')
                user_id = line_array[1].replace('"', '')
                print(count, order_id, user_id)
                # add_note 方法, 参数: user_id, order_id, count
                obj = executor.submit(add_note, user_id, order_id, count)
                obj_list.append(obj)

        # executor.submit 是异步提交, 所以几万次 submit 0.3s 就结束了,
        # 利用 as_completed 方法 保证所有的 任务被执行完, future.result() 会阻塞未执行完的线程
        # (任务都提交了, 只有 3 个 worker , 未执行的任务是否放在 队列中? 暂未查证.)
        for future in as_completed(obj_list):
            future.result()


if __name__ == '__main__':
    start_ticks = round(time.time() * 1000)
    print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
    operate()
    print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
    print(str((round(time.time() * 1000) - start_ticks) / 1000) + "秒")
