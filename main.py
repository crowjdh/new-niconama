import requests
import sys
import websocket
from bs4 import BeautifulSoup
import os
import json
import threading
import time
import re
import multiprocessing as mp
from getpass import getpass


magic_offset = -20


def parse_message(message):
    message_dict = json.loads(message)
    return message_dict['type'], message_dict['body']


def now():
    return time.time()


def recv_or_none(socket):
    try:
        return socket.recv()
    except websocket.WebSocketTimeoutException:
        return None


def get_or_false(queue):
    try:
        return queue.get(timeout=1)
    except:
        return False


def ts_name_from_url(ts_url):
    match_result = re.compile(r'ts/([\d]*).ts').search(ts_url)
    ts_name = ts_url[match_result.start(1):match_result.end(1)]

    return ts_name


def start_time_from_name(ts_name):
    start_time_millisec = int(ts_name)

    return 0 if start_time_millisec < 1000 else start_time_millisec // 1000


def download(ts_queue, tasks, output_dir, is_done):
    pid = os.getpid()
    print("Waiting for items to download: {}".format(pid))
    while True:
        result = get_or_false(ts_queue)
        if result is False:
            continue
        ts_url = result
        if ts_url is None:
            print("None received", file=sys.stderr)
            is_done.value = 1
            break
        ts_name = ts_name_from_url(ts_url)
        if ts_name in tasks and tasks[ts_name] is True:
            print("Task {} already downloaded".format(ts_name), file=sys.stderr)
            continue
        tasks[ts_name] = False
        if not download_video(ts_url, ts_name, output_dir):
            print("Failed to download {}".format(ts_name), file=sys.stderr)
            continue
        print("Downloaded: {}.ts".format(ts_name))
        tasks[ts_name] = True
    print("Downloader stopped: {}".format(pid))


def download_video(ts_url, ts_name, output_dir):
    file_name = ts_name.rsplit('/', 1)[-1]
    file_path = os.path.join(output_dir, file_name + '.ts')

    with requests.get(ts_url) as response:
        if response.status_code != 200:
            return False
        with open(file_path, 'wb') as out_file:
            out_file.write(response.content)

        return True


class ConnectionRetainer(threading.Thread):
    def __init__(self, socket, interval, broadcast_id, event):
        threading.Thread.__init__(self)
        self.socket = socket
        self.interval = interval
        self.broadcast_id = broadcast_id
        self.prev_req_time = None
        self.event = event

    def run(self):
        self.prev_req_time = now()
        print("Wait for {} seconds before sending watch message".format(self.interval))
        while self.socket.connected and not self.event.is_set():
            if now() - self.prev_req_time < self.interval:
                time.sleep(0.1)
                continue

            self.prev_req_time += self.interval
            watch_message = '{type: "watch", body: {command: "watching", params: ["' + self.broadcast_id + '", "-1", "0"]}}'
            print("Sending watch: {}".format(watch_message))
            self.socket.send(watch_message)
            print("Wait for {} seconds before sending watch message".format(self.interval))
        print("{} stopped".format(self.__class__.__name__), file=sys.stderr)


class PingThread(threading.Thread):
    def __init__(self, socket, event):
        threading.Thread.__init__(self)
        self.socket = socket
        self.event = event

    def run(self):
        while self.socket.connected and not self.event.is_set():
            message = recv_or_none(self.socket)
            if message is None:
                time.sleep(1)
                continue
            message_type, message_body = parse_message(message)
            if message_type == 'ping':
                print("Sending pong...")
                self.socket.send('{type: "pong", body: {}}')
        print("{} stopped".format(self.__class__.__name__), file=sys.stderr)


class CommentSocketThread(threading.Thread):
    class Chat:
        def __init__(self, chat_json):
            self.no = chat_json['no']
            self.date = chat_json['date']
            self.vpos = chat_json['vpos']
            self.user_id = chat_json['user_id']
            self.content = chat_json['content']

    def __init__(self, message_server_uri, thread_id, user_id, open_time, waybackkey, cookies, event, output_dir):
        threading.Thread.__init__(self)
        self.message_server_uri = message_server_uri
        self.thread_id = thread_id
        self.user_id = user_id
        self.open_time = open_time
        self.waybackkey = waybackkey
        self.r = 0
        self.cookies = cookies
        self.event = event
        self.output_dir = output_dir
        self.chats = []

    def run(self):
        if os.path.isfile(self.chat_file_path):
            print("Chat file exists. Skipping...", file=sys.stderr)
            return
        headers = {
            'Connection': 'Upgrade',
            'Upgrade': 'websocket',
            'Sec-WebSocket-Version': '13',
            'Sec-WebSocket-Extensions': 'permessage-deflate; client_max_window_bits',
            'Sec-WebSocket-Protocol': 'msg.nicovideo.jp#json'
        }
        res_from, when = -200, self.open_time

        ws = websocket.create_connection(self.message_server_uri, header=headers)
        self.send_message(ws, when, res_from)
        ws.settimeout(1)

        chats = None
        while ws.connected and not self.event.is_set():
            message = recv_or_none(ws)
            if message is None:
                time.sleep(1)
                continue

            message_json = json.loads(message)
            if 'ping' in message_json:
                body = message_json['ping']
                content = str(body['content'])
                if content.startswith('rs'):
                    chats = []
                elif content.startswith('rf'):
                    self.extend_chats(chats)

                    disconnect_chats = [chat for chat in chats if chat.content == '/disconnect']
                    if len(disconnect_chats) > 0:
                        self.save_chats()
                        break
                    res_from = chats[-1].no + 1 if len(chats) > 0 else -200
                    when += 90

                    self.send_message(ws, when, res_from)
            elif 'chat' in message_json:
                chats.append(CommentSocketThread.Chat(message_json['chat']))
        print("{} stopped".format(self.__class__.__name__), file=sys.stderr)

    def send_message(self, ws, when, res_from):
        r = str(self.r)
        p = str(self.r * 5)
        ping_message = '[{"ping":{"content":"rs:' + r + '"}},{"ping":{"content":"ps:' + p + '"}},{"thread":' \
                       '{"thread":"' + self.thread_id + '","version":"20061206","fork":0,"when":' + str(when)\
                       + ',"user_id":"' + self.user_id + '","res_from":' + str(res_from)\
                       + ',"with_global":1,"scores":1,"nicoru":0,"waybackkey":"' + self.waybackkey + '"}},' \
                       '{"ping":{"content":"pf:' + p + '"}},{"ping":{"content":"rf:' + r + '"}}]'
        ws.send(ping_message)

        self.r += 1

    def extend_chats(self, chats):
        self.chats.extend(chats)

    def save_chats(self):
        with open(self.chat_file_path, 'w') as f:
            f.write('<?xml version="1.0" encoding="UTF-8"?><packet>')

            for chat in self.chats:
                message = '<chat no="{}" vpos="{}" date="{}" user_id="{}">{}</chat>'.format(
                    chat.no, chat.vpos, chat.date, chat.user_id, chat.content)
                f.write(message)

            f.write('</packet>')

    @property
    def chat_file_path(self):
        return os.path.join(self.output_dir, 'chats.txt')


class PlaylistLoader(threading.Thread):
    interval = 1

    def __init__(self, sess, master_url, start_time, event, ts_queue):
        threading.Thread.__init__(self)
        self.sess = sess
        self.master_url = master_url
        self.start_time = start_time
        self.prev_req_time = None
        self.event = event
        self.ts_queue = ts_queue

    def run(self):
        try:
            self._run()
        except ConnectionRefusedError:
            print("Failed to get playlist")
            self.event.set()
        print("{} stopped".format(self.__class__.__name__), file=sys.stderr)

    def _run(self):
        start_time = self.start_time

        self.prev_req_time = now()
        while True and not self.event.is_set():
            if now() - self.prev_req_time < PlaylistLoader.interval:
                time.sleep(0.1)
                continue

            master_url, playlist_url, playlist_base_url = self.parse_playlist(start_time)

            self.prev_req_time += PlaylistLoader.interval
            print("Requesting playlist from: {}".format(start_time))
            playlist = requests.get(playlist_url)
            if playlist.status_code != 200:
                raise ConnectionRefusedError()

            prog = re.compile(r'#EXTINF:([\d.]*),\n([^\n]*)\n')
            duration_and_ts_paths = prog.findall(playlist.text)
            has_ended = re.compile(r'#EXT-X-ENDLIST').search(playlist.text) is not None

            ts_urls = [playlist_base_url + ts_path for _, ts_path in duration_and_ts_paths]
            for ts_url in ts_urls:
                self.ts_queue.put(ts_url)

            last_ts_duration = float(duration_and_ts_paths[-1][0])
            ts_name = ts_name_from_url(ts_urls[-1])
            start_time = start_time_from_name(ts_name)
            start_time += last_ts_duration

            if has_ended:
                self.ts_queue.put(None)
                self.event.set()
                break

    def parse_playlist(self, start_time):
        master_url = self.master_url + '&start=' + str(start_time)

        master_m3u8_response = self.sess.get(master_url)
        if master_m3u8_response.status_code != 200:
            raise ConnectionRefusedError()
        playlist_paths = master_m3u8_response.text
        playlist_path = playlist_paths.split('\n')[-2]

        match_result = re.compile(r'(master.m3u8)').search(master_url)
        base_url = master_url[:match_result.start()]
        playlist_url = base_url + playlist_path

        match_result = re.compile(r'/ts/(playlist.m3u8)').search(playlist_url)
        playlist_base_url = playlist_url[:match_result.start(1)]

        return master_url, playlist_url, playlist_base_url


class MainThread(threading.Thread):
    def __init__(self, sess, video_info, output_dir, ts_queue, start_time, event):
        threading.Thread.__init__(self)
        self.sess = sess
        self.url = video_info['site']['relive']['webSocketUrl']
        self.broadcast_id = video_info['program']['broadcastId']
        self.user_id = video_info['user']['id']
        self.open_time = video_info['program']['openTime']
        self.output_dir = output_dir
        self.stream_info = {}
        self.ws = None
        self.ts_queue = ts_queue
        self.start_time = start_time
        self.event = event

    def run(self):
        # self.ws = websocket.create_connection(self.url, timeout=1)
        self.ws = websocket.create_connection(self.url)
        self.ws.settimeout(1)

        self.init()

        while True:
            message = recv_or_none(self.ws)
            if message is None:
                continue
            should_break = self.on_message(message)
            if should_break:
                break
        print("{} stopped".format(self.__class__.__name__), file=sys.stderr)

    def init(self):
        self.ws.send('{"type":"watch","body":{"command":"playerversion","params":["leo"]}}')
        self.ws.send('{"type":"watch","body":{"command":"getpermit","requirement":{"broadcastId":"' + self.broadcast_id +
                     '","route":"","stream":{"protocol":"hls","requireNewStream":true,"priorStreamQuality":"high",'
                     '"isLowLatency":true},"room":{"isCommentable":true,"protocol":"webSocket"}}}}')

    def fill_stream_info(self, message_body):
        if len(self.stream_info) >= 4:
            return
        if 'currentStream' in message_body:
            self.stream_info['master_m3u8'] = message_body['currentStream']['uri']
        elif 'command' in message_body and message_body['command'] == 'watchinginterval':
            self.stream_info['watching_interval'] = int(message_body['params'][0])
        elif 'command' in message_body and message_body['command'] == 'currentroom':
            self.stream_info['message_server_uri'] = message_body['room']['messageServerUri']
            self.stream_info['thread_id'] = message_body['room']['threadId']

    def try_to_start_threads(self):
        if len(self.stream_info) < 4:
            return False
        print(self.stream_info)
        master_url = self.stream_info['master_m3u8']

        wayback_url = 'http://watch.live.nicovideo.jp/api/getwaybackkey?thread={}'.format(self.stream_info['thread_id'])
        waybackkey = self.sess.get(wayback_url).text
        waybackkey = waybackkey[waybackkey.index('=')+1:]

        threads = []
        event = self.event

        def start_and_append(t):
            t.start()
            threads.append(t)

        comment_socket_thread = CommentSocketThread(
            self.stream_info['message_server_uri'], self.stream_info['thread_id'], self.user_id,
            self.open_time, waybackkey, self.sess.cookies, event, self.output_dir)
        connection_retainer = ConnectionRetainer(self.ws, self.stream_info['watching_interval'], self.broadcast_id, event)
        playlist_loader = PlaylistLoader(self.sess, master_url, self.start_time, event, self.ts_queue)
        ping_thread = PingThread(self.ws, event)

        start_and_append(comment_socket_thread)
        start_and_append(connection_retainer)
        start_and_append(playlist_loader)
        start_and_append(ping_thread)

        for thread in threads:
            thread.join()

        return True

    def on_message(self, message):
        print("Received: {}".format(message))
        message_type, message_body = parse_message(message)

        if message_type == 'ping':
            print("Sending pong...")
            self.ws.send('{type: "pong", body: {}}')
            return False
        self.fill_stream_info(message_body)
        threads_joined = self.try_to_start_threads()

        return threads_joined


def parse_argv(argv):
    if len(argv) < 4:
        file_name = os.path.basename(__file__)
        print("Usage: python {} ID PW lv123456789".format(file_name))
        sys.exit()
    _, n_id, n_pw, lv_id = argv

    return n_id, n_pw, lv_id


def get_args():
    n_id = input('E-mail: ')
    n_pw = getpass('Password: ')
    lv_id = input('lv id(lv123456789): ')

    return n_id, n_pw, lv_id


def ensure_output_directory(output_dir='output'):
    if os.path.isdir(output_dir):
        print("Directory already exists.")
        sys.exit(-1)
    os.mkdir(output_dir)

    return output_dir


def main(argv):
    n_id, n_pw, lv_id = get_args()
    # n_id, n_pw, lv_id = parse_argv(argv)
    output_dir = ensure_output_directory()
    with requests.session() as sess:
        if not login(sess, n_id, n_pw):
            return
        video_info = get_video_info(sess, lv_id)
        start = now()
        start_emulation(sess, video_info, output_dir)
        end = now()
        diff = end - start
        print("Time taken: {}".format(diff))


def get_video_info(sess, live_id):
    live_url = 'http://live2.nicovideo.jp/watch/{}'.format(live_id)
    response = sess.get(live_url)

    soup = BeautifulSoup(response.text, 'html.parser')
    script_tag = soup.select_one('script#embedded-data')

    return json.loads(script_tag['data-props'])


def login(sess, n_id, n_pw):
    login_url = "https://secure.nicovideo.jp/secure/login?site=niconico&mail={}&password={}".format(n_id, n_pw)
    response = sess.post(login_url)
    soup = BeautifulSoup(response.text, 'html.parser')

    return len(soup.select("div.notice.error")) <= 0


def get_start_time(tasks):
    if tasks is None or len(tasks) == 0:
        return 0
    undone_ts_names = []
    done_ts_names = []
    for ts_name, has_downloaded in tasks.items():
        ts_time = int(ts_name)
        if not has_downloaded:
            undone_ts_names.append(ts_time)
        else:
            done_ts_names.append(ts_time)
    # print("undone_ts_names: {}".format(undone_ts_names), file=sys.stderr)
    # print("done_ts_names: {}".format(done_ts_names), file=sys.stderr)
    if len(undone_ts_names) > 0:
        ts_name = sorted(undone_ts_names)[0]
    else:
        ts_name = sorted(done_ts_names)[-1]
    return start_time_from_name(ts_name)


def start_emulation(sess, video_info, output_dir):
    manager = mp.Manager()
    tasks = manager.dict()
    is_done = manager.Value('i', 0)

    while True:
        start_time = get_start_time(tasks)
        ts_queue = manager.Queue()
        error_event = threading.Event()

        pool = mp.Pool()
        for _ in range(os.cpu_count()):
            _ = pool.apply_async(download, (ts_queue, tasks, output_dir, is_done,))
        pool.close()

        print("Restarting from: {}".format(start_time), file=sys.stderr)

        main_thread = MainThread(sess, video_info, output_dir, ts_queue, start_time, error_event)
        main_thread.start()
        main_thread.join()

        should_break = is_done.value == 1
        if should_break:
            # If there was no error, wait for pool to download remaining videos
            for _ in range(os.cpu_count()):
                ts_queue.put(None)
            pool.join()
            break
        else:
            # If error has occurred, terminate pool immediately.
            pool.terminate()


if __name__ == '__main__':
    main(sys.argv)
