#!/usr/bin/python
# coding:utf-8

# @FileName:    liveMan.py
# @Time:        2024/1/2 21:51
# @Author:      bubu
# @Project:     douyinLiveWebFetcher

import base64
import codecs
import gzip
import hashlib
import logging
import random
import re
import string
import subprocess
import threading
import time
import json
import execjs
import urllib.parse
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional
from unittest.mock import patch

import requests
import websocket
from py_mini_racer import MiniRacer
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib3.util.url import parse_url

from ac_signature import get__ac_signature
from config import MonitorConfig
from logging_config import get_logger
from protobuf.douyin import *

USER_AGENT_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:129.0) Gecko/20100101 Firefox/129.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
]

logger = get_logger("liveMan")


def execute_js(js_file: str):
    """
    执行 JavaScript 文件
    :param js_file: JavaScript 文件路径
    :return: 执行结果
    """
    with open(js_file, 'r', encoding='utf-8') as file:
        js_code = file.read()
    
    ctx = execjs.compile(js_code)
    return ctx


@contextmanager
def patched_popen_encoding(encoding='utf-8'):
    original_popen_init = subprocess.Popen.__init__
    
    def new_popen_init(self, *args, **kwargs):
        kwargs['encoding'] = encoding
        original_popen_init(self, *args, **kwargs)
    
    with patch.object(subprocess.Popen, '__init__', new_popen_init):
        yield


def generateSignature(wss, script_file='sign.js'):
    """
    出现gbk编码问题则修改 python模块subprocess.py的源码中Popen类的__init__函数参数encoding值为 "utf-8"
    """
    params = ("live_id,aid,version_code,webcast_sdk_version,"
              "room_id,sub_room_id,sub_channel_id,did_rule,"
              "user_unique_id,device_platform,device_type,ac,"
              "identity").split(',')
    wss_params = urllib.parse.urlparse(wss).query.split('&')
    wss_maps = {i.split('=')[0]: i.split("=")[-1] for i in wss_params}
    tpl_params = [f"{i}={wss_maps.get(i, '')}" for i in params]
    param = ','.join(tpl_params)
    md5 = hashlib.md5()
    md5.update(param.encode())
    md5_param = md5.hexdigest()
    
    with codecs.open(script_file, 'r', encoding='utf8') as f:
        script = f.read()
    
    ctx = MiniRacer()
    ctx.eval(script)
    
    try:
        signature = ctx.call("get_sign", md5_param)
        return signature
    except Exception:
        logger.exception("Failed to generate signature using MiniRacer")
    
    # 以下代码对应js脚本为sign_v0.js
    # context = execjs.compile(script)
    # with patched_popen_encoding(encoding='utf-8'):
    #     ret = context.call('getSign', {'X-MS-STUB': md5_param})
    # return ret.get('X-Bogus')


def generateMsToken(length=182):
    """
    产生请求头部cookie中的msToken字段，其实为随机的107位字符
    :param length:字符位数
    :return:msToken
    """
    random_str = ''
    base_str = string.ascii_letters + string.digits + '-_'
    _len = len(base_str) - 1
    for _ in range(length):
        random_str += base_str[random.randint(0, _len)]
    return random_str


class DouyinLiveWebFetcher:
    
    def __init__(
        self,
        live_id: str,
        *,
        abogus_file: str = 'a_bogus.js',
        streamer: Optional[Dict[str, Any]] = None,
        config: Optional[MonitorConfig] = None,
        event_sink: Optional[Callable[[Dict[str, Any]], None]] = None,
        stop_callback: Optional[Callable[[str], None]] = None,
    ):
        """
        直播间弹幕抓取对象
        :param live_id: 直播间的直播id，打开直播间web首页的链接如：https://live.douyin.com/261378947940，
                        其中的261378947940即是live_id
        """
        self.logger = logger.getChild(self.__class__.__name__)
        self.abogus_file = abogus_file
        self.streamer = streamer or {}
        self.config = config
        self.event_sink = event_sink
        self.stop_callback = stop_callback
        self.live_id = str(live_id)
        self.__ttwid: Optional[str] = None
        self.__room_id: Optional[str] = None
        self._stop_event = threading.Event()
        self._shutdown_notified = False
        self._heartbeats_thread: Optional[threading.Thread] = None
        self._room_status_cache: Optional[Dict[str, Any]] = None
        self.session = requests.Session()
        self._configure_session()
        self.host = "https://www.douyin.com/"
        self.live_url = "https://live.douyin.com/"
        self.user_agent = random.choice(USER_AGENT_POOL)
        self.headers = {
            'User-Agent': self.user_agent
        }
        self.session.headers.update(self.headers)
        self._delay_range = (
            (self.config.anti_crawl_min_delay, self.config.anti_crawl_max_delay)
            if self.config
            else (1.5, 4.0)
        )

    def _configure_session(self) -> None:
        retry = Retry(
            total=3,
            read=3,
            connect=3,
            backoff_factor=0.6,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=5, pool_maxsize=5)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
    
    def start(self):
        self._stop_event.clear()
        self._shutdown_notified = False
        delay = random.uniform(*self._delay_range)
        self.logger.debug("Anti-crawl delay before websocket connect | delay=%.2fs", delay)
        time.sleep(delay)
        self._connectWebSocket()
    
    def stop(self, reason: str = "manual_stop"):
        if self._stop_event.is_set():
            return
        self._stop_event.set()
        try:
            if hasattr(self, "ws") and self.ws:
                self.ws.close()
        except Exception:
            self.logger.exception("Error closing websocket connection")
        finally:
            self._notify_stopped(reason=reason)

    def _notify_stopped(self, reason: str) -> None:
        if self._shutdown_notified:
            return
        self._shutdown_notified = True
        if self.stop_callback:
            try:
                self.stop_callback(self.live_id, reason=reason)
            except TypeError:
                self.stop_callback(self.live_id)
            except Exception:
                self.logger.exception("Stop callback raised an exception")

    def _emit_event(self, event_type: str, payload: Any) -> None:
        if not self.event_sink:
            return
        now = datetime.now(timezone.utc)
        event = {
            "event_type": event_type,
            "payload": self._prepare_payload(payload),
            "live_id": self.live_id,
            "ts": int(now.timestamp() * 1000),
            "received_at": now.isoformat(),
        }
        if self.streamer:
            event["streamer_id"] = self.streamer.get("id")
            event["room_id"] = self.streamer.get("room_id") or self.streamer.get("live_id") or self.live_id
        try:
            self.event_sink(event)
        except Exception:
            self.logger.exception("Event sink failed | event_type=%s", event_type)

    def _prepare_payload(self, payload: Any) -> Any:
        if payload is None:
            return None
        if hasattr(payload, "to_json"):
            try:
                return json.loads(payload.to_json())
            except Exception:
                self.logger.exception("Failed to serialize message payload via to_json | type=%s", type(payload))
                return {}
        if isinstance(payload, (str, int, float, bool)):
            return payload
        if isinstance(payload, (bytes, bytearray)):
            return base64.b64encode(payload).decode("utf-8")
        if isinstance(payload, dict):
            return {k: self._prepare_payload(v) for k, v in payload.items()}
        if isinstance(payload, list):
            return [self._prepare_payload(item) for item in payload]
        if isinstance(payload, tuple):
            return tuple(self._prepare_payload(item) for item in payload)
        return str(payload)
    
    @property
    def ttwid(self):
        """
        产生请求头部cookie中的ttwid字段，访问抖音网页版直播间首页可以获取到响应cookie中的ttwid
        :return: ttwid
        """
        if self.__ttwid:
            return self.__ttwid
        headers = {
            "User-Agent": self.user_agent,
        }
        try:
            response = self.session.get(self.live_url, headers=headers)
            response.raise_for_status()
        except Exception as err:
            self.logger.error("Request live url error | err=%s", err)
        else:
            self.__ttwid = response.cookies.get('ttwid')
            return self.__ttwid
    
    @property
    def room_id(self):
        """
        根据直播间的地址获取到真正的直播间roomId，有时会有错误，可以重试请求解决
        :return:room_id
        """
        if self.__room_id:
            return self.__room_id
        url = self.live_url + self.live_id
        headers = {
            "User-Agent": self.user_agent,
            "cookie": f"ttwid={self.ttwid}&msToken={generateMsToken()}; __ac_nonce=0123407cc00a9e438deb4",
        }
        try:
            response = self.session.get(url, headers=headers)
            response.raise_for_status()
        except Exception as err:
            self.logger.error("Request live room url error | err=%s", err)
        else:
            match = re.search(r'roomId\\":\\"(\d+)\\"', response.text)
            if match is None or len(match.groups()) < 1:
                self.logger.warning("No match found for roomId | live_id=%s", self.live_id)
                return None
            self.__room_id = match.group(1)
            
            return self.__room_id
    
    def get_ac_nonce(self):
        """
        获取 __ac_nonce
        """
        try:
            response = self.session.get(self.host, headers=self.headers, timeout=10)
            response.raise_for_status()
        except Exception as err:
            self.logger.error("Failed to fetch __ac_nonce | err=%s", err)
            return None
        return response.cookies.get("__ac_nonce")
    
    def get_ac_signature(self, __ac_nonce: str = None) -> str:
        """
        获取 __ac_signature
        """
        if not __ac_nonce:
            self.logger.warning("__ac_nonce missing while generating signature | live_id=%s", self.live_id)
        __ac_signature = get__ac_signature(self.host[8:], __ac_nonce, self.user_agent)
        self.session.cookies.set("__ac_signature", __ac_signature)
        return __ac_signature
    
    def get_a_bogus(self, url_params: dict):
        """
        获取 a_bogus
        """
        url = urllib.parse.urlencode(url_params)
        try:
            ctx = execute_js(self.abogus_file)
            _a_bogus = ctx.call("get_ab", url, self.user_agent)
        except Exception as err:
            self.logger.exception("Failed to generate a_bogus | err=%s", err)
            raise
        return _a_bogus
    
    def get_room_status(self):
        """
        获取直播间开播状态:
        room_status: 2 直播已结束
        room_status: 0 直播进行中
        """
        msToken = generateMsToken()
        nonce = self.get_ac_nonce()
        signature = self.get_ac_signature(nonce)
        current_room_id = self.room_id
        if not current_room_id:
            self.logger.warning("Unable to resolve room_id for status check | live_id=%s", self.live_id)
            return None
        url = ('https://live.douyin.com/webcast/room/web/enter/?aid=6383'
               '&app_name=douyin_web&live_id=1&device_platform=web&language=zh-CN&enter_from=page_refresh'
               '&cookie_enabled=true&screen_width=5120&screen_height=1440&browser_language=zh-CN&browser_platform=Win32'
               '&browser_name=Edge&browser_version=140.0.0.0'
               f'&web_rid={self.live_id}'
               f'&room_id_str={current_room_id}'
               '&enter_source=&is_need_double_stream=false&insert_task_id=&live_reason=&msToken=' + msToken)
        query = parse_url(url).query
        params = {i[0]: i[1] for i in [j.split('=') for j in query.split('&')]}
        a_bogus = self.get_a_bogus(params)  # 计算a_bogus,成功率不是100%，出现失败时重试即可
        url += f"&a_bogus={a_bogus}"
        headers = self.headers.copy()
        headers.update({
            'Referer': f'https://live.douyin.com/{self.live_id}',
            'Cookie': f'ttwid={self.ttwid};__ac_nonce={nonce}; __ac_signature={signature}',
        })
        try:
            resp = self.session.get(url, headers=headers, timeout=10)
            resp.raise_for_status()
            payload = resp.json()
        except Exception as err:
            self.logger.error("Failed to fetch room status | live_id=%s err=%s", self.live_id, err)
            return None

        data = (payload or {}).get('data')
        if not data:
            self.logger.debug("Room status response missing data | live_id=%s", self.live_id)
            return None

        room_status = data.get('room_status')
        user = data.get('user') or {}
        user_id = user.get('id_str')
        nickname = user.get('nickname')
        status_label = ['正在直播', '已结束'][bool(room_status)]

        result = {
            "room_status": room_status,
            "user_id": user_id,
            "nickname": nickname,
            "raw": data,
        }
        self._room_status_cache = result
        self.logger.info(
            "Room status | nickname=%s user_id=%s status=%s",
            nickname,
            user_id,
            status_label,
        )
        self._emit_event("room_status", result)
        return result

    def is_streaming(self) -> bool:
        status = self.get_room_status()
        if not status:
            return False
        return status.get("room_status") == 0
    
    def _connectWebSocket(self):
        """
        连接抖音直播间websocket服务器，请求直播间数据
        """
        if self._stop_event.is_set():
            self.logger.debug("Stop event set before websocket connect; aborting connect.")
            return
        room_id = self.room_id
        if not room_id:
            self.logger.error("Cannot start websocket without room_id | live_id=%s", self.live_id)
            self._notify_stopped(reason="missing_room_id")
            return

        user_unique_id = ''.join(random.choice(string.digits) for _ in range(18))

        wss = ("wss://webcast100-ws-web-lq.douyin.com/webcast/im/push/v2/?app_name=douyin_web"
               "&version_code=180800&webcast_sdk_version=1.0.14-beta.0"
               "&update_version_code=1.0.14-beta.0&compress=gzip&device_platform=web&cookie_enabled=true"
               "&screen_width=1536&screen_height=864&browser_language=zh-CN&browser_platform=Win32"
               "&browser_name=Mozilla"
                "&browser_version=5.0%20(Windows%20NT%2010.0;%20Win64;%20x64)%20AppleWebKit/537.36%20(KHTML,"
               "%20like%20Gecko)%20Chrome/126.0.0.0%20Safari/537.36"
               "&browser_online=true&tz_name=Asia/Shanghai"
               "&cursor=d-1_u-1_fh-7392091211001140287_t-1721106114633_r-1"
               f"&internal_ext=internal_src:dim|wss_push_room_id:{room_id}|wss_push_did:{user_unique_id}"
               f"|first_req_ms:1721106114541|fetch_time:1721106114633|seq:1|wss_info:0-1721106114633-0-0|"
               f"wrds_v:7392094459690748497"
               f"&host=https://live.douyin.com&aid=6383&live_id=1&did_rule=3&endpoint=live_pc&support_wrds=1"
               f"&user_unique_id={user_unique_id}&im_path=/webcast/im/fetch/&identity=audience"
               f"&need_persist_msg_count=15&insert_task_id=&live_reason=&room_id={room_id}&heartbeatDuration=0")
        
        signature = generateSignature(wss)
        wss += f"&signature={signature}"
        
        headers = {
            "cookie": f"ttwid={self.ttwid}",
            'user-agent': self.user_agent,
        }
        self.ws = websocket.WebSocketApp(wss,
                                         header=headers,
                                         on_open=self._wsOnOpen,
                                         on_message=self._wsOnMessage,
                                         on_error=self._wsOnError,
                                         on_close=self._wsOnClose)
        try:
            self._emit_event("connection_attempt", {"live_id": self.live_id})
            self.ws.run_forever(ping_interval=0, skip_utf8_validation=True)
        except Exception as err:
            self.logger.exception("WebSocket run_forever error | live_id=%s err=%s", self.live_id, err)
            self._notify_stopped(reason="ws_exception")
            raise
    
    def _sendHeartbeat(self):
        """
        发送心跳包
        """
        while not self._stop_event.is_set():
            try:
                heartbeat = PushFrame(payload_type='hb').SerializeToString()
                if self.ws:
                    self.ws.send(heartbeat, websocket.ABNF.OPCODE_PING)
                self.logger.debug("Heartbeat sent")
            except Exception as e:
                self.logger.error("Heartbeat error | err=%s", e)
                self._notify_stopped(reason="heartbeat_error")
                break
            else:
                wait_seconds = 5 + random.uniform(0.5, 1.5)
                self._stop_event.wait(wait_seconds)
    
    def _wsOnOpen(self, ws):
        """
        连接建立成功
        """
        self.logger.info("WebSocket connection established")
        self._emit_event("connection_open", {"live_id": self.live_id})
        if not self._heartbeats_thread or not self._heartbeats_thread.is_alive():
            self._heartbeats_thread = threading.Thread(target=self._sendHeartbeat, daemon=True)
            self._heartbeats_thread.start()
    
    def _wsOnMessage(self, ws, message):
        """
        接收到数据
        :param ws: websocket实例
        :param message: 数据
        """
        
        # 根据proto结构体解析对象
        package = PushFrame().parse(message)
        response = Response().parse(gzip.decompress(package.payload))
        
        # 返回直播间服务器链接存活确认消息，便于持续获取数据
        if response.need_ack:
            ack = PushFrame(log_id=package.log_id,
                            payload_type='ack',
                            payload=response.internal_ext.encode('utf-8')
                            ).SerializeToString()
            ws.send(ack, websocket.ABNF.OPCODE_BINARY)
        
        # 根据消息类别解析消息体
        handlers = {
            'WebcastChatMessage': self._parseChatMsg,  # 聊天消息
            'WebcastGiftMessage': self._parseGiftMsg,  # 礼物消息
            'WebcastLikeMessage': self._parseLikeMsg,  # 点赞消息
            'WebcastMemberMessage': self._parseMemberMsg,  # 进入直播间消息
            'WebcastSocialMessage': self._parseSocialMsg,  # 关注消息
            'WebcastRoomUserSeqMessage': self._parseRoomUserSeqMsg,  # 直播间统计
            'WebcastFansclubMessage': self._parseFansclubMsg,  # 粉丝团消息
            'WebcastControlMessage': self._parseControlMsg,  # 直播间状态消息
            'WebcastEmojiChatMessage': self._parseEmojiChatMsg,  # 聊天表情包消息
            'WebcastRoomStatsMessage': self._parseRoomStatsMsg,  # 直播间统计信息
            'WebcastRoomMessage': self._parseRoomMsg,  # 直播间信息
            'WebcastRoomRankMessage': self._parseRankMsg,  # 直播间排行榜信息
            'WebcastRoomStreamAdaptationMessage': self._parseRoomStreamAdaptationMsg,  # 直播间流配置
        }

        for msg in response.messages_list:
            method = msg.method
            handler = handlers.get(method)
            if not handler:
                self.logger.debug("Unhandled message method | method=%s", method)
                continue
            try:
                handler(msg.payload)
            except Exception:
                self.logger.exception("Failed to handle message | method=%s", method)
    
    def _wsOnError(self, ws, error):
        self.logger.error("WebSocket error | err=%s", error)
        self._emit_event("connection_error", {"live_id": self.live_id, "error": str(error)})
    
    def _wsOnClose(self, ws, *args):
        self.logger.info("WebSocket connection closed | args=%s", args)
        self._emit_event("connection_closed", {"live_id": self.live_id, "args": args})
        self._stop_event.set()
        if self._heartbeats_thread and self._heartbeats_thread.is_alive():
            self._heartbeats_thread.join(timeout=5)
        self.get_room_status()
        self._notify_stopped(reason="ws_closed")
    
    def _parseChatMsg(self, payload):
        """聊天消息"""
        message = ChatMessage().parse(payload)
        user_name = message.user.nick_name
        user_id = message.user.id
        content = message.content
        self.logger.info("Chat message | user_id=%s user_name=%s content=%s", user_id, user_name, content)
        self._emit_event("chat", message)
    
    def _parseGiftMsg(self, payload):
        """礼物消息"""
        message = GiftMessage().parse(payload)
        user_name = message.user.nick_name
        gift_name = message.gift.name
        gift_cnt = message.combo_count
        self.logger.info("Gift message | user_name=%s gift=%s count=%s", user_name, gift_name, gift_cnt)
        self._emit_event("gift", message)
    
    def _parseLikeMsg(self, payload):
        '''点赞消息'''
        message = LikeMessage().parse(payload)
        user_name = message.user.nick_name
        count = message.count
        self.logger.info("Like message | user_name=%s count=%s", user_name, count)
        self._emit_event("like", message)
    
    def _parseMemberMsg(self, payload):
        '''进入直播间消息'''
        message = MemberMessage().parse(payload)
        user_name = message.user.nick_name
        user_id = message.user.id
        gender_map = {
            0: "未知",
            1: "男",
            2: "女",
        }
        gender_value = message.user.gender
        gender = gender_map.get(gender_value, "未知")
        self.logger.info("Member join | user_id=%s user_name=%s gender=%s", user_id, user_name, gender)
        self._emit_event("member", message)
    
    def _parseSocialMsg(self, payload):
        '''关注消息'''
        message = SocialMessage().parse(payload)
        user_name = message.user.nick_name
        user_id = message.user.id
        self.logger.info("Social follow | user_id=%s user_name=%s", user_id, user_name)
        self._emit_event("social", message)
    
    def _parseRoomUserSeqMsg(self, payload):
        '''直播间统计'''
        message = RoomUserSeqMessage().parse(payload)
        current = message.total
        total = message.total_pv_for_anchor
        self.logger.info("Room stats | current=%s total=%s", current, total)
        self._emit_event("room_user_seq", message)
    
    def _parseFansclubMsg(self, payload):
        '''粉丝团消息'''
        message = FansclubMessage().parse(payload)
        content = message.content
        self.logger.info("Fansclub message | content=%s", content)
        self._emit_event("fansclub", message)
    
    def _parseEmojiChatMsg(self, payload):
        '''聊天表情包消息'''
        message = EmojiChatMessage().parse(payload)
        emoji_id = message.emoji_id
        user = message.user
        common = message.common
        default_content = message.default_content
        self.logger.info(
            "Emoji chat | emoji_id=%s user=%s common=%s default_content=%s",
            emoji_id,
            user,
            common,
            default_content,
        )
        self._emit_event("emoji_chat", message)
    
    def _parseRoomMsg(self, payload):
        message = RoomMessage().parse(payload)
        common = message.common
        room_id = common.room_id
        self.logger.info("Room info | room_id=%s", room_id)
        self._emit_event("room_info", message)
    
    def _parseRoomStatsMsg(self, payload):
        message = RoomStatsMessage().parse(payload)
        display_long = message.display_long
        self.logger.info("Room stats summary | display_long=%s", display_long)
        self._emit_event("room_stats", message)
    
    def _parseRankMsg(self, payload):
        message = RoomRankMessage().parse(payload)
        ranks_list = message.ranks_list
        self.logger.info("Room rank | ranks_list=%s", ranks_list)
        self._emit_event("room_rank", message)
    
    def _parseControlMsg(self, payload):
        '''直播间状态消息'''
        message = ControlMessage().parse(payload)
        self._emit_event("control", message)
        
        if message.status == 3:
            self.logger.warning("Room ended signal received")
            self.stop(reason="room_ended")
    
    def _parseRoomStreamAdaptationMsg(self, payload):
        message = RoomStreamAdaptationMessage().parse(payload)
        adaptationType = message.adaptation_type
        self.logger.debug("Room stream adaptation | adaptation_type=%s", adaptationType)
        self._emit_event("stream_adaptation", message)
