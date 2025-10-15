#!/usr/bin/python
# coding:utf-8

# @FileName:    liveMan.py
# @Time:        2024/1/2 21:51
# @Author:      bubu
# @Project:     douyinLiveWebFetcher

import codecs
import gzip
import hashlib
import random
import re
import string
import subprocess
import threading
import time
import execjs
import urllib.parse
from contextlib import contextmanager
from unittest.mock import patch

import requests
import websocket
from py_mini_racer import MiniRacer

from ac_signature import get__ac_signature
from protobuf.douyin import *

from urllib3.util.url import parse_url

# 顶层导入与模块级 logger
import logging
logger = logging.getLogger(__name__)

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
    except Exception as e:
        logger.exception("【X】生成签名失败: %s", e)
    
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
    
    def __init__(self, live_id, abogus_file='a_bogus.js', writer=None, heartbeat_interval: float = 5.0, anchor_name: str = None):
        """
        直播间弹幕抓取对象
        :param live_id: 直播间的直播id，打开直播间web首页的链接如：https://live.douyin.com/261378947940，
                        其中的261378947940即是live_id
        """
        self.abogus_file = abogus_file
        self.__ttwid = None
        self.__room_id = None
        self.session = requests.Session()
        self.live_id = live_id
        self.host = "https://www.douyin.com/"
        self.live_url = "https://live.douyin.com/"
        self.user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36 Edg/140.0.0.0"
        self.headers = {
            'User-Agent': self.user_agent
        }
        self.writer = writer
        self.logger = logger
        self.heartbeat_interval = float(heartbeat_interval)
        self.anchor_name = anchor_name
        self.writer = writer
    
    def start(self):
        self._connectWebSocket()
    
    def stop(self):
        self.ws.close()
    
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
            self.logger.error("【X】Request the live url error: %s", err)
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
            self.logger.error("【X】Request the live room url error: %s", err)
        else:
            match = re.search(r'roomId\\":\\"(\d+)\\"', response.text)
            if match is None or len(match.groups()) < 1:
                self.logger.error("【X】No match found for roomId")
            self.__room_id = match.group(1)
            return self.__room_id
    
    def get_ac_nonce(self):
        """
        获取 __ac_nonce
        """
        resp_cookies = self.session.get(self.host, headers=self.headers).cookies
        return resp_cookies.get("__ac_nonce")
    
    def get_ac_signature(self, __ac_nonce: str = None) -> str:
        """
        获取 __ac_signature
        """
        __ac_signature = get__ac_signature(self.host[8:], __ac_nonce, self.user_agent)
        self.session.cookies.set("__ac_signature", __ac_signature)
        return __ac_signature
    
    def get_a_bogus(self, url_params: dict):
        """
        获取 a_bogus
        """
        url = urllib.parse.urlencode(url_params)
        ctx = execute_js(self.abogus_file)
        _a_bogus = ctx.call("get_ab", url, self.user_agent)
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
        url = ('https://live.douyin.com/webcast/room/web/enter/?aid=6383'
               '&app_name=douyin_web&live_id=1&device_platform=web&language=zh-CN&enter_from=page_refresh'
               '&cookie_enabled=true&screen_width=5120&screen_height=1440&browser_language=zh-CN&browser_platform=Win32'
               '&browser_name=Edge&browser_version=140.0.0.0'
               f'&web_rid={self.live_id}'
               f'&room_id_str={self.room_id}'
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
        resp = self.session.get(url, headers=headers)
        data = resp.json().get('data')
        if data:
            room_status = data.get('room_status')
            user = data.get('user')
            user_id = user.get('id_str')
            nickname = user.get('nickname')
            self.logger.info("【%s】[%s]直播间：%s.", nickname, user_id, ['正在直播', '已结束'][bool(room_status)])
    
    def _connectWebSocket(self):
        """
        连接抖音直播间websocket服务器，请求直播间数据
        """
        wss = ("wss://webcast100-ws-web-lq.douyin.com/webcast/im/push/v2/?app_name=douyin_web"
               "&version_code=180800&webcast_sdk_version=1.0.14-beta.0"
               "&update_version_code=1.0.14-beta.0&compress=gzip&device_platform=web&cookie_enabled=true"
               "&screen_width=1536&screen_height=864&browser_language=zh-CN&browser_platform=Win32"
               "&browser_name=Mozilla"
               "&browser_version=5.0%20(Windows%20NT%2010.0;%20Win64;%20x64)%20AppleWebKit/537.36%20(KHTML,"
               "%20like%20Gecko)%20Chrome/126.0.0.0%20Safari/537.36"
               "&browser_online=true&tz_name=Asia/Shanghai"
               "&cursor=d-1_u-1_fh-7392091211001140287_t-1721106114633_r-1"
               f"&internal_ext=internal_src:dim|wss_push_room_id:{self.room_id}|wss_push_did:7319483754668557238"
               f"|first_req_ms:1721106114541|fetch_time:1721106114633|seq:1|wss_info:0-1721106114633-0-0|"
               f"wrds_v:7392094459690748497"
               f"&host=https://live.douyin.com&aid=6383&live_id=1&did_rule=3&endpoint=live_pc&support_wrds=1"
               f"&user_unique_id=7319483754668557238&im_path=/webcast/im/fetch/&identity=audience"
               f"&need_persist_msg_count=15&insert_task_id=&live_reason=&room_id={self.room_id}&heartbeatDuration=0")
        
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
            self.ws.run_forever()
        except Exception:
            self.stop()
            raise
    
    def _sendHeartbeat(self):
        """
        发送心跳包
        """
        while True:
            try:
                heartbeat = PushFrame(payload_type='hb').SerializeToString()
                self.ws.send(heartbeat, websocket.ABNF.OPCODE_PING)
                self.logger.debug("【√】发送心跳包")
            except Exception as e:
                self.logger.error("【X】心跳包检测错误: %s", e)
                break
            else:
                time.sleep(self.heartbeat_interval)
    
    def _wsOnOpen(self, ws):
        """
        连接建立成功
        """
        self.logger.info("【√】WebSocket连接成功. 主播:%s live_id:%s room_id:%s",
                         self.anchor_name or "未知", self.live_id, self.room_id)
        threading.Thread(target=self._sendHeartbeat).start()
    
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
        for msg in response.messages_list:
            method = msg.method
            try:
                {
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
                }.get(method)(msg.payload)
            except Exception:
                pass
    
    def _wsOnError(self, ws, error):
        self.logger.error("WebSocket error: %s [主播:%s live_id:%s room_id:%s]",
                          error, self.anchor_name or "未知", self.live_id, self.room_id)
    
    def _wsOnClose(self, ws, *args):
        self.get_room_status()
        self.logger.info("WebSocket connection closed. 主播:%s live_id:%s room_id:%s",
                         self.anchor_name or "未知", self.live_id, self.room_id)
    
    def _emit_event(self, event_type: str, data: dict):
        if not self.writer:
            return
        record = {
            "live_id": self.live_id,
            "room_id": self.room_id,
            "event_type": event_type,
            "data": data,
            "ts": int(time.time() * 1000),
        }
        self.writer.enqueue(record)

    def _parseChatMsg(self, payload):
        """聊天消息"""
        message = ChatMessage().parse(payload)
        user_name = message.user.nick_name
        user_id = message.user.id
        content = message.content
        self.logger.debug("【聊天msg】[%s]%s: %s", user_id, user_name, content)
        self._emit_event("chat", {"user_id": user_id, "user_name": user_name, "content": content})
    
    def _parseGiftMsg(self, payload):
        """礼物消息"""
        message = GiftMessage().parse(payload)
        user_name = message.user.nick_name
        gift_name = message.gift.name
        gift_cnt = message.combo_count
        self.logger.debug("【礼物msg】%s 送出了 %sx%s", user_name, gift_name, gift_cnt)
        self._emit_event("gift", {"user_name": user_name, "gift_name": gift_name, "gift_count": gift_cnt})
    
    def _parseLikeMsg(self, payload):
        '''点赞消息'''
        message = LikeMessage().parse(payload)
        user_name = message.user.nick_name
        count = message.count
        self.logger.debug("【点赞msg】%s 点了%s个赞", user_name, count)
        self._emit_event("like", {"user_name": user_name, "count": count})
    
    def _parseMemberMsg(self, payload):
        '''进入直播间消息'''
        message = MemberMessage().parse(payload)
        user_name = message.user.nick_name
        user_id = message.user.id
        gender = ["女", "男"][message.user.gender]
        self.logger.debug("【进场msg】[%s][%s]%s 进入了直播间", user_id, gender, user_name)
        self._emit_event("member_enter", {"user_id": user_id, "user_name": user_name, "gender": gender})
    
    def _parseSocialMsg(self, payload):
        '''关注消息'''
        message = SocialMessage().parse(payload)
        user_name = message.user.nick_name
        user_id = message.user.id
        self.logger.debug("【关注msg】[%s]%s 关注了主播", user_id, user_name)
        self._emit_event("follow", {"user_id": user_id, "user_name": user_name})
    
    def _parseRoomUserSeqMsg(self, payload):
        '''直播间统计'''
        message = RoomUserSeqMessage().parse(payload)
        current = message.total
        total = message.total_pv_for_anchor
        self.logger.debug("【统计msg】当前观看人数: %s, 累计观看人数: %s", current, total)
        self._emit_event("room_user_seq", {"current": current, "total": total})
    
    def _parseFansclubMsg(self, payload):
        '''粉丝团消息'''
        message = FansclubMessage().parse(payload)
        content = message.content
        self.logger.debug("【粉丝团msg】 %s", content)
        self._emit_event("fansclub", {"content": content})
    
    def _parseEmojiChatMsg(self, payload):
        '''聊天表情包消息'''
        message = EmojiChatMessage().parse(payload)
        emoji_id = message.emoji_id
        user = message.user
        common = message.common
        default_content = message.default_content
        self.logger.debug("【聊天表情包id】 %s,user：%s,common:%s,default_content:%s", emoji_id, user, common, default_content)
        self._emit_event("emoji_chat", {
            "emoji_id": emoji_id,
            "user": str(user),
            "common": str(common),
            "default_content": default_content,
        })
    
    def _parseRoomMsg(self, payload):
        message = RoomMessage().parse(payload)
        common = message.common
        room_id = common.room_id
        self.logger.debug("【直播间msg】直播间id:%s", room_id)
        self._emit_event("room", {"room_id": room_id})
    
    def _parseRoomStatsMsg(self, payload):
        message = RoomStatsMessage().parse(payload)
        display_long = message.display_long
        self.logger.debug("【直播间统计msg】%s", display_long)
        self._emit_event("room_stats", {"display_long": display_long})
    
    def _parseRankMsg(self, payload):
        message = RoomRankMessage().parse(payload)
        ranks_list = message.ranks_list
        self.logger.debug("【直播间排行榜msg】%s", ranks_list)
        self._emit_event("rank", {"ranks_list": ranks_list})
    
    def _parseControlMsg(self, payload):
        '''直播间状态消息'''
        message = ControlMessage().parse(payload)
        if message.status == 3:
            self.logger.info("直播间已结束")
            self.stop()
        self._emit_event("control", {"status": message.status})
    
    def _parseRoomStreamAdaptationMsg(self, payload):
        message = RoomStreamAdaptationMessage().parse(payload)
        adaptationType = message.adaptation_type
        self.logger.debug("直播间adaptation: %s", adaptationType)
        self._emit_event("room_stream_adaptation", {"adaptationType": adaptationType})
