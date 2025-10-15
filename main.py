# 模块入口 main()
#!/usr/bin/python
# coding:utf-8

# @FileName:    main.py
# @Time:        2024/1/2 22:27
# @Author:      bubu
# @Project:     douyinLiveWebFetcher

import argparse
import json
import logging
import os
import threading
import time
from liveMan import DouyinLiveWebFetcher
from supabase_streamers import SupabaseStreamers
from supabase_writer import SupabaseBatchWriter

# 初始化基础日志配置
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s"
)
logger = logging.getLogger(__name__)


def run_room(live_id: str, writer, heartbeat_interval: float, anchor_name: str):
    """
    为指定直播间启动监控。
    """
    try:
        room = DouyinLiveWebFetcher(
            live_id,
            writer=writer,
            heartbeat_interval=heartbeat_interval,
            anchor_name=anchor_name
        )
        room.start()
    except Exception as e:
        logger.error(f"【X】启动直播间 {live_id} 监控时出错: {e}")


def parse_args():
    """
    解析命令行参数。
    """
    parser = argparse.ArgumentParser(description="监控抖音直播间（支持多个 live_id 并发）")
    parser.add_argument("--config", type=str, default="config.json", help="配置文件路径（默认 config.json）")
    parser.add_argument("--heartbeat-interval", type=float, default=None, help="心跳包发送间隔（秒），CLI 优先")
    # 新增：隐藏 liveMan 日志开关
    parser.add_argument("--hide-live-logs", action="store_true", help="不显示 liveMan 模块日志")
    return parser.parse_args()


def load_config(args):
    """
    加载配置，优先级：CLI > 配置文件 > 默认值。
    """
    config = {
        "heartbeat_interval": 5.0,
        "live_room_discovery_interval": 60.0
    }
    try:
        with open(args.config, "r", encoding="utf-8") as f:
            file_config = json.load(f)
            config.update(file_config)
    except FileNotFoundError:
        logger.warning(f"【!】配置文件 {args.config} 未找到，使用默认配置。")
    except json.JSONDecodeError:
        logger.error(f"【X】配置文件 {args.config} 格式错误。")

    if args.heartbeat_interval is not None:
        config["heartbeat_interval"] = args.heartbeat_interval

    return config


class LiveRoomManager:
    """
    管理直播间监控的生命周期。
    """

    def __init__(self, args, config):
        self.args = args
        self.config = config
        self.active_threads = {}
        self.writer = self._init_writer()
        self.streamer_loader = SupabaseStreamers.from_config(args.config)

    def _init_writer(self):
        """
        初始化 Supabase 写入器。
        """
        writer = SupabaseBatchWriter.from_file(self.args.config)
        if not writer:
            writer = SupabaseBatchWriter.from_env()
        if writer:
            writer.start()
            logger.info(f"【√】Supabase 写入器启用，表：{writer.table}")
        else:
            logger.warning("【!】未启用 Supabase 持久化（配置/环境缺失）")
        return writer

    def fetch_streamers(self):
        """
        从 Supabase 获取最新的直播间列表。
        """
        if not self.streamer_loader:
            logger.error("【X】无法初始化 SupabaseStreamers，无法获取直播间列表。")
            return {}
        return self.streamer_loader.fetch_streamer_map()

    def start_monitoring(self, live_id: str, anchor_name: str):
        """
        为新的直播间启动监控线程。
        """
        if live_id in self.active_threads and self.active_threads[live_id].is_alive():
            logger.debug(f"【!】直播间 {live_id} 监控已在运行中。")
            return

        logger.info(f"【+】启动新直播间监控：{anchor_name} ({live_id})")
        thread = threading.Thread(
            target=run_room,
            args=(live_id, self.writer, self.config["heartbeat_interval"], anchor_name),
            daemon=True,
            name=f"douyin-{live_id}"
        )
        thread.start()
        self.active_threads[live_id] = thread

    def manage_rooms(self):
        """
        主管理循环，动态监控和调整直播间。
        """
        try:
            while True:
                streamer_map = self.fetch_streamers()
                if not streamer_map:
                    logger.info("【...】未从 Supabase 获取到直播间列表，进入发现模式。")
                    time.sleep(self.config["live_room_discovery_interval"])
                    continue

                # 启动新房间的监控
                for live_id, anchor_name in streamer_map.items():
                    if live_id not in self.active_threads or not self.active_threads[live_id].is_alive():
                        self.start_monitoring(live_id, anchor_name)

                # 清理已结束的线程
                self.active_threads = {
                    lid: t for lid, t in self.active_threads.items() if t.is_alive()
                }

                # 判断是否进入发现模式
                if not self.active_threads:
                    logger.info(f"【...】所有直播间已关闭，进入发现模式，等待 {self.config['live_room_discovery_interval']} 秒...")
                    time.sleep(self.config["live_room_discovery_interval"])
                else:
                    # 在有活跃直播时，较短间隔轮询以快速发现新房间
                    time.sleep(10)

        except KeyboardInterrupt:
            logger.warning("【X】收到中断，正在退出...")
        finally:
            if self.writer:
                self.writer.stop()
            logger.info("【√】程序已退出。")


def main():
    """
    模块主入口。
    """
    args = parse_args()
    config = load_config(args)
    manager = LiveRoomManager(args, config)
    manager.manage_rooms()

if __name__ == '__main__':
    main()