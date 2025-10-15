import json
import logging
import requests

logger = logging.getLogger(__name__)


class SupabaseStreamers:
    def __init__(self, supabase_url: str, supabase_key: str, schema: str = "public", table: str = "douyin_streamers", timeout: int = 10):
        self.url = str(supabase_url).rstrip("/")
        self.key = supabase_key
        self.schema = schema
        self.table = table
        self.timeout = timeout
        self.logger = logger

    @classmethod
    def from_config(cls, path: str = "config.json") -> "SupabaseStreamers | None":
        try:
            with open(path, "r", encoding="utf-8") as f:
                cfg = json.load(f)
            url = cfg.get("supabase_url")
            key = cfg.get("supabase_key")
            schema = str(cfg.get("schema", "public"))
            table = str(cfg.get("streamers_table", "douyin_streamers"))
            if not url or not key:
                logger.error("【X】配置缺少 supabase_url/supabase_key，无法初始化 SupabaseStreamers")
                return None
            return cls(url, key, schema=schema, table=table)
        except FileNotFoundError:
            logger.error("【X】未找到配置文件: %s", path)
            return None
        except Exception as e:
            logger.error("【X】读取配置文件失败: %s", e)
            return None

    def fetch_live_ids(self) -> list[str]:
        headers = {
            "apikey": self.key,
            "Authorization": f"Bearer {self.key}",
            "Accept-Profile": self.schema,
        }
        api = f"{self.url}/rest/v1/{self.table}?select=live_id"
        try:
            resp = requests.get(api, headers=headers, timeout=self.timeout)
            if resp.status_code >= 300:
                self.logger.error("【X】查询 Supabase 失败: %s %s", resp.status_code, resp.text)
                return []
            data = resp.json()
            ids = [str(item.get("live_id")).strip() for item in data if item.get("live_id")]
            unique_ids = sorted(set([i for i in ids if i]))
            if not unique_ids:
                self.logger.warning("【!】Supabase 表 %s 无 live_id 记录", self.table)
            else:
                self.logger.info("【√】从 Supabase 获取 %d 个 live_id（表：%s）", len(unique_ids), self.table)
            return unique_ids
        except Exception as e:
            self.logger.error("【X】Supabase 获取 live_ids 异常: %s", e)
            return []

    def fetch_streamer_map(self) -> dict:
        # 返回 { live_id: anchor_name } 映射，用于日志标注主播名称
        headers = {
            "apikey": self.key,
            "Authorization": f"Bearer {self.key}",
            "Accept-Profile": self.schema,
        }
        api = f"{self.url}/rest/v1/{self.table}?select=live_id,anchor_name"
        try:
            resp = requests.get(api, headers=headers, timeout=self.timeout)
            if resp.status_code >= 300:
                self.logger.error("【X】查询 Supabase 失败: %s %s", resp.status_code, resp.text)
                return {}
            data = resp.json()
            result = {}
            for item in data:
                lid = str(item.get("live_id")).strip() if item.get("live_id") else None
                name = str(item.get("anchor_name")).strip() if item.get("anchor_name") else None
                if lid:
                    result[lid] = name or ""
            if not result:
                self.logger.warning("【!】Supabase 表 %s 无 live_id/anchor_name 记录", self.table)
            else:
                self.logger.info("【√】从 Supabase 获取 %d 个主播映射（表：%s）", len(result), self.table)
            return result
        except Exception as e:
            self.logger.error("【X】Supabase 获取主播映射异常: %s", e)
            return {}