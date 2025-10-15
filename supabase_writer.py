import os
import time
import threading
import queue
from typing import Optional, List, Dict

try:
    from supabase import create_client
except Exception:
    create_client = None
import logging
import requests
from datetime import datetime


class SupabaseBatchWriter:
    def __init__(
        self,
        supabase_url: str,
        supabase_key: str,
        table: str,
        batch_size: int = 50,
        flush_interval: float = 2.0,
        queue_maxsize: int = 10000,
        use_rest: bool = True,
        schema: str = "public",
        prefer_client: bool = False,
    ):
        # 不强制要求 supabase 库存在，失败自动走 REST
        self.table = table
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.queue = queue.Queue(maxsize=queue_maxsize)
        self.supabase_url = supabase_url.rstrip("/")  # 规范化，避免双斜杠
        self.supabase_key = supabase_key
        self.schema = schema

        self.client = None
        self.use_rest = True
        if prefer_client:
            if create_client:
                try:
                    self.client = create_client(supabase_url, supabase_key)
                    self.use_rest = False
                except Exception as e:
                    print(f"【!】Supabase 客户端创建失败，改用 REST: {e}")
                    self.client = None
                    self.use_rest = True
            else:
                print("【!】未安装 supabase 库，改用 REST 模式")
                self.use_rest = True

        self._stop_evt = threading.Event()
        self._worker_thread = threading.Thread(
            target=self._worker, name="supabase-writer", daemon=True
        )
        self.logger = logging.getLogger(__name__)

    @staticmethod
    def from_env() -> Optional["SupabaseBatchWriter"]:
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_KEY")
        if not url or not key:
            return None
        table = os.getenv("SUPABASE_TABLE", "douyin_events")
        batch_size = int(os.getenv("SUPABASE_BATCH_SIZE", "50"))
        flush_interval = float(os.getenv("SUPABASE_FLUSH_INTERVAL", "1.0"))
        queue_maxsize = int(os.getenv("SUPABASE_QUEUE_MAXSIZE", "10000"))
        prefer_client = os.getenv("SUPABASE_DRIVER", "rest").lower() == "client"
        schema = os.getenv("SUPABASE_SCHEMA", "public")
        try:
            return SupabaseBatchWriter(
                url,
                key,
                table,
                batch_size,
                flush_interval,
                queue_maxsize,
                prefer_client=prefer_client,
                schema=schema,
            )
        except Exception as e:
            print(f"【X】Supabase 写入器初始化失败: {e}")
            return None

    @staticmethod
    def from_file(path: str) -> Optional["SupabaseBatchWriter"]:
        import json
        try:
            with open(path, "r", encoding="utf-8") as f:
                cfg = json.load(f)
            url = cfg.get("supabase_url")
            key = cfg.get("supabase_key")
            if not url or not key:
                print("【X】配置文件缺少 supabase_url 或 supabase_key")
                return None
            table = cfg.get("supabase_table", "douyin_events")
            batch_size = int(cfg.get("supabase_batch_size", 50))
            flush_interval = float(cfg.get("supabase_flush_interval", 1.0))
            queue_maxsize = int(cfg.get("supabase_queue_maxsize", 10000))
            prefer_client = str(cfg.get("driver", "rest")).lower() == "client"
            schema = str(cfg.get("schema", "public"))
            return SupabaseBatchWriter(
                url,
                key,
                table,
                batch_size,
                flush_interval,
                queue_maxsize,
                prefer_client=prefer_client,
                schema=schema,
            )
        except FileNotFoundError:
            print(f"【!】未找到配置文件: {path}")
            return None
        except Exception as e:
            print(f"【X】读取配置文件失败: {e}")
            return None

    def start(self):
        if not self._worker_thread.is_alive():
            self._worker_thread.start()
            self.logger.info("【√】Supabase 批量写入器已启动（模式：%s）", 'REST' if self.use_rest else 'Client')

    def stop(self):
        self._stop_evt.set()
        # 放一个空元素唤醒 get 阻塞
        try:
            self.queue.put_nowait(None)
        except Exception:
            pass
        self._worker_thread.join(timeout=5)
        # 尝试刷写残留数据
        self._flush_remaining()

    def enqueue(self, record: Dict):
        try:
            self.queue.put_nowait(record)
        except queue.Full:
            self.logger.warning("【X】Supabase 队列已满，丢弃记录")

    def _worker(self):
        batch: List[Dict] = []
        last_flush = time.time()

        while not self._stop_evt.is_set():
            timeout = max(0.0, self.flush_interval - (time.time() - last_flush))
            try:
                item = self.queue.get(timeout=timeout if timeout > 0 else 0.01)
            except queue.Empty:
                item = None

            if item is None:
                # 可能是 stop 唤醒或超时
                if batch:
                    self._flush_batch(batch)
                    batch = []
                    last_flush = time.time()
                continue

            batch.append(item)

            need_flush = len(batch) >= self.batch_size
            time_reached = (time.time() - last_flush) >= self.flush_interval

            if need_flush or time_reached:
                self._flush_batch(batch)
                batch = []
                last_flush = time.time()

    def _flush_batch(self, batch: List[Dict]):
        if not batch:
            return
        # 统一清洗为可 JSON 序列化的 payload
        payload = [self._sanitize_record(r) for r in batch]
        if not self.use_rest and self.client:
            try:
                self.client.table(self.table).insert(payload).execute()
                return
            except Exception as e:
                self.logger.warning("【!】Supabase 客户端写入失败，尝试 REST: %s", e)
        self._insert_via_rest(payload)

    # 将任意对象转换为可 JSON 序列化的形式
    def _jsonable(self, obj):
        try:
            import enum
            from dataclasses import is_dataclass, asdict
            if obj is None or isinstance(obj, (bool, int, float, str)):
                return obj
            if isinstance(obj, bytes):
                import base64
                return base64.b64encode(obj).decode("ascii")
            if isinstance(obj, (list, tuple, set)):
                return [self._jsonable(v) for v in obj]
            if isinstance(obj, dict):
                return {str(k): self._jsonable(v) for k, v in obj.items()}
            if isinstance(obj, enum.Enum):
                return obj.value
            to_dict = getattr(obj, "to_dict", None)
            if callable(to_dict):
                try:
                    return self._jsonable(to_dict())
                except Exception:
                    pass
            if is_dataclass(obj):
                return self._jsonable(asdict(obj))
            return str(obj)
        except Exception as e:
            # 兜底：不可转换对象转为 repr
            self.logger.debug("【!】_jsonable 兜底转换: %s", e)
            return repr(obj)

    def _sanitize_record(self, rec: Dict) -> Dict:
        ts_ms = rec.get("ts")
        try:
            local_tz = datetime.now().astimezone().tzinfo
            if ts_ms is None:
                ts_ms = int(time.time() * 1000)
            local_dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=local_tz)
            created_at = local_dt.isoformat(timespec="milliseconds")
            updated_at = created_at
        except Exception:
            created_at = None
            updated_at = None

        sanitized = {
            "live_id": rec.get("live_id"),
            "room_id": str(rec.get("room_id")) if rec.get("room_id") is not None else None,
            "event_type": rec.get("event_type"),
            "data": self._jsonable(rec.get("data")),
            "ts": ts_ms,
        }
        # 仅对 douyin_events 表写入 created_at / updated_at
        if self.table == "douyin_events":
            sanitized["created_at"] = created_at
            sanitized["updated_at"] = updated_at
        return sanitized

    def _insert_via_rest(self, batch: List[Dict]):
        if not batch:
            return
        url = f"{self.supabase_url}/rest/v1/{self.table}"
        headers = {
            "apikey": self.supabase_key,
            "Authorization": f"Bearer {self.supabase_key}",
            "Content-Type": "application/json",
            "Content-Profile": self.schema,
            "Prefer": "return=minimal",
        }
        # batch 已是清洗后的 payload
        try:
            resp = requests.post(url, json=batch, headers=headers, timeout=10)
            if resp.status_code >= 300:
                self.logger.error("【X】REST 写入失败: %s %s (table=%s, schema=%s)", resp.status_code, resp.text, self.table, self.schema)
        except Exception as e:
            self.logger.error("【X】REST 写入异常: %s", e)

    def _flush_remaining(self):
        remaining = []
        while not self.queue.empty():
            try:
                item = self.queue.get_nowait()
                if item is not None:
                    remaining.append(item)
            except Exception:
                break
        if remaining:
            self._flush_batch(remaining)