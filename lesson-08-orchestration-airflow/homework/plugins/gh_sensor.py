from __future__ import annotations

import urllib.error
import urllib.request

from airflow.sensors.base import BaseSensorOperator


class GHArchiveSensor(BaseSensorOperator):
    def __init__(self, hour: int = 14, **kwargs) -> None:
        super().__init__(**kwargs)
        self.hour = hour

    def poke(self, context) -> bool:
        ds = context["ds"]
        url = f"https://data.gharchive.org/{ds}-{self.hour:02d}.json.gz"
        req = urllib.request.Request(
            url, method="HEAD", headers={"User-Agent": "gh-sensor/1.0"}
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                self.log.info("HEAD %s -> %s", url, resp.status)
                return resp.status == 200
        except Exception as exc:  # недоступний файл / мережеві помилки -> ще не готово
            self.log.info("HEAD %s failed: %s", url, exc)
            return False
