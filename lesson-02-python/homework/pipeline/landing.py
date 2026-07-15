"""Landing stage — idempotent download of the raw hour into the landing zone.

This module is GIVEN. You do not need to change it. It mirrors the lesson's
"landing zone" idea: the raw file is immutable, so a repeat run that finds the
file already present skips the download.
"""

from __future__ import annotations

import os
import urllib.request

from . import config


def land_raw_hour() -> str:
    """Download the configured GitHub Archive hour into the landing zone.

    Returns the local path. Idempotent: if the file already exists it is not
    re-downloaded.
    """
    os.makedirs(config.LANDING_DIR, exist_ok=True)
    if os.path.exists(config.LANDING_FILE):
        print(f"[landing] already present, skip: {os.path.basename(config.LANDING_FILE)}")
        return config.LANDING_FILE

    print(f"[landing] downloading {config.GH_URL} ...")
    # gharchive.org returns 403 to urllib's default User-Agent.
    req = urllib.request.Request(config.GH_URL, headers={"User-Agent": "de-course-l02/1.0"})
    with urllib.request.urlopen(req) as resp, open(config.LANDING_FILE, "wb") as out:
        while chunk := resp.read(1 << 20):
            out.write(chunk)
    size_mb = os.path.getsize(config.LANDING_FILE) / 1_000_000
    print(f"[landing] saved {os.path.basename(config.LANDING_FILE)} ({size_mb:.0f} MB)")
    return config.LANDING_FILE
