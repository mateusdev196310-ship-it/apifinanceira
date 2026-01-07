import os
import time
import json
try:
    import requests as _req
except Exception:
    _req = None
from app.config import DEEPSEEK_API_KEY

_cooldown_until = 0.0

def set_cooldown(seconds: int = 900):
    global _cooldown_until
    try:
        _cooldown_until = float(time.time()) + float(seconds or 900)
    except:
        _cooldown_until = float(time.time()) + 900.0

def is_available() -> bool:
    try:
        if not DEEPSEEK_API_KEY:
            return False
        return time.time() >= _cooldown_until
    except:
        return False

def _headers():
    try:
        if not DEEPSEEK_API_KEY:
            return None
        return {
            "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
            "Content-Type": "application/json",
        }
    except:
        return None

def generate_json(prompt: str, temperature: float = 0.1, max_tokens: int = 800, timeout: int = 25, system_instruction: str = ""):
    try:
        if _req is None:
            return None
        if not is_available():
            return None
        headers = _headers()
        if headers is None:
            return None
        body = {
            "model": "deepseek-chat",
            "messages": [
                {"role": "system", "content": system_instruction or "Responda em JSON válido sem texto extra."},
                {"role": "user", "content": prompt},
            ],
            "temperature": float(temperature or 0.1),
            "max_tokens": int(max_tokens or 800),
        }
        body["stream"] = False
        r = _req.post("https://api.deepseek.com/v1/chat/completions", headers=headers, json=body, timeout=timeout)
        if not getattr(r, "ok", False):
            try:
                if r.status_code == 429:
                    try:
                        set_cooldown(int(os.getenv("DEEPSEEK_COOLDOWN_SECONDS", "900") or "900"))
                    except:
                        set_cooldown(900)
            except:
                pass
            return None
        j = r.json()
        choices = j.get("choices") or []
        if not choices:
            return None
        content = str(((choices[0] or {}).get("message") or {}).get("content") or "").strip()
        if not content:
            return None
        return content
    except Exception:
        return None
