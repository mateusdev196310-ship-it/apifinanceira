from google import genai
from app.config import GEMINI_API_KEY, DEEPSEEK_API_KEY
from google.genai import types
import time
import os
import json
try:
    import requests as _req
except Exception:
    _req = None

_client = None
_cooldown_until = 0.0

def set_cooldown(seconds: int = 900):
    global _cooldown_until
    try:
        _cooldown_until = float(time.time()) + float(seconds or 900)
    except:
        _cooldown_until = float(time.time()) + 900.0

def is_available() -> bool:
    try:
        if not GEMINI_API_KEY and not DEEPSEEK_API_KEY:
            return False
        return time.time() >= _cooldown_until
    except:
        return False

def get_client():
    global _client
    try:
        if not is_available():
            return None
        if _client is not None:
            return _client
        if not GEMINI_API_KEY:
            return None
        _client = genai.Client(api_key=GEMINI_API_KEY)
        return _client
    except:
        return None

def _deepseek_headers():
    try:
        if not DEEPSEEK_API_KEY:
            return None
        return {
            "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
            "Content-Type": "application/json",
        }
    except:
        return None

def generate_json_deepseek(prompt: str, temperature: float = 0.1, max_tokens: int = 800, timeout: int = 25, system_instruction: str = ""):
    try:
        if _req is None:
            return None
        if not is_available():
            return None
        headers = _deepseek_headers()
        if headers is None:
            return None
        body = {
            "model": os.getenv("DEEPSEEK_MODEL") or "deepseek-chat",
            "messages": [
                {"role": "system", "content": system_instruction or "Responda em JSON válido sem texto extra."},
                {"role": "user", "content": prompt},
            ],
            "temperature": float(temperature or 0.1),
            "max_tokens": int(max_tokens or 800),
        }
        r = _req.post("https://api.deepseek.com/chat/completions", headers=headers, json=body, timeout=timeout)
        if not getattr(r, "ok", False):
            try:
                msg = f"{r.status_code}"
                if ("429" in msg) or ("Too Many Requests" in msg):
                    try:
                        set_cooldown(int(os.getenv("DEEPSEEK_COOLDOWN_SECONDS", "900") or "900"))
                    except:
                        set_cooldown(900)
                else:
                    pass
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

def sintetizar_descricao_curta(texto, categoria=None, forma=None):
    hint = ""
    if forma:
        hint = forma
    elif categoria == 'vendas':
        hint = "Venda de"
    elif categoria == 'salario':
        hint = "Salário"
    elif categoria == 'outros':
        hint = ""
    prompt = (
        f'TEXTO: "{texto}"\n'
        f'Reescreva em 3-7 palavras uma descrição financeira breve em português, concisa, sem números ou moeda. '
        f'Use capitalização adequada. Se for venda, comece com "Venda de"; se transferência, comece com "Transferência"; '
        f'se despesa, use "Gastos com". Corrija gramática e complete preposições. '
        f'Retorne somente a frase final.'
    )
    try:
        client = get_client()
        if client is not None:
            try:
                resposta = client.models.generate_content(
                    model='gemini-2.5-flash',
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        temperature=0.2,
                        max_output_tokens=64,
                    ),
                )
                txt = (getattr(resposta, "text", "") or "").strip()
                if not txt:
                    raise Exception("empty")
                txt = txt.replace("```json", "").replace("```", "").strip()
                return txt.splitlines()[0].strip()
            except:
                pass
        out = generate_json_deepseek(
            prompt,
            temperature=0.2,
            max_tokens=64,
            timeout=15,
            system_instruction="Responda apenas com uma frase curta em PT-BR, sem números ou moeda."
        ) or ""
        out = out.replace("```", "").strip()
        return out.splitlines()[0].strip() if out else None
    except:
        return None
