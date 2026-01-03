from google import genai
from app.config import GEMINI_API_KEY
from google.genai import types
import time

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
        if not GEMINI_API_KEY:
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

def sintetizar_descricao_curta(texto, categoria=None, forma=None):
    client = get_client()
    if client is None:
        return None
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
            return None
        txt = txt.replace("```json", "").replace("```", "").strip()
        return txt.splitlines()[0].strip()
    except:
        return None
