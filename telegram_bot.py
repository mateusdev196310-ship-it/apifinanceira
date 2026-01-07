# arquivo: bot_financeiro_formatado.py
import requests
import json
import os
import logging
import re
from datetime import datetime, timedelta, timezone, time as dt_time
import calendar
import asyncio
import difflib
import unicodedata
from collections import defaultdict
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, CallbackContext, CallbackQueryHandler
from app.config import TELEGRAM_BOT_TOKEN, api_url
from urllib.parse import quote_plus
from types import SimpleNamespace
import atexit
import tempfile
from app.utils.formatting import (
    formatar_moeda,
    criar_linha_tabela,
    criar_cabecalho,
    criar_secao,
    wrap_code_block,
)
from app.constants.categories import CATEGORY_NAMES, CATEGORY_LIST
from app.services.image_extractor import extrair_informacoes_da_imagem
from app.services.database import salvar_transacao_cliente, ensure_cliente, get_db, firestore, get_categoria_memoria, atualizar_memoria_categoria
from time import time as _now_ts
from app.services.gemini import is_available as _gemini_ok
from app.services.gemini import sintetizar_descricao_curta
from app.services.rule_based import parse_value
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None
_TZ_SP = None
try:
    if ZoneInfo is not None:
        _TZ_SP = ZoneInfo("America/Sao_Paulo")
except Exception:
    _TZ_SP = None
_BOT_LOCK_PATH = os.path.join(os.getcwd(), ".bot.lock")
_BOT_LOCK_ACQUIRED = False
def _acquire_bot_lock():
    global _BOT_LOCK_ACQUIRED
    try:
        if os.getenv("BOT_FORCE_RUN") or os.getenv("BOT_DISABLE_LOCK"):
            _BOT_LOCK_ACQUIRED = True
            return True
        ttl = int(os.getenv("BOT_LOCK_TTL") or "900")
        if os.path.exists(_BOT_LOCK_PATH):
            try:
                st = os.stat(_BOT_LOCK_PATH)
                age = int(_now_ts() - st.st_mtime)
                if age > ttl:
                    os.remove(_BOT_LOCK_PATH)
                else:
                    pid = None
                    try:
                        with open(_BOT_LOCK_PATH, "r", encoding="utf-8") as f2:
                            pid = int(str(f2.read()).strip() or "0")
                    except:
                        pid = None
                    alive = False
                    if pid and pid > 0:
                        try:
                            os.kill(pid, 0)
                            alive = True
                        except:
                            alive = False
                    if alive:
                        return False
                    else:
                        try:
                            os.remove(_BOT_LOCK_PATH)
                        except:
                            pass
            except:
                try:
                    os.remove(_BOT_LOCK_PATH)
                except:
                    pass
        f = open(_BOT_LOCK_PATH, "x")
        try:
            f.write(str(os.getpid()))
            f.flush()
        except:
            pass
        _BOT_LOCK_ACQUIRED = True
        return True
    except:
        return False
def _release_bot_lock():
    global _BOT_LOCK_ACQUIRED
    try:
        if _BOT_LOCK_ACQUIRED and os.path.exists(_BOT_LOCK_PATH):
            os.remove(_BOT_LOCK_PATH)
        _BOT_LOCK_ACQUIRED = False
    except:
        pass
atexit.register(_release_bot_lock)

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

# ⚠️ SUBSTITUA ESTE TOKEN!
API_URL = api_url()
_CACHE = {}
def _cache_get(k):
    e = _CACHE.get(k)
    if not e:
        return None
    if e["exp"] < _now_ts():
        return None
    return e["val"]
def _cache_set(k, v, ttl=15):
    _CACHE[k] = {"val": v, "exp": _now_ts() + ttl}
def _req_json(url, timeout=4):
    try:
        return requests.get(url, timeout=timeout).json()
    except:
        return {}
def _req_json_cached(url, key, ttl=15, timeout=4):
    v = _cache_get(key)
    if v is not None:
        return v
    d = _req_json(url, timeout=timeout)
    _cache_set(key, d, ttl)
    return d
async def _req_json_async(url, timeout=4):
    loop = asyncio.get_event_loop()
    def _call():
        try:
            return requests.get(url, timeout=timeout).json()
        except:
            return {}
    return await loop.run_in_executor(None, _call)
async def _req_json_cached_async(url, key, ttl=15, timeout=4):
    v = _cache_get(key)
    if v is not None:
        return v
    d = await _req_json_async(url, timeout=timeout)
    _cache_set(key, d, ttl)
    return d

def _now_sp():
    try:
        now_utc = datetime.now(timezone.utc)
        try:
            tz = ZoneInfo("America/Sao_Paulo") if ZoneInfo else None
        except Exception:
            tz = None
        return now_utc.astimezone(tz) if tz is not None else (now_utc + timedelta(hours=-3))
    except:
        return datetime.now()

def _day_key_sp():
    try:
        return _now_sp().strftime("%Y-%m-%d")
    except:
        return datetime.now().strftime("%Y-%m-%d")

def _month_key_sp():
    try:
        return _now_sp().strftime("%Y-%m")
    except:
        return datetime.now().strftime("%Y-%m")

_BG_LIMIT = int(os.getenv("BOT_MAX_CONCURRENCY", "2") or "2")
_bg_semaphore = asyncio.Semaphore(_BG_LIMIT if _BG_LIMIT > 0 else 1)
async def _post_json_async(url, payload=None, timeout=10):
    loop = asyncio.get_event_loop()
    def _call():
        try:
            r = requests.post(url, json=payload, timeout=timeout)
            try:
                return r.json()
            except:
                return {}
        except:
            return {}
    return await loop.run_in_executor(None, _call)

async def mini_report(cliente_id: str, cliente_nome: str = None, username: str = None):
    try:
        dkey = _day_key_sp()
        mkey = _month_key_sp()
        qs = f"cliente_id={quote_plus(str(cliente_id))}"
        if cliente_nome:
            qs += f"&cliente_nome={quote_plus(str(cliente_nome))}"
        if username:
            qs += f"&username={quote_plus(str(username))}"
        day_url = f"{API_URL}/saldo/atual?inicio={dkey}&fim={dkey}&{qs}"
        month_url = f"{API_URL}/saldo/atual?mes={mkey}&{qs}"
        geral_url = f"{API_URL}/saldo/atual?{qs}"
        day_api, month_api, geral_api = await asyncio.gather(
            _req_json_cached_async(day_url, f"day:{cliente_id}:{dkey}", ttl=10, timeout=4),
            _req_json_cached_async(month_url, f"month:{cliente_id}:{mkey}", ttl=15, timeout=4),
            _req_json_cached_async(geral_url, f"geral:{cliente_id}", ttl=20, timeout=4),
        )
        receitas_dia = float(((day_api or {}).get("total") or {}).get("receitas", 0) or 0)
        despesas_dia = float(((day_api or {}).get("total") or {}).get("despesas", 0) or 0)
        saldo_dia = float(((day_api or {}).get("total") or {}).get("saldo", receitas_dia - despesas_dia) or (receitas_dia - despesas_dia))
        m_tot = (month_api or {}).get("total") or {}
        receitas_mes = float(m_tot.get("receitas", 0) or 0)
        despesas_mes = float(m_tot.get("despesas", 0) or 0)
        saldo_mes = float(m_tot.get("saldo", receitas_mes - despesas_mes) or (receitas_mes - despesas_mes))
        if (receitas_mes == 0 and despesas_mes == 0 and float(m_tot.get("ajustes", 0) or 0) == 0 and float(m_tot.get("estornos", 0) or 0) == 0):
            try:
                recompute_url = f"{API_URL}/recompute/cliente?{qs}"
                await _req_json_async(recompute_url, timeout=6)
                month_api = await _req_json_cached_async(month_url, f"month:{cliente_id}:{mkey}:fix", ttl=8, timeout=4)
                m_tot = (month_api or {}).get("total") or {}
                receitas_mes = float(m_tot.get("receitas", 0) or 0)
                despesas_mes = float(m_tot.get("despesas", 0) or 0)
                saldo_mes = float(m_tot.get("saldo", receitas_mes - despesas_mes) or (receitas_mes - despesas_mes))
            except:
                pass
        saldo_final = float(((geral_api or {}).get("total") or {}).get("saldo_real", ((geral_api or {}).get("total") or {}).get("saldo", 0)) or 0)
        titulo = criar_cabecalho("LEMBRETE DIÁRIO", 40) + "\n\n"
        titulo += "📝 Registre todas as suas transações\n\n"
        titulo += "📊 MINI RELATÓRIO\n"
        largura = 30
        caixa_dia = ""
        caixa_dia += "+" + ("-" * largura) + "+\n"
        caixa_dia += f"|{criar_linha_tabela('HOJE', '', False, '', largura=largura)}|\n"
        caixa_dia += "+" + ("-" * largura) + "+\n"
        caixa_dia += f"|{criar_linha_tabela('RECEITAS:', formatar_moeda(receitas_dia, negrito=False), True, '', largura=largura)}|\n"
        caixa_dia += f"|{criar_linha_tabela('DESPESAS:', formatar_moeda(despesas_dia, negrito=False), True, '', largura=largura)}|\n"
        caixa_dia += f"|{criar_linha_tabela('SALDO:', formatar_moeda(saldo_dia, negrito=False), True, '', largura=largura)}|\n"
        caixa_dia += "+" + ("-" * largura) + "+\n"
        caixa_mes = ""
        caixa_mes += "+" + ("-" * largura) + "+\n"
        caixa_mes += f"|{criar_linha_tabela('ESTE MÊS', '', False, '', largura=largura)}|\n"
        caixa_mes += "+" + ("-" * largura) + "+\n"
        caixa_mes += f"|{criar_linha_tabela('RECEITAS:', formatar_moeda(receitas_mes, negrito=False), True, '', largura=largura)}|\n"
        caixa_mes += f"|{criar_linha_tabela('DESPESAS:', formatar_moeda(despesas_mes, negrito=False), True, '', largura=largura)}|\n"
        caixa_mes += f"|{criar_linha_tabela('SALDO:', formatar_moeda(saldo_mes, negrito=False), True, '', largura=largura)}|\n"
        caixa_mes += "+" + ("-" * largura) + "+\n"
        bloco = wrap_code_block(caixa_dia) + "\n" + wrap_code_block(caixa_mes)
        rodape = f"\n💹 Saldo final: {formatar_moeda(saldo_final, negrito=True)}"
        return titulo + bloco + rodape
    except:
        return "🔔 LEMBRETE: Registre todas as suas transações hoje!"

async def enviar_lembrete_diario(context: CallbackContext):
    try:
        db = get_db()
        try:
            clientes = list(db.collection("clientes").stream())
        except:
            clientes = []
        for c in clientes:
            cid = c.id
            o = c.to_dict() or {}
            nome = str(o.get("cliente_nome") or o.get("cliente_display") or "")
            uname = str(o.get("cliente_username") or "")
            try:
                msg = await mini_report(cid, cliente_nome=nome or None, username=uname or None)
                await context.bot.send_message(chat_id=cid, text=msg, parse_mode='Markdown')
            except:
                try:
                    await context.bot.send_message(chat_id=cid, text="🔔 LEMBRETE: Registre todas as suas transações hoje!", parse_mode='Markdown')
                except:
                    pass
            try:
                await asyncio.sleep(0.05)
            except:
                pass
    except:
        pass
async def _periodic_lembrete(application, interval_seconds: int = 60):
    try:
        await asyncio.sleep(5)
    except:
        pass
    while True:
        try:
            ctx = SimpleNamespace(bot=application.bot)
            await enviar_lembrete_diario(ctx)
        except:
            pass
        try:
            await asyncio.sleep(interval_seconds)
        except:
            break
def mini_report_sync(cliente_id: str, cliente_nome: str = None, username: str = None):
    try:
        dkey = _day_key_sp()
        mkey = _month_key_sp()
        qs = f"cliente_id={quote_plus(str(cliente_id))}"
        if cliente_nome:
            qs += f"&cliente_nome={quote_plus(str(cliente_nome))}"
        if username:
            qs += f"&username={quote_plus(str(username))}"
        day_url = f"{API_URL}/saldo/atual?inicio={dkey}&fim={dkey}&{qs}"
        month_url = f"{API_URL}/saldo/atual?mes={mkey}&{qs}"
        geral_url = f"{API_URL}/saldo/atual?{qs}"
        day_api = _req_json_cached(day_url, f"day:{cliente_id}:{dkey}", ttl=10, timeout=4)
        month_api = _req_json_cached(month_url, f"month:{cliente_id}:{mkey}", ttl=15, timeout=4)
        geral_api = _req_json_cached(geral_url, f"geral:{cliente_id}", ttl=20, timeout=4)
        receitas_dia = float(((day_api or {}).get("total") or {}).get("receitas", 0) or 0)
        despesas_dia = float(((day_api or {}).get("total") or {}).get("despesas", 0) or 0)
        saldo_dia = float(((day_api or {}).get("total") or {}).get("saldo", receitas_dia - despesas_dia) or (receitas_dia - despesas_dia))
        m_tot = (month_api or {}).get("total") or {}
        receitas_mes = float(m_tot.get("receitas", 0) or 0)
        despesas_mes = float(m_tot.get("despesas", 0) or 0)
        saldo_mes = float(m_tot.get("saldo", receitas_mes - despesas_mes) or (receitas_mes - despesas_mes))
        if (receitas_mes == 0 and despesas_mes == 0 and float(m_tot.get("ajustes", 0) or 0) == 0 and float(m_tot.get("estornos", 0) or 0) == 0):
            try:
                recompute_url = f"{API_URL}/recompute/cliente?{qs}"
                _req_json(recompute_url, timeout=6)
                month_api = _req_json_cached(month_url, f"month:{cliente_id}:{mkey}:fix", ttl=8, timeout=4)
                m_tot = (month_api or {}).get("total") or {}
                receitas_mes = float(m_tot.get("receitas", 0) or 0)
                despesas_mes = float(m_tot.get("despesas", 0) or 0)
                saldo_mes = float(m_tot.get("saldo", receitas_mes - despesas_mes) or (receitas_mes - despesas_mes))
            except:
                pass
        saldo_final = float(((geral_api or {}).get("total") or {}).get("saldo_real", ((geral_api or {}).get("total") or {}).get("saldo", 0)) or 0)
        titulo = criar_cabecalho("LEMBRETE DIÁRIO", 40) + "\n\n"
        titulo += "📝 Registre todas as suas transações\n\n"
        titulo += "📊 MINI RELATÓRIO\n"
        largura = 30
        caixa_dia = ""
        caixa_dia += "+" + ("-" * largura) + "+\n"
        caixa_dia += f"|{criar_linha_tabela('HOJE', '', False, '', largura=largura)}|\n"
        caixa_dia += "+" + ("-" * largura) + "+\n"
        caixa_dia += f"|{criar_linha_tabela('RECEITAS:', formatar_moeda(receitas_dia, negrito=False), True, '', largura=largura)}|\n"
        caixa_dia += f"|{criar_linha_tabela('DESPESAS:', formatar_moeda(despesas_dia, negrito=False), True, '', largura=largura)}|\n"
        caixa_dia += f"|{criar_linha_tabela('SALDO:', formatar_moeda(saldo_dia, negrito=False), True, '', largura=largura)}|\n"
        caixa_dia += "+" + ("-" * largura) + "+\n"
        caixa_mes = ""
        caixa_mes += "+" + ("-" * largura) + "+\n"
        caixa_mes += f"|{criar_linha_tabela('ESTE MÊS', '', False, '', largura=largura)}|\n"
        caixa_mes += "+" + ("-" * largura) + "+\n"
        caixa_mes += f"|{criar_linha_tabela('RECEITAS:', formatar_moeda(receitas_mes, negrito=False), True, '', largura=largura)}|\n"
        caixa_mes += f"|{criar_linha_tabela('DESPESAS:', formatar_moeda(despesas_mes, negrito=False), True, '', largura=largura)}|\n"
        caixa_mes += f"|{criar_linha_tabela('SALDO:', formatar_moeda(saldo_mes, negrito=False), True, '', largura=largura)}|\n"
        caixa_mes += "+" + ("-" * largura) + "+\n"
        bloco = wrap_code_block(caixa_dia) + "\n" + wrap_code_block(caixa_mes)
        rodape = f"\n💹 Saldo final: {formatar_moeda(saldo_final, negrito=True)}"
        return titulo + bloco + rodape
    except:
        return "🔔 LEMBRETE: Registre todas as suas transações hoje!"
def _scheduler_thread(interval_seconds: int = 60):
    try:
        db = get_db()
    except:
        return
    try:
        import time as _t
        _t.sleep(5)
    except:
        pass
    while True:
        try:
            import datetime as _dt, time as _t
            now_utc = _dt.datetime.now(_dt.timezone.utc)
            tz = None
            try:
                tz = ZoneInfo("America/Sao_Paulo") if ZoneInfo else None
            except Exception:
                tz = None
            now_local = now_utc.astimezone(tz) if tz is not None else (now_utc + _dt.timedelta(hours=-3))
            target_local = now_local.replace(hour=8, minute=0, second=0, microsecond=0)
            if now_local >= target_local:
                target_local = target_local + _dt.timedelta(days=1)
            wait_s = max(1, int((target_local - now_local).total_seconds()))
            _t.sleep(wait_s)
            clientes = []
            try:
                clientes = list(db.collection("clientes").stream())
            except:
                clientes = []
            for c in clientes:
                cid = c.id
                o = c.to_dict() or {}
                nome = str(o.get("cliente_nome") or o.get("cliente_display") or "")
                uname = str(o.get("cliente_username") or "")
                saldo = 0.0
                try:
                    url_s = f"{API_URL}/saldo/atual?cliente_id={cid}"
                    r = requests.get(url_s, timeout=6)
                    j = r.json()
                    if j.get("sucesso"):
                        tot = j.get("total", {}) or {}
                        saldo = float(tot.get("saldo_real", tot.get("saldo", 0)) or 0)
                except:
                    saldo = 0.0
                try:
                    msg = "🔔 LEMBRETE: Registre suas transações hoje!\n\n" + f"💹 SALDO ATUAL REAL: {formatar_moeda(saldo, negrito=True)}"
                    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
                    payload = {"chat_id": cid, "text": msg, "parse_mode": "Markdown"}
                    try:
                        requests.post(url, json=payload, timeout=8)
                    except:
                        pass
                except:
                    pass
                try:
                    _t.sleep(0.05)
                except:
                    pass
        except:
            break

def obter_saldo_geral(cliente_id=None):
    try:
        url = f"{API_URL}/saldo/atual"
        if cliente_id:
            url += f"?cliente_id={cliente_id}"
        resp = requests.get(url, timeout=4).json()
        if resp.get("sucesso"):
            tot = resp.get("total", {}) or {}
            return float(tot.get("saldo_real", tot.get("saldo", 0)) or 0)
    except:
        return 0.0
def calcular_estatisticas(transacoes, periodo="DIA"):
    despesas = []
    receitas = []
    for t in transacoes or []:
        if t.get('estornado'):
            continue
        tp = str(t.get('tipo', '')).strip().lower()
        if tp in ('0', 'saida'):
            despesas.append(t)
        elif tp in ('1', 'entrada'):
            receitas.append(t)
    total_despesas = sum(float(x.get('valor', 0) or 0) for x in despesas)
    total_receitas = sum(float(x.get('valor', 0) or 0) for x in receitas)
    saldo = total_receitas - total_despesas
    media_despesa = (total_despesas / len(despesas)) if despesas else 0
    media_receita = (total_receitas / len(receitas)) if receitas else 0
    maior_despesa = max(despesas, key=lambda x: float(x.get('valor', 0) or 0)) if despesas else None
    maior_receita = max(receitas, key=lambda x: float(x.get('valor', 0) or 0)) if receitas else None
    categorias_despesas = {}
    for t in despesas:
        cat = str(t.get('categoria', 'outros') or 'outros').strip().lower()
        v = float(t.get('valor', 0) or 0)
        categorias_despesas[cat] = categorias_despesas.get(cat, 0.0) + v
    return {
        'periodo': periodo,
        'total_despesas': total_despesas,
        'total_receitas': total_receitas,
        'saldo': saldo,
        'quantidade': {
            'total': (len(despesas) + len(receitas)),
            'despesas': len(despesas),
            'receitas': len(receitas),
        },
        'medias': {
            'despesa': media_despesa,
            'receita': media_receita,
        },
        'maiores': {
            'despesa': maior_despesa,
            'receita': maior_receita,
        },
        'categorias_despesas': categorias_despesas,
    }
def md_escape(s: str) -> str:
    return (
        str(s)
        .replace('\\', '\\\\')
        .replace('_', '\\_')
        .replace('*', '\\*')
        .replace('[', '\\[')
        .replace(']', '\\]')
        .replace('(', '\\(')
        .replace(')', '\\)')
        .replace('~', '\\~')
        .replace('`', '\\`')
        .replace('>', '\\>')
        .replace('#', '\\#')
        .replace('+', '\\+')
        .replace('-', '\\-')
        .replace('=', '\\=')
        .replace('|', '\\|')
        .replace('{', '\\{')
        .replace('}', '\\}')
        .replace('.', '\\.')
        .replace('!', '\\!')
    )

def _normalize_ascii(s: str) -> str:
    try:
        return unicodedata.normalize('NFKD', str(s)).encode('ascii', 'ignore').decode('ascii').lower().strip()
    except:
        return str(s or '').lower().strip()
def _map_text_to_category(texto: str) -> str:
    tx = _normalize_ascii(texto)
    if not tx:
        return "outros"
    for k, v in CATEGORY_NAMES.items():
        if tx == _normalize_ascii(k) or tx == _normalize_ascii(v):
            return k
    for k, v in CATEGORY_NAMES.items():
        if _normalize_ascii(k) in tx or _normalize_ascii(v).split()[0] in tx:
            return k
    keys = list(CATEGORY_NAMES.keys())
    try:
        best = max(keys, key=lambda k: difflib.SequenceMatcher(a=tx, b=_normalize_ascii(k)).ratio())
        if difflib.SequenceMatcher(a=tx, b=_normalize_ascii(best)).ratio() >= 0.6:
            return best
    except:
        pass
    return "outros"
def _top_categorias_cliente(cliente_id: str, tipo: str, limit: int = 9):
    try:
        db = get_db()
        root = db.collection("clientes").document(str(cliente_id))
        campo = "categorias_entrada" if str(tipo).strip().lower() in ("entrada", "1", "receita") else "categorias_saida"
        agg = {}
        try:
            hoje = _now_sp()
            dt_ini = hoje - timedelta(days=60)
            cur = dt_ini
            while cur <= hoje:
                dkey = cur.strftime("%Y-%m-%d")
                try:
                    dd = root.collection("dias").document(dkey).get().to_dict() or {}
                except:
                    dd = {}
                for k, v in dict(dd.get(campo, {}) or {}).items():
                    agg[k] = float(agg.get(k, 0.0) or 0.0) + float(v or 0.0)
                cur = cur + timedelta(days=1)
        except:
            try:
                for mdoc in root.collection("meses").stream():
                    mo = mdoc.to_dict() or {}
                    for k, v in dict(mo.get(campo, {}) or {}).items():
                        agg[k] = float(agg.get(k, 0.0) or 0.0) + float(v or 0.0)
            except:
                mm = root.collection("meses").document(_month_key_sp()).get().to_dict() or {}
                for k, v in dict(mm.get(campo, {}) or {}).items():
                    agg[k] = float(agg.get(k, 0.0) or 0.0) + float(v or 0.0)
        items = sorted(((k, float(v or 0)) for k, v in agg.items()), key=lambda x: (-x[1], x[0]))
        lst = [k for k, _ in items if k not in ("duvida", "outros")]
        return lst[:limit]
    except:
        return []
def _categoria_keyboard(ref_id: str, tipo: str, chat_id: str):
    base_prior = ["alimentacao", "transporte", "saude"]
    usados = _top_categorias_cliente(chat_id, tipo, limit=9)
    lst = []
    seen = set()
    for c in base_prior + usados:
        if c not in ("duvida", "outros") and c not in seen:
            seen.add(c)
            lst.append(c)
    if len(lst) > 9:
        lst = lst[:9]
    rows = []
    row = []
    for c in lst:
        label = CATEGORY_NAMES.get(c, c)
        row.append(InlineKeyboardButton(label, callback_data=f"confirm_categoria:{ref_id}:{c}"))
        if len(row) >= 3:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("📝 Digitar categoria", callback_data=f"categoria_digitar:{ref_id}:{tipo}")])
    rows.append([InlineKeyboardButton("❌ Cancelar", callback_data="confirm_categoria_cancelar")])
    return InlineKeyboardMarkup(rows)

async def _disparar_confirmacoes(update_or_query, context, transacoes, salvas):
    try:
        chat_id = get_cliente_id(update_or_query)
        idx = {}
        for s in salvas or []:
            k = (str(s.get("tipo")).strip(), float(s.get("valor", 0)), str(s.get("categoria", "")).strip().lower())
            idx[k] = s
        count = 0
        for it in transacoes or []:
            cat = str(it.get("categoria", "outros") or "outros").strip().lower()
            conf = 0.0
            try:
                conf = float(it.get("confidence_score", 0.0) or 0.0)
            except:
                conf = 0.0
            pend = (cat in ("duvida", "outros")) or (conf < 0.95) or bool(it.get("pendente_confirmacao", False))
            if not pend:
                continue
            tp_n = str(it.get("tipo", "0")).strip()
            tp_txt = "saida" if tp_n in ("0", "despesa", "saida") else "entrada"
            k = (tp_txt, float(it.get("valor", 0) or 0), cat)
            sref = idx.get(k)
            rid = sref.get("ref_id") if sref else None
            if not rid:
                continue
            emoji = "🔴" if tp_txt == "saida" else "🟢"
            tipo = "DESPESA" if tp_txt == "saida" else "RECEITA"
            desc = str(it.get("descricao", "") or "")
            valor = float(it.get("valor", 0) or 0)
            cat_nome = md_escape(CATEGORY_NAMES.get(cat, cat))
            texto = criar_cabecalho("CATEGORIZAÇÃO", 40)
            texto += f"\n{emoji} {formatar_moeda(valor)}\n"
            texto += f"`{desc}`\n"
            texto += f"Categoria atual/sugerida: {cat_nome}\n"
            texto += f"\nDeseja categorizar?"
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Sim", callback_data=f"cat_yes:{rid}:{tp_txt}"),
                 InlineKeyboardButton("❌ Não", callback_data=f"cat_no:{rid}:{cat}")]
            ])
            try:
                m = await context.bot.send_message(chat_id=chat_id, text=texto, parse_mode='Markdown', reply_markup=kb)
                try:
                    pend = context.user_data.get("pending_confirmations") or {}
                    pend[rid] = {"chat_id": m.chat_id, "message_id": m.message_id, "ref_id": rid, "categoria": cat, "tipo": tp_txt}
                    context.user_data["pending_confirmations"] = pend
                except:
                    pass
                async def _auto_timeout():
                    try:
                        import asyncio as _a
                        await _a.sleep(60)
                        pend2 = context.user_data.get("pending_confirmations") or {}
                        info = pend2.get(rid)
                        if not info:
                            return
                        payload = {
                            "cliente_id": str(chat_id),
                            "referencia_id": rid,
                            "nova_categoria": cat,
                        }
                        try:
                            r = requests.post(f"{API_URL}/transacoes/atualizar_categoria", json=payload, timeout=8)
                            data = r.json() if r.ok else {"sucesso": False}
                        except:
                            data = {"sucesso": False}
                        try:
                            pend2.pop(rid, None)
                            context.user_data["pending_confirmations"] = pend2
                        except:
                            pass
                        try:
                            await context.bot.delete_message(chat_id=info["chat_id"], message_id=info["message_id"])
                        except:
                            pass
                        try:
                            ltb = context.user_data.get("last_tx_block") or {}
                            items = list(ltb.get("items", []) or [])
                            for it2 in items:
                                if str(it2.get("ref_id") or "") == rid:
                                    it2["categoria"] = cat
                                    try:
                                        key = _normalize_ascii(re.sub(r'\s+', ' ', str(it2.get("descricao", ""))).strip())
                                        mem = context.user_data.get("cat_memory", {}) or {}
                                        if key:
                                            mem[key] = cat
                                            context.user_data["cat_memory"] = mem
                                            try:
                                                await asyncio.to_thread(atualizar_memoria_categoria, str(chat_id), key, cat)
                                            except:
                                                pass
                                    except:
                                        pass
                            if ltb.get("chat_id") and ltb.get("message_id"):
                                resposta = criar_cabecalho("TRANSAÇÃO REGISTRADA", 40)
                                resposta += f"\n✅ *{len(items)} transação(ões) registrada(s)*\n\n"
                                for it3 in items:
                                    tp3 = str(it3.get('tipo', '')).strip().lower()
                                    emoji3 = "🔴" if tp3 in ('saida', '0') else "🟢"
                                    tipo3 = "DESPESA" if tp3 in ('saida', '0') else "RECEITA"
                                    cat_nome3 = md_escape(CATEGORY_NAMES.get(it3.get('categoria', 'outros'), it3.get('categoria', 'outros')))
                                    desc_json3 = str(it3.get('descricao', ''))
                                    resposta += f"{emoji3} *{tipo3}:* {formatar_moeda(float(it3.get('valor', 0)))}\n"
                                    resposta += f"   `{desc_json3}`\n"
                                    resposta += f"   Categoria: {cat_nome3}\n\n"
                                kb = InlineKeyboardMarkup([
                                    [InlineKeyboardButton("💰 TOTAIS DO DIA", callback_data="total_dia"),
                                     InlineKeyboardButton("📅 TOTAIS DO MÊS", callback_data="analise_mes")]
                                ])
                                await context.bot.edit_message_text(chat_id=ltb["chat_id"], message_id=ltb["message_id"], text=resposta, parse_mode='Markdown', reply_markup=kb)
                        except:
                            pass
                    except:
                        pass
                try:
                    import asyncio as _a
                    _a.create_task(_auto_timeout())
                except:
                    pass
            except:
                pass
            count += 1
            if count >= 3:
                break
    except:
        pass
def formatar_data_hora_local(ts_raw):
    try:
        s = str(ts_raw or "").strip()
        if not s:
            return None
        st = s.replace('Z', '+00:00')
        dt = datetime.fromisoformat(st)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        tz = _TZ_SP
        if tz is not None:
            return dt.astimezone(tz).strftime('%Y-%m-%d %H:%M')
        return dt.astimezone().strftime('%Y-%m-%d %H:%M')
    except:
        try:
            return str(ts_raw)[:16]
        except:
            return None
def get_cliente_id(obj) -> str:
    try:
        if hasattr(obj, 'effective_chat') and getattr(obj, 'effective_chat', None):
            return str(obj.effective_chat.id)
        if hasattr(obj, 'message') and getattr(obj, 'message', None) and hasattr(obj.message, 'chat') and getattr(obj.message, 'chat', None):
            return str(obj.message.chat.id)
    except:
        pass
    return "default"
def get_cliente_nome(obj) -> str:
    try:
        if hasattr(obj, 'effective_user') and getattr(obj, 'effective_user', None):
            user = obj.effective_user
            fn = getattr(user, 'full_name', None) or (getattr(user, 'first_name', None) or "")
            ln = getattr(user, 'last_name', None) or ""
            nm = (fn + (" " + ln if ln else "")).strip()
            return nm or (getattr(user, 'username', "") or "")
        if hasattr(obj, 'message') and getattr(obj, 'message', None) and hasattr(obj.message, 'from_user'):
            u = obj.message.from_user
            fn = getattr(u, 'full_name', None) or (getattr(u, 'first_name', None) or "")
            ln = getattr(u, 'last_name', None) or ""
            nm = (fn + (" " + ln if ln else "")).strip()
            return nm or (getattr(u, 'username', "") or "")
    except:
        return ""
    return ""
def get_cliente_username(obj) -> str:
    try:
        if hasattr(obj, 'effective_user') and getattr(obj, 'effective_user', None):
            return str(getattr(obj.effective_user, 'username', "") or "")
        if hasattr(obj, 'message') and getattr(obj, 'message', None) and hasattr(obj.message, 'from_user'):
            return str(getattr(obj.message.from_user, 'username', "") or "")
    except:
        return ""
    return ""
def build_cliente_query_params(obj) -> str:
    cid = get_cliente_id(obj)
    nome = get_cliente_nome(obj)
    uname = get_cliente_username(obj)
    return f"cliente_id={quote_plus(str(cid))}&cliente_nome={quote_plus(str(nome))}&username={quote_plus(str(uname))}"

def normalizar_data(s: str) -> str:
    t = str(s or "").strip()
    import re as _re
    m = _re.match(r'^(\d{2})[./-](\d{2})[./-](\d{4})$', t)
    if m:
        d, m_, y = m.groups()
        return f"{y}-{m_}-{d}"
    m = _re.match(r'^(\d{4})-(\d{2})-(\d{2})$', t)
    if m:
        return t
    m = _re.search(r'\bdia\s+(\d{1,2})\s+de\s+(\d{1,2})\s+de\s+(\d{4})\b', t, _re.IGNORECASE)
    if m:
        d, m_, y = m.groups()
        return f"{y}-{int(m_):02d}-{int(d):02d}"
    return t
def extrair_campos_estorno(texto: str):
    tl = (texto or "").strip()
    m_val = re.search(r'(?:R?\$?\s*)?(\d{1,3}(?:[.\s]\d{3})*(?:,\d{2})|\d+[.,]\d{2}|\d+)\b', tl)
    v = None
    if m_val:
        raw = m_val.group(1)
        pv = parse_value(raw)
        if pv is not None:
            v = float(pv)
    m_dt = re.search(r'(\d{2}[./-]\d{2}[./-]\d{4}|\d{4}-\d{2}-\d{2})', tl)
    dr = None
    if m_dt:
        dr = normalizar_data(m_dt.group(1))
    else:
        m2 = re.search(r'\bdia\s+\d{1,2}\s+de\s+\d{1,2}\s+de\s+\d{4}\b', tl, re.IGNORECASE)
        if m2:
            dr = normalizar_data(m2.group(0))
    tipo = None
    if re.search(r'\b(receita|entrada|recebi|ganhei|vendi)\b', tl, re.IGNORECASE):
        tipo = "entrada"
    elif re.search(r'\b(despesa|gasto|paguei|comprei|custou|fatura|boleto|pagamento)\b', tl, re.IGNORECASE):
        tipo = "saida"
    return v, dr, tipo
def formatar_data_br(s: str) -> str:
    t = str(s or "").strip()
    m = re.match(r'^(\d{4})-(\d{2})-(\d{2})$', t)
    if m:
        y, mm, d = m.groups()
        return f"{int(d):02d}/{int(mm):02d}/{y}"
    m2 = re.match(r'^(\d{2})[./-](\d{2})[./-](\d{4})$', t)
    if m2:
        d, mm, y = m2.groups()
        return f"{int(d):02d}/{int(mm):02d}/{y}"
    return t

def _mes_ano_pt(dt):
    nomes = [
        "janeiro", "fevereiro", "março", "abril", "maio", "junho",
        "julho", "agosto", "setembro", "outubro", "novembro", "dezembro"
    ]
    try:
        nome = nomes[int(dt.month) - 1].capitalize()
        return f"{nome}/{int(dt.year)}"
    except:
        return dt.strftime("%m/%Y")
def _debitos_header(compact: bool) -> str:
    if compact:
        return f"{'VENC':<10} {'DESCR':<12} {'VALOR':>9} {'ST':^1}\n"
    return f"{'VENC.':<10}  {'DESCRIÇÃO':<20}  {'VALOR':>12}  {'ST':^2}\n"
def _debitos_line(it: dict, compact: bool) -> str:
    data_v = formatar_data_br(str(it.get('vencimento', '') or '')[:10])
    desc = str(it.get('descricao', '') or '')
    val_f = float(it.get('valor', 0) or 0)
    st = str(it.get('status', '') or '') or ''
    if compact:
        desc = desc[:12]
        val = formatar_moeda(val_f).replace("R$ ", "R$")[:9]
        stc = st[:1]
        return f"{data_v:<10} {desc:<12} {val:>9} {stc:^1}"
    desc = desc[:20]
    val = formatar_moeda(val_f)[:12]
    std = st[:2]
    return f"{data_v:<10}  {desc:<20}  {val:>12}  {std:^2}"
def _group_por_data(lst):
    gs = {}
    for it in sorted(lst, key=lambda x: x.get("vencimento", "") or ""):
        dt = formatar_data_br(str(it.get('vencimento', '') or '')[:10]) or "(sem data)"
        if dt not in gs:
            gs[dt] = []
        gs[dt].append(it)
    return gs
def _render_grouped(lst, compact: bool) -> str:
    grupos = _group_por_data(lst)
    blocos = []
    total_grupos = len(grupos)
    idx_grupo = 0
    for dt, items in grupos.items():
        blocos.append(dt)
        if compact:
            target_len = 36
            val_w_cap = 14
        else:
            target_len = 60
            val_w_cap = 20
        vals_fmt = []
        descs = []
        for it in items:
            descs.append(str(it.get('descricao', '') or ''))
            v = float(it.get('valor', 0) or 0)
            vv = formatar_moeda(v)
            if compact:
                vv = vv.replace("R$ ", "R$")
            vals_fmt.append(vv)
        try:
            val_w = max(len("VALOR"), min(max((len(x) for x in vals_fmt), default=len("VALOR")), val_w_cap))
        except:
            val_w = len("VALOR")
        desc_w = max(len("DESCRIÇÃO"), target_len - (3 + val_w))
        header_sub = f"{'DESCRIÇÃO':<{desc_w}} | {'VALOR':>{val_w}}"
        blocos.append(header_sub)
        blocos.append(f"{'-'*desc_w} | {'-'*val_w}")
        def _ord_status(s):
            t = str(s or '').strip().lower()
            return 0 if t.startswith('s') else 1
        items_sorted = sorted(items, key=lambda it: (_ord_status(it.get('status')), str(it.get('descricao', '') or '')))
        subtotal = 0.0
        for it in items_sorted:
            desc = str(it.get('descricao', '') or '')
            val_f = float(it.get('valor', 0) or 0)
            dsc = f"{desc[:desc_w]:<{desc_w}}"
            vtxt = formatar_moeda(val_f)
            if compact:
                vtxt = vtxt.replace("R$ ", "R$")
            vtxt = vtxt[-val_w:] if len(vtxt) > val_w else vtxt
            vv = f"{vtxt:>{val_w}}"
            blocos.append(f"{dsc} | {vv}")
            subtotal += float(val_f or 0)
        blocos.append(f"Total do dia: {formatar_moeda(subtotal, negrito=True)}")
        idx_grupo += 1
        if idx_grupo < total_grupos:
            blocos.append(f"{'_'*desc_w} | {'_'*val_w}")
            blocos.append("")
    return "\n".join(blocos).strip()
def _render_grouped_fixed(lst, compact: bool, desc_w: int, val_w: int) -> str:
    grupos = _group_por_data(lst)
    blocos = []
    total_grupos = len(grupos)
    idx_grupo = 0
    for dt, items in grupos.items():
        blocos.append(dt)
        header_sub = f"{'DESCRIÇÃO':<{desc_w}} | {'VALOR':>{val_w}}"
        blocos.append(header_sub)
        blocos.append(f"{'-'*desc_w} | {'-'*val_w}")
        def _ord_status(s):
            t = str(s or '').strip().lower()
            return 0 if t.startswith('s') else 1
        items_sorted = sorted(items, key=lambda it: (_ord_status(it.get('status')), str(it.get('descricao', '') or '')))
        subtotal = 0.0
        for it in items_sorted:
            desc = str(it.get('descricao', '') or '')
            val_f = float(it.get('valor', 0) or 0)
            dsc = f"{desc[:desc_w]:<{desc_w}}"
            vtxt = formatar_moeda(val_f)
            if compact:
                vtxt = vtxt.replace("R$ ", "R$")
            vtxt = vtxt[-val_w:] if len(vtxt) > val_w else vtxt
            vv = f"{vtxt:>{val_w}}"
            blocos.append(f"{dsc} | {vv}")
            subtotal += float(val_f or 0)
        blocos.append(f"Total do dia: {formatar_moeda(subtotal, negrito=True)}")
        idx_grupo += 1
        if idx_grupo < total_grupos:
            blocos.append(f"{'_'*desc_w} | {'_'*val_w}")
            blocos.append("")
    return "\n".join(blocos).strip()
def _render_descricoes_only(lst, compact: bool) -> str:
    grupos = _group_por_data(lst)
    blocos = []
    target_len = 36 if compact else 60
    total_grupos = len(grupos)
    idx = 0
    for dt, items in grupos.items():
        blocos.append(dt)
        subtotal = 0.0
        for it in items:
            desc = str(it.get('descricao', '') or '')
            try:
                subtotal += float(it.get('valor', 0) or 0)
            except:
                pass
            blocos.append(f"{desc[:target_len]}")
        blocos.append(f"Total do dia: {formatar_moeda(subtotal, negrito=True)}")
        idx += 1
        if idx < total_grupos:
            blocos.append("_" * target_len)
            blocos.append("")
    return "\n".join(blocos).strip()
def _mes_key_from_text(texto: str) -> str:
    t = (texto or "").strip().lower()
    meses = {
        "janeiro": "01", "fevereiro": "02", "março": "03", "marco": "03", "abril": "04",
        "maio": "05", "junho": "06", "julho": "07", "agosto": "08", "setembro": "09",
        "outubro": "10", "novembro": "11", "dezembro": "12"
    }
    try:
        now = _now_sp()
    except:
        now = datetime.now()
    if re.search(r'\b(este|esse|neste)\s+m[êe]s\b', t):
        return now.strftime("%Y-%m")
    if re.search(r'\b(pr[óo]ximo)\s+m[êe]s\b', t):
        return _add_months(now, 1).strftime("%Y-%m")
    m = re.search(r'\bem\s+(janeiro|fevereiro|mar(?:ç|c)o|abril|maio|junho|julho|agosto|setembro|outubro|novembro|dezembro)(?:\s+de\s+(\d{4}))?', t)
    if m:
        nome = m.group(1)
        ano = m.group(2) or str(now.year)
        key_m = meses.get(nome.replace("ç", "c"), None)
        if key_m:
            return f"{ano}-{key_m}"
    m2 = re.search(r'\b(\d{4})[-/](\d{1,2})\b', t)
    if m2:
        y, mm = m2.groups()
        return f"{y}-{int(mm):02d}"
    return ""
def _extract_first_value(texto: str) -> float:
    tl = (texto or "").strip()
    m_val = re.search(r'(?:R?\$?\s*)?(\d{1,3}(?:[.\s]\d{3})*(?:,\d{2})|\d+[.,]\d{2}|\d+)\b', tl)
    if m_val:
        raw = m_val.group(1)
        pv = parse_value(raw)
        if pv is not None:
            return float(pv)
    return 0.0
def _detectar_intencao(texto: str):
    tl = (texto or "").strip().lower()
    if re.search(r'quanto\s+devo', tl):
        mk = _mes_key_from_text(tl) or _month_key_sp()
        return {"tipo": "debitos_mes", "mes": mk}
    if re.search(r'(d[aá]\s+pra|posso|consigo)\s+comprar', tl):
        val = _extract_first_value(tl)
        mk = _mes_key_from_text(tl) or _month_key_sp()
        return {"tipo": "compra_viabilidade", "valor": val, "mes": mk}
    m_desc = re.match(r'^\s*descri(?:c|ç)ao\s+(.+)$', tl)
    if m_desc:
        termo = m_desc.group(1).strip()
        mk = _mes_key_from_text(tl) or _month_key_sp()
        return {"tipo": "buscar_descricao", "termo": termo, "mes": mk}
    return None
async def iniciar_fluxo_estorno_por_valor(update, context, valor, data_ref, tipo=None, processamento=None):
    payload = {
        "cliente_id": str(update.effective_chat.id),
        "valor": float(valor),
        "data_referencia": data_ref
    }
    if tipo:
        payload["tipo"] = tipo
    try:
        r = requests.post(f"{API_URL}/ajustes/buscar_por_valor", json=payload, timeout=8)
        data = r.json() if r.ok else {"sucesso": False}
    except:
        data = {"sucesso": False}
    if not data.get("sucesso"):
        msg = "⚠️ Erro ao buscar transações."
        if processamento:
            await context.bot.edit_message_text(chat_id=processamento.chat_id, message_id=processamento.message_id, text=msg, parse_mode='Markdown')
        else:
            await update.message.reply_text(msg, parse_mode='Markdown')
        return
    q = int(data.get("quantidade", 0) or 0)
    ms = data.get("matches", []) or []
    omit = int(data.get("omitidos_estornados", 0) or 0)
    avisos = data.get("avisos", []) or []
    if q == 0:
        msg = "Não há transações para este dia e valor."
        if omit > 0:
            msg += f"\n⚠️ {omit} transação(ões) já estão estornadas e foram omitidas."
        if processamento:
            await context.bot.edit_message_text(chat_id=processamento.chat_id, message_id=processamento.message_id, text=msg, parse_mode='Markdown')
        else:
            await update.message.reply_text(msg, parse_mode='Markdown')
        return
    if q == 1:
        t = ms[0]
        tp = str(t.get('tipo', '')).strip().lower()
        emoji = "🔴" if tp in ('0', 'saida') else "🟢"
        cat_raw = t.get('categoria', 'outros')
        cat_nome = md_escape(CATEGORY_NAMES.get(cat_raw, cat_raw))
        desc_json = str(t.get('descricao', ''))
        texto = criar_cabecalho("CONFIRMAR ESTORNO", 40)
        texto += f"\n{emoji} {formatar_moeda(float(t.get('valor', 0)))}\n"
        texto += f"`{desc_json}`\n"
        texto += f"Categoria: {cat_nome}\n"
        ts_raw = str(t.get('timestamp_criacao', '') or '')
        fmt_ts = formatar_data_hora_local(ts_raw)
        if fmt_ts:
            texto += f"Data de criação: {fmt_ts}\n"
        origem = str(t.get('origem', '') or '')
        if origem:
            texto += f"Origem: {md_escape(origem)}\n"
        if omit > 0:
            texto += f"\n⚠️ {omit} transação(ões) já estornadas foram omitidas."
        for a in avisos[:2]:
            texto += f"\n⚠️ {md_escape(a)}"
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Confirmar", callback_data=f"estornar_confirmar:{t.get('id')}")],
            [InlineKeyboardButton("❌ Cancelar", callback_data="estornar_cancelar")]
        ])
        if processamento:
            await context.bot.edit_message_text(chat_id=processamento.chat_id, message_id=processamento.message_id, text=texto, parse_mode='Markdown', reply_markup=kb)
        else:
            await update.message.reply_text(texto, parse_mode='Markdown', reply_markup=kb)
        return
    texto = criar_cabecalho("SELECIONE PARA ESTORNO", 40)
    linhas = []
    for t in ms[:6]:
        tp = str(t.get('tipo', '')).strip().lower()
        emoji = "🔴" if tp in ('0', 'saida') else "🟢"
        desc_json = str(t.get('descricao', ''))
        linhas.append(f"{emoji} {formatar_moeda(float(t.get('valor', 0)))} — `{desc_json}`")
    texto += "\n" + "\n".join(linhas) + "\n"
    kb_rows = [[InlineKeyboardButton(f"{i+1}", callback_data=f"estornar_escolher:{t.get('id')}")] for i, t in enumerate(ms[:6])]
    kb_rows.append([InlineKeyboardButton("❌ Cancelar", callback_data="estornar_cancelar")])
    kb = InlineKeyboardMarkup(kb_rows)
    if processamento:
        await context.bot.edit_message_text(chat_id=processamento.chat_id, message_id=processamento.message_id, text=texto, parse_mode='Markdown', reply_markup=kb)
    else:
        await update.message.reply_text(texto, parse_mode='Markdown', reply_markup=kb)

# ===== COMANDOS DO BOT =====
async def start(update: Update, context: CallbackContext) -> None:
    """Envia mensagem de boas-vindas com dashboard."""
    
    keyboard = [
        [InlineKeyboardButton("💰 TOTAIS DO DIA", callback_data="total_dia")],
        [InlineKeyboardButton("📅 TOTAIS DO MÊS", callback_data="analise_mes")],
        [InlineKeyboardButton("🎯 METAS MENSAIS", callback_data="projetados_menu")],
        [InlineKeyboardButton("📅 COMPROMISSOS DO MÊS", callback_data="debitos_menu")],
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    welcome_text = criar_cabecalho("BOT FINANCEIRO PROFISSIONAL", 40)
    welcome_text += "\n\n"
    welcome_text += "💼 *SISTEMA DE CONTROLE FINANCEIRO PESSOAL*\n\n"
    
    welcome_text += "📋 *COMANDOS DISPONÍVEIS:*\n"
    welcome_text += "• /total     - Totais do dia atual\n"
    welcome_text += "• /hoje      - Resumo detalhado do dia\n"
    welcome_text += "• /semana    - Totais da semana atual\n"
    welcome_text += "• /mes       - Totais do mês atual\n"
    welcome_text += "• /resumo    - Resumo financeiro completo\n"
    welcome_text += "• /extrato   - Extrato detalhado\n"
    welcome_text += "• /analise   - Análise mensal\n"
    welcome_text += "• /categorias- Gastos por categoria\n"
    welcome_text += "• /projetados- Metas mensais\n"
    welcome_text += "• /meta      - Lançar meta mensal (ex.: /meta Internet 120)\n"
    welcome_text += "• /debitos   - Compromissos do mês (faturas/boletos)\n"
    welcome_text += "• /compromissos - Atalho para compromissos do mês\n\n"
    welcome_text += "• /compromisso - Adicionar compromisso (ex.: /compromisso Internet 120 dia 10)\n\n"
    
    welcome_text += "🎯 *DIFERENCIAIS:*\n"
    welcome_text += "• Formatação profissional\n"
    welcome_text += "• Distinção clara: DIA vs PERÍODO\n"
    welcome_text += "• Análises personalizadas\n"
    welcome_text += "• Metas mensais\n\n"
    
    welcome_text += "💡 *COMO USAR:*\n"
    welcome_text += "1. Envie: \"gastei 50 no mercado\"\n"
    welcome_text += "2. Use /total para verificar\n"
    welcome_text += "3. Acompanhe com /analise\n\n"
    
    welcome_text += "📝 *Legenda:*\n"
    welcome_text += "• ST - Vencido\n"
    welcome_text += "• V  - A vencer\n\n"
    
    welcome_text += "*Ou use os botões abaixo ↓*"
    
    await update.message.reply_text(welcome_text, parse_mode='Markdown', reply_markup=reply_markup)

async def button_handler(update: Update, context: CallbackContext) -> None:
    """Processa cliques nos botões."""
    query = update.callback_query
    await query.answer()
    
    if query.data == "relatorio_dia":
        await relatorio_diario(query, context)
    elif query.data == "total_dia":
        await relatorio_total(query, context)
    elif query.data == "total_dia_categorias":
        await categorias_dia(query, context)
    elif query.data == "total_semana":
        await total_semana(query, context)
    elif query.data == "analise_mes":
        await analise_mensal(query, context)
    elif query.data == "analise_mes_categorias":
        await categorias_mes(query, context)
    elif query.data == "catdia_expand":
        await expandir_categorias_dia(query, context, limit=6)
    elif query.data.startswith("catdia:"):
        try:
            _, cslug = query.data.split(":", 1)
        except:
            cslug = "outros"
        await detalhar_categoria_dia(query, context, cslug)
    elif query.data.startswith("catdia_more_saida:"):
        try:
            _, cslug, off_s, off_e = query.data.split(":", 3)
            off_s = int(off_s)
            off_e = int(off_e)
        except:
            cslug, off_s, off_e = "outros", 0, 0
        await detalhar_categoria_dia(query, context, cslug, off_saida=off_s, off_entrada=off_e)
    elif query.data.startswith("catdia_more_entrada:"):
        try:
            _, cslug, off_s, off_e = query.data.split(":", 3)
            off_s = int(off_s)
            off_e = int(off_e)
        except:
            cslug, off_s, off_e = "outros", 0, 0
        await detalhar_categoria_dia(query, context, cslug, off_saida=off_s, off_entrada=off_e)
    elif query.data.startswith("catmes:"):
        try:
            _, cslug = query.data.split(":", 1)
        except:
            cslug = "outros"
        await detalhar_categoria_mes(query, context, cslug)
    elif query.data == "catmes_expand":
        await expandir_categorias_mes(query, context, limit=6)
    elif query.data.startswith("catmes_more_saida:"):
        try:
            _, cslug, off_s, off_e = query.data.split(":", 3)
            off_s = int(off_s)
            off_e = int(off_e)
        except:
            cslug, off_s, off_e = "outros", 0, 0
        await detalhar_categoria_mes(query, context, cslug, off_saida=off_s, off_entrada=off_e)
    elif query.data.startswith("catmes_more_entrada:"):
        try:
            _, cslug, off_s, off_e = query.data.split(":", 3)
            off_s = int(off_s)
            off_e = int(off_e)
        except:
            cslug, off_s, off_e = "outros", 0, 0
        await detalhar_categoria_mes(query, context, cslug, off_saida=off_s, off_entrada=off_e)
    elif query.data == "categorias":
        await relatorio_categorias(query, context)
    elif query.data == "extrato_completo":
        await extrato_detalhado(query, context)
    elif query.data == "resumo":
        await resumo_financeiro(query, context)
    elif query.data == "menu":
        await start(query, context)
    elif query.data == "debitos_menu":
        await _menu_debitos(query, context)
    elif query.data.startswith("debitos_mes:"):
        try:
            mes = query.data.split(":", 1)[1]
        except Exception:
            mes = _month_key_sp()
        await mostrar_debitos_mes(query, context, mes, compact=True)
    elif query.data.startswith("debitos_mes_m:"):
        try:
            mes = query.data.split(":", 1)[1]
        except Exception:
            mes = _month_key_sp()
        await mostrar_debitos_mes(query, context, mes, compact=True)
    elif query.data.startswith("debitos_mes_d:"):
        try:
            mes = query.data.split(":", 1)[1]
        except Exception:
            mes = _month_key_sp()
        await mostrar_debitos_mes(query, context, mes, compact=False)
    elif query.data == "projetados_menu":
        await _menu_projetados(query, context)
    elif query.data.startswith("projetados_mes:"):
        try:
            mes = query.data.split(":", 1)[1]
        except Exception:
            mes = _month_key_sp()
        await mostrar_projetados_mes(query, context, mes)
    elif query.data.startswith("debitos_tipo:"):
        parts = query.data.split(":")
        mes = parts[1] if len(parts) > 1 else _month_key_sp()
        tipo = parts[2] if len(parts) > 2 else "vencidos"
        compact = True
        try:
            url_all = f"{API_URL}/compromissos/mes?mes={mes}&{build_cliente_query_params(query)}"
            data_all = requests.get(url_all, timeout=6).json()
        except:
            logging.exception("Falha ao carregar compromissos (tipo)")
            data_all = {"sucesso": False}
        if not data_all.get("sucesso"):
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Voltar", callback_data="debitos_menu")]])
            await query.edit_message_text("⚠️ Não foi possível carregar os compromissos.", parse_mode='Markdown', reply_markup=kb)
            return
        vencidos = data_all.get("vencidos", []) or []
        a_vencer = data_all.get("a_vencer", []) or []
        all_items = list(vencidos) + list(a_vencer)
        target_len = 36
        val_w_cap = 14
        if all_items:
            vals_fmt = []
            for it in all_items:
                v = float(it.get('valor', 0) or 0)
                vv = formatar_moeda(v).replace("R$ ", "R$")
                vals_fmt.append(vv)
            try:
                val_w = max(len("VALOR"), min(max((len(x) for x in vals_fmt), default=len("VALOR")), val_w_cap))
            except:
                val_w = len("VALOR")
            desc_w = max(len("DESCRIÇÃO"), target_len - (3 + val_w))
        else:
            val_w = 12
            desc_w = 21
        lst = vencidos if tipo == "vencidos" else a_vencer
        titulo = criar_cabecalho(f"COMPROMISSOS • {_mes_label(mes)} • {'VENCIDOS' if tipo=='vencidos' else 'A VENCER'}", 40)
        resposta = titulo + "\n\n"
        cx = _render_grouped_fixed(lst, compact=True, desc_w=desc_w, val_w=val_w)
        resposta += wrap_code_block(cx) + "\n"
        tot_sel = sum(float(it.get('valor', 0) or 0) for it in lst)
        resposta += f"Total: {formatar_moeda(tot_sel, negrito=True)}\n"
        kb = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("Vencidos", callback_data=f"debitos_tipo:{mes}:vencidos"),
                InlineKeyboardButton("A vencer", callback_data=f"debitos_tipo:{mes}:a_vencer"),
            ],
            [InlineKeyboardButton("⬅️ Voltar", callback_data="debitos_menu")],
            [InlineKeyboardButton("🏠 MENU", callback_data="menu")],
        ])
        await query.edit_message_text(resposta, parse_mode='Markdown', reply_markup=kb)
    elif query.data.startswith("confirm_categoria:"):
        try:
            _, ref_id, cat = query.data.split(":", 2)
        except Exception:
            ref_id, cat = None, None
        if not (ref_id and cat):
            await query.edit_message_text("⚠️ Dados inválidos.", parse_mode='Markdown')
            return
        try:
            pend = context.user_data.get("pending_confirmations") or {}
            pend.pop(ref_id, None)
            context.user_data["pending_confirmations"] = pend
        except:
            pass
        payload = {
            "cliente_id": get_cliente_id(query),
            "referencia_id": ref_id,
            "nova_categoria": cat,
        }
        try:
            r = requests.post(f"{API_URL}/transacoes/atualizar_categoria", json=payload, timeout=8)
            data = r.json() if r.ok else {"sucesso": False}
        except:
            data = {"sucesso": False}
        if not data.get("sucesso"):
            err = str(data.get("erro", "Falha ao atualizar categoria."))
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("🏠 MENU", callback_data="menu")]])
            await query.edit_message_text(f"⚠️ {md_escape(err)}", parse_mode='Markdown', reply_markup=kb)
            return
        try:
            up = data.get("atualizacao", {}) or {}
            rid = str(up.get("ref_id") or ref_id or "")
            nova = str(up.get("categoria_nova") or cat)
            ltb = context.user_data.get("last_tx_block") or {}
            items = list(ltb.get("items", []) or [])
            for it in items:
                if str(it.get("ref_id") or "") == rid:
                    it["categoria"] = nova
                    try:
                        key = _normalize_ascii(re.sub(r'\s+', ' ', str(it.get("descricao", ""))).strip())
                        mem = context.user_data.get("cat_memory", {}) or {}
                        mem[key] = nova
                        context.user_data["cat_memory"] = mem
                        try:
                            await asyncio.to_thread(atualizar_memoria_categoria, get_cliente_id(query), key, nova)
                        except:
                            pass
                    except:
                        pass
            resposta = criar_cabecalho("TRANSAÇÃO REGISTRADA", 40)
            resposta += f"\n✅ *{len(items)} transação(ões) registrada(s)*\n\n"
            for it in items:
                tp = str(it.get('tipo', '')).strip().lower()
                emoji = "🔴" if tp in ('saida', '0') else "🟢"
                tipo = "DESPESA" if tp in ('saida', '0') else "RECEITA"
                cat_nome = md_escape(CATEGORY_NAMES.get(it.get('categoria', 'outros'), it.get('categoria', 'outros')))
                desc_json = str(it.get('descricao', ''))
                resposta += f"{emoji} *{tipo}:* {formatar_moeda(float(it.get('valor', 0)))}\n"
                resposta += f"   `{desc_json}`\n"
                resposta += f"   Categoria: {cat_nome}\n\n"
            chat_id = ltb.get("chat_id")
            message_id = ltb.get("message_id")
            if chat_id and message_id:
                kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton("💰 TOTAIS DO DIA", callback_data="total_dia"),
                     InlineKeyboardButton("📅 TOTAIS DO MÊS", callback_data="analise_mes")]
                ])
                await context.bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=resposta, parse_mode='Markdown', reply_markup=kb)
        except:
            pass
        try:
            await context.bot.delete_message(chat_id=query.message.chat_id, message_id=query.message.message_id)
        except:
            pass
    elif query.data.startswith("cat_yes:"):
        try:
            _, ref_id, tipo = query.data.split(":", 2)
        except Exception:
            ref_id, tipo = None, None
        if not (ref_id and tipo):
            await query.edit_message_text("⚠️ Dados inválidos.", parse_mode='Markdown')
            return
        kb = _categoria_keyboard(ref_id, tipo, get_cliente_id(query))
        await query.edit_message_text("Selecione a categoria ou digite uma nova:", parse_mode='Markdown', reply_markup=kb)
    elif query.data.startswith("cat_no:"):
        try:
            _, ref_id, cat = query.data.split(":", 2)
        except Exception:
            ref_id, cat = None, None
        if not (ref_id and cat):
            await query.edit_message_text("⚠️ Dados inválidos.", parse_mode='Markdown')
            return
        try:
            pend = context.user_data.get("pending_confirmations") or {}
            pend.pop(ref_id, None)
            context.user_data["pending_confirmations"] = pend
        except:
            pass
        payload = {
            "cliente_id": get_cliente_id(query),
            "referencia_id": ref_id,
            "nova_categoria": cat,
        }
        try:
            r = requests.post(f"{API_URL}/transacoes/atualizar_categoria", json=payload, timeout=8)
            data = r.json() if r.ok else {"sucesso": False}
        except:
            data = {"sucesso": False}
        if not data.get("sucesso"):
            err = str(data.get("erro", "Falha ao confirmar categoria."))
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("🏠 MENU", callback_data="menu")]])
            await query.edit_message_text(f"⚠️ {md_escape(err)}", parse_mode='Markdown', reply_markup=kb)
            return
        try:
            await context.bot.delete_message(chat_id=query.message.chat_id, message_id=query.message.message_id)
        except:
            pass
    elif query.data.startswith("categoria_digitar:"):
        try:
            _, ref_id, tipo = query.data.split(":", 2)
        except Exception:
            ref_id, tipo = None, None
        if not (ref_id and tipo):
            await query.edit_message_text("⚠️ Dados inválidos.", parse_mode='Markdown')
            return
        try:
            context.user_data["cat_input"] = {"ref_id": ref_id, "tipo": tipo}
        except:
            pass
        try:
            context.user_data["last_cat_prompt_message_id"] = query.message.message_id
            context.user_data["last_cat_prompt_chat_id"] = query.message.chat_id
        except:
            pass
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancelar", callback_data="confirm_categoria_cancelar")]])
        await query.edit_message_text("Digite o nome da categoria:", parse_mode='Markdown', reply_markup=kb)
    elif query.data == "confirm_categoria_cancelar":
        await query.edit_message_text("Operação cancelada.", parse_mode='Markdown')
    elif query.data.startswith("estornar_confirmar:") or query.data.startswith("estornar_escolher:"):
        try:
            rid = query.data.split(":", 1)[1]
            payload = {
                "cliente_id": get_cliente_id(query),
                "referencia_id": rid,
                "motivo": "Estorno confirmado"
            }
            try:
                r = requests.post(f"{API_URL}/ajustes/estornar", json=payload, timeout=8)
                data = r.json() if r.ok else {"sucesso": False}
            except:
                data = {"sucesso": False}
            if not data.get("sucesso"):
                err = str(data.get("erro", "Falha ao estornar. Verifique."))
                kb = InlineKeyboardMarkup([[InlineKeyboardButton("🏠 MENU", callback_data="menu")]])
                await query.edit_message_text(f"⚠️ {md_escape(err)}", parse_mode='Markdown', reply_markup=kb)
                return
            dd = data.get("totais_dia", {})
            mm = data.get("totais_mes", {})
            resp = criar_cabecalho("ESTORNO APLICADO", 40)
            caixa = ""
            caixa += "+" + ("-" * 28) + "+\n"
            caixa += f"|{criar_linha_tabela('DIA - SALDO:', formatar_moeda(dd.get('saldo', 0), negrito=False), True, '', largura=28)}|\n"
            caixa += f"|{criar_linha_tabela('DIA - DESPESAS:', formatar_moeda(dd.get('despesas', 0), negrito=False), True, '', largura=28)}|\n"
            caixa += f"|{criar_linha_tabela('DIA - RECEITAS:', formatar_moeda(dd.get('receitas', 0), negrito=False), True, '', largura=28)}|\n"
            caixa += "+" + ("-" * 28) + "+\n"
            caixa += f"|{criar_linha_tabela('MÊS - SALDO:', formatar_moeda(mm.get('saldo', 0), negrito=False), True, '', largura=28)}|\n"
            caixa += f"|{criar_linha_tabela('MÊS - DESPESAS:', formatar_moeda(mm.get('despesas', 0), negrito=False), True, '', largura=28)}|\n"
            caixa += f"|{criar_linha_tabela('MÊS - RECEITAS:', formatar_moeda(mm.get('receitas', 0), negrito=False), True, '', largura=28)}|\n"
            caixa += "+" + ("-" * 28) + "+"
            resp += wrap_code_block(caixa) + "\n\n"
            if dd.get('estornos', 0) > 0:
                resp += f"🔁 Estornos do dia: {formatar_moeda(dd.get('estornos', 0), negrito=True)}\n"
            if mm.get('estornos', 0) > 0:
                resp += f"🔁 Estornos do mês: {formatar_moeda(mm.get('estornos', 0), negrito=True)}\n"
            resp += "📊 Use /total para ver os totais atualizados"
            await query.edit_message_text(resp, parse_mode='Markdown')
        except:
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("🏠 MENU", callback_data="menu")]])
            try:
                await query.edit_message_text("⚠️ Erro ao aplicar estorno. Tente novamente.", parse_mode='Markdown', reply_markup=kb)
            except:
                pass
    elif query.data.startswith("estornar_filtrar:"):
        parts = query.data.split(":")
        tp = parts[1] if len(parts) > 1 else None
        val = parse_value((parts[2] if len(parts) > 2 else "0").replace("R$", "").strip()) or 0.0
        dr = parts[3] if len(parts) > 3 else _day_key_sp()
        processamento = query.message
        await iniciar_fluxo_estorno_por_valor(query, context, val, dr, tipo=tp, processamento=processamento)
    elif query.data == "estornar_cancelar":
        await query.edit_message_text("Operação cancelada.", parse_mode='Markdown')

# ===== RELATÓRIO DE TOTAIS DO DIA (/total) =====
async def relatorio_total(query, context):
    """Gera relatório simples do DIA ATUAL."""
    data_atual = _now_sp()
    data_str = data_atual.strftime("%d/%m/%Y")
    
    processing_msg = None
    try:
        if hasattr(query, 'edit_message_text'):
            await query.edit_message_text("🔄 Gerando relatório do dia...", parse_mode='Markdown')
        else:
            processing_msg = await query.message.reply_text("🔄 Gerando relatório do dia...", parse_mode='Markdown')
    except:
        processing_msg = None
    try:
        cid = get_cliente_id(query)
        dkey = _day_key_sp()
        mkey = _month_key_sp()
        day_url = f"{API_URL}/saldo/atual?inicio={dkey}&fim={dkey}&{build_cliente_query_params(query)}"
        month_url = f"{API_URL}/saldo/atual?mes={mkey}&{build_cliente_query_params(query)}"
        geral_url = f"{API_URL}/saldo/atual?{build_cliente_query_params(query)}"
        try:
            day_api, mes_api, geral_api = await asyncio.gather(
                _req_json_cached_async(day_url, f"day:{cid}:{dkey}", ttl=10, timeout=4),
                _req_json_cached_async(month_url, f"month:{cid}:{mkey}", ttl=15, timeout=4),
                _req_json_cached_async(geral_url, f"geral:{cid}", ttl=30, timeout=4),
            )
        except:
            day_api = {}
            mes_api = {}
            geral_api = {}
        tot_day = day_api.get("total", {"receitas": 0, "despesas": 0, "saldo": 0})
        if not (float(tot_day.get("receitas", 0) or 0) or float(tot_day.get("despesas", 0) or 0) or float(tot_day.get("ajustes", 0) or 0)):
            try:
                extrato_url = f"{API_URL}/extrato/hoje?include_transacoes=true&{build_cliente_query_params(query)}"
                extrato_api = await _req_json_cached_async(extrato_url, f"extrato:{cid}:{dkey}", ttl=10, timeout=5)
                if extrato_api.get("sucesso"):
                    et = extrato_api.get("total", {}) or {}
                    if float(et.get("receitas", 0) or 0) or float(et.get("despesas", 0) or 0) or float(et.get("ajustes", 0) or 0):
                        tot_day = {
                            "receitas": float(et.get("receitas", 0) or 0),
                            "despesas": float(et.get("despesas", 0) or 0),
                            "saldo": float(et.get("saldo", (et.get("receitas", 0) or 0) - (et.get("despesas", 0) or 0) + (et.get("ajustes", 0) or 0)) or 0),
                        }
                    else:
                        tot_day = {"receitas": 0, "despesas": 0, "saldo": 0}
                else:
                    tot_day = {"receitas": 0, "despesas": 0, "saldo": 0}
            except:
                tot_day = {"receitas": 0, "despesas": 0, "saldo": 0}
        
        resposta = criar_cabecalho("TOTAIS DO DIA", 40)
        resposta += f"\n\n📅 {data_str}\n\n"
        tot = tot_day
        caixa = ""
        caixa += "+" + ("-" * 28) + "+\n"
        caixa += f"|{criar_linha_tabela('RECEITAS:', formatar_moeda(tot.get('receitas', 0), negrito=False), True, '', largura=28)}|\n"
        caixa += f"|{criar_linha_tabela('DESPESAS:', formatar_moeda(tot.get('despesas', 0), negrito=False), True, '', largura=28)}|\n"
        caixa += f"|{criar_linha_tabela('SALDO:', formatar_moeda(tot.get('saldo', 0), negrito=False), True, '', largura=28)}|\n"
        caixa += "+" + ("-" * 28) + "+"
        resposta += wrap_code_block(caixa) + "\n\n"
        try:
            tot_geral = geral_api.get("total", {}) if geral_api.get("sucesso") else {}
            saldo_geral = float(tot_geral.get("saldo_real", tot_geral.get("saldo", 0)) or 0)
        except:
            saldo_geral = obter_saldo_geral(get_cliente_id(query))
        resposta += f"💹 Saldo atual real: {formatar_moeda(saldo_geral, negrito=True)}\n"
        
        keyboard = [
            [InlineKeyboardButton("📊 POR CATEGORIA (HOJE)", callback_data="total_dia_categorias")],
            [InlineKeyboardButton("🏠 MENU PRINCIPAL", callback_data="menu")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        if hasattr(query, 'edit_message_text'):
            await query.edit_message_text(resposta, parse_mode='Markdown', reply_markup=reply_markup)
        elif processing_msg:
            await context.bot.edit_message_text(chat_id=processing_msg.chat_id, message_id=processing_msg.message_id, text=resposta, parse_mode='Markdown', reply_markup=reply_markup)
        else:
            await query.message.reply_text(resposta, parse_mode='Markdown', reply_markup=reply_markup)
    except:
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 Tentar novamente", callback_data="total_dia")],
            [InlineKeyboardButton("🏠 MENU", callback_data="menu")]
        ])
        err_txt = "⚠️ Erro ao gerar relatório do dia. Tente novamente."
        try:
            if hasattr(query, 'edit_message_text'):
                await query.edit_message_text(err_txt, parse_mode='Markdown', reply_markup=kb)
            elif processing_msg:
                await context.bot.edit_message_text(chat_id=processing_msg.chat_id, message_id=processing_msg.message_id, text=err_txt, parse_mode='Markdown', reply_markup=kb)
            else:
                await query.message.reply_text(err_txt, parse_mode='Markdown', reply_markup=kb)
        except:
            pass

# ===== RESUMO FINANCEIRO (/resumo) =====
async def resumo_financeiro(query, context):
    """Gera resumo financeiro simples com DIA vs MÊS."""
    hoje = _now_sp()
    data_str = hoje.strftime("%d/%m/%Y")
    processing_msg = None
    try:
        if hasattr(query, 'edit_message_text'):
            await query.edit_message_text("🔄 Gerando resumo financeiro...", parse_mode='Markdown')
        else:
            processing_msg = await query.message.reply_text("🔄 Gerando resumo financeiro...", parse_mode='Markdown')
    except:
        processing_msg = None
    try:
        cid = get_cliente_id(query)
        dkey = hoje.strftime("%Y-%m-%d")
        mkey = hoje.strftime("%Y-%m")
        day_url = f"{API_URL}/saldo/atual?inicio={dkey}&fim={dkey}&{build_cliente_query_params(query)}"
        month_url = f"{API_URL}/total/mes?{build_cliente_query_params(query)}"
        sum_url = f"{API_URL}/saldo/atual?mes={mkey}&{build_cliente_query_params(query)}"
        cats_group_url = f"{API_URL}/saldo/atual?mes={mkey}&group_by=categoria&{build_cliente_query_params(query)}"
        extrato_url = f"{API_URL}/extrato/hoje?{build_cliente_query_params(query)}"
        consistency_url = f"{API_URL}/health/consistency?{build_cliente_query_params(query)}"
        try:
            day_api, mes_api, sum_api, cats_group_api, extrato_api, cons_api = await asyncio.gather(
                _req_json_cached_async(day_url, f"day:{cid}:{dkey}", ttl=10, timeout=4),
                _req_json_cached_async(month_url, f"month:{cid}:{mkey}", ttl=15, timeout=4),
                _req_json_cached_async(sum_url, f"month-sum:{cid}:{mkey}", ttl=15, timeout=4),
                _req_json_cached_async(cats_group_url, f"monthcatgrp:{cid}:{mkey}", ttl=15, timeout=4),
                _req_json_cached_async(extrato_url, f"extrato:{cid}:{dkey}", ttl=10, timeout=4),
                _req_json_cached_async(consistency_url, f"consistency:{cid}:{mkey}", ttl=10, timeout=4),
            )
        except:
            day_api = {}
            mes_api = {}
            sum_api = {}
            cats_group_api = {}
            extrato_api = {}
            cons_api = {}
        tot_day = day_api.get("total", {"receitas": 0, "despesas": 0, "saldo": 0})
        tot_mes = mes_api.get("total", {"receitas": 0, "despesas": 0, "saldo": 0, "ajustes": 0, "estornos": 0}) if mes_api.get("sucesso") else {"receitas": 0, "despesas": 0, "saldo": 0, "ajustes": 0, "estornos": 0}
        try:
            incons = False
            try:
                incons = not bool(cons_api.get("mes", {}).get("consistente_totais", True))
            except:
                incons = False
            candidates = []
            if mes_api.get("sucesso"):
                candidates.append(("mes", {
                    "receitas": float(tot_mes.get('receitas', 0) or 0),
                    "despesas": float(tot_mes.get('despesas', 0) or 0),
                    "saldo": float(tot_mes.get('saldo', 0) or 0),
                    "estornos": float(tot_mes.get('estornos', 0) or 0),
                    "ajustes": float(tot_mes.get('ajustes', 0) or 0),
                }))
            if cons_api.get("sucesso"):
                soma = cons_api.get("mes", {}).get("soma_dias", {}) or {}
                candidates.append(("soma", {
                    "receitas": float(soma.get('receitas', 0) or 0),
                    "despesas": float(soma.get('despesas', 0) or 0),
                    "saldo": float(soma.get('saldo', 0) or 0),
                    "estornos": float(soma.get('estornos', 0) or 0),
                    "ajustes": float(soma.get('ajustes', 0) or 0),
                }))
            if sum_api.get("sucesso"):
                st = sum_api.get("total", {}) or {}
                candidates.append(("sum", {
                    "receitas": float(st.get('receitas', 0) or 0),
                    "despesas": float(st.get('despesas', 0) or 0),
                    "saldo": float(st.get('saldo', 0) or 0),
                    "estornos": float(st.get('estornos', 0) or 0),
                    "ajustes": float(st.get('ajustes', 0) or 0),
                }))
            if cats_group_api.get("sucesso"):
                cats_grp = cats_group_api.get("categorias", {}) if cats_group_api.get("sucesso") else {}
                ce = cats_grp.get("despesas", {}) if isinstance(cats_grp, dict) else {}
                ci = cats_grp.get("receitas", {}) if isinstance(cats_grp, dict) else {}
                adj = float(cats_group_api.get("total", {}).get("ajustes", 0) or 0) if cats_group_api.get("sucesso") else 0.0
                est = float(cats_group_api.get("total", {}).get("estornos", 0) or 0) if cats_group_api.get("sucesso") else 0.0
                rsum = sum(float(v or 0) for v in ci.values())
                dsum = sum(float(v or 0) for v in ce.values())
                candidates.append(("grp", {
                    "receitas": rsum,
                    "despesas": dsum,
                    "saldo": rsum - dsum + adj,
                    "estornos": est,
                    "ajustes": adj,
                }))
            best = None
            best_score = -1e9
            for name, t in candidates:
                s = 0.0
                if float(t.get("receitas", 0) or 0) > 0:
                    s += 2.0
                if float(t.get("despesas", 0) or 0) > 0:
                    s += 2.0
                if float(t.get("ajustes", 0) or 0) > 0:
                    s += 1.0
                if float(t.get("estornos", 0) or 0) > 0:
                    s += 0.5
                if name == "mes" and incons:
                    s -= 3.0
                if s > best_score:
                    best_score = s
                    best = t
            if best:
                tot_mes = best
        except:
            pass
        resposta = criar_cabecalho("RESUMO FINANCEIRO", 40)
        resposta += f"\n📅 {data_str}\n\n"
        tabela = ""
        tabela += "+" + ("-" * 14) + "+" + ("-" * 14) + "+\n"
        tabela += f"|{'HOJE':^14}|{'ESTE MÊS':^14}|\n"
        tabela += "+" + ("-" * 14) + "+" + ("-" * 14) + "+\n"
        receitas_dia = formatar_moeda(tot_day.get('receitas', 0))
        receitas_mes = formatar_moeda(tot_mes.get('receitas', 0))
        tabela += f"|{receitas_dia:>14}|{receitas_mes:>14}|\n"
        despesas_dia = formatar_moeda(tot_day.get('despesas', 0))
        despesas_mes = formatar_moeda(tot_mes.get('despesas', 0))
        tabela += f"|{despesas_dia:>14}|{despesas_mes:>14}|\n"
        saldo_dia = formatar_moeda(tot_day.get('saldo', 0), negrito=False)
        saldo_mes = formatar_moeda(tot_mes.get('saldo', 0), negrito=False)
        tabela += f"|{saldo_dia:>14}|{saldo_mes:>14}|\n"
        trans_dia = 0
        try:
            if extrato_api.get("sucesso"):
                transacoes_dia = extrato_api.get("transacoes", []) or []
                stats_dia = calcular_estatisticas(transacoes_dia, "HOJE")
                trans_dia = int(stats_dia.get("quantidade", {}).get("total", 0) or 0)
                if trans_dia == 0:
                    qv = int(extrato_api.get("quantidade_transacoes_validas", 0) or 0)
                    trans_dia = qv
        except:
            trans_dia = 0
        trans_mes = int(mes_api.get("quantidade_transacoes_validas", 0) or 0) if mes_api.get("sucesso") else 0
        try:
            if trans_mes == 0 and cons_api.get("sucesso"):
                qv_mes = int(cons_api.get("mes", {}).get("soma_dias", {}).get("quantidade_transacoes_validas", 0) or 0)
                if qv_mes > 0:
                    trans_mes = qv_mes
        except:
            pass
        tabela += f"|{str(trans_dia):>14}|{str(trans_mes):>14}|\n"
        tabela += "+" + ("-" * 14) + "+" + ("-" * 14) + "+\n"
        resposta += wrap_code_block(tabela) + "\n"
        try:
            saldo_geral = float((sum_api.get("total", {}) or {}).get("saldo_real", (sum_api.get("total", {}) or {}).get("saldo", 0)) or 0) if sum_api.get("sucesso") else obter_saldo_geral(get_cliente_id(query))
        except:
            saldo_geral = obter_saldo_geral(get_cliente_id(query))
        resposta += f"💹 Saldo atual real: {formatar_moeda(saldo_geral, negrito=True)}\n"
        keyboard = [
            [InlineKeyboardButton("💰 TOTAIS DO DIA", callback_data="total_dia")],
            [InlineKeyboardButton("🏠 MENU PRINCIPAL", callback_data="menu")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        if hasattr(query, 'edit_message_text'):
            await query.edit_message_text(resposta, parse_mode='Markdown', reply_markup=reply_markup)
        elif processing_msg:
            await context.bot.edit_message_text(chat_id=processing_msg.chat_id, message_id=processing_msg.message_id, text=resposta, parse_mode='Markdown', reply_markup=reply_markup)
        else:
            await query.message.reply_text(resposta, parse_mode='Markdown', reply_markup=reply_markup)
    except:
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 Tentar novamente", callback_data="resumo")],
            [InlineKeyboardButton("🏠 MENU", callback_data="menu")]
        ])
        err_txt = "⚠️ Erro ao gerar resumo. Tente novamente."
        try:
            if hasattr(query, 'edit_message_text'):
                await query.edit_message_text(err_txt, parse_mode='Markdown', reply_markup=kb)
            elif processing_msg:
                await context.bot.edit_message_text(chat_id=processing_msg.chat_id, message_id=processing_msg.message_id, text=err_txt, parse_mode='Markdown', reply_markup=kb)
            else:
                await query.message.reply_text(err_txt, parse_mode='Markdown', reply_markup=kb)
        except:
            pass

# ===== RELATÓRIO DIÁRIO DETALHADO =====
async def relatorio_diario(query, context):
    """Gera relatório simples do dia."""
    hoje = _now_sp()
    data_str = hoje.strftime("%d/%m/%Y")
    processing_msg = None
    try:
        if hasattr(query, 'edit_message_text'):
            await query.edit_message_text("🔄 Gerando relatório detalhado do dia...", parse_mode='Markdown')
        else:
            processing_msg = await query.message.reply_text("🔄 Gerando relatório detalhado do dia...", parse_mode='Markdown')
    except:
        processing_msg = None
    try:
        try:
            extrato = requests.get(f"{API_URL}/extrato/hoje?{build_cliente_query_params(query)}", timeout=5).json()
        except:
            extrato = {"sucesso": False}
        transacoes_dia = extrato.get("transacoes", []) if extrato.get("sucesso") else []
        if not transacoes_dia:
            resposta = criar_cabecalho("RELATÓRIO DIÁRIO", 40)
            resposta += f"\n\n📅 *Data:* {data_str}\n"
            resposta += "=" * 40 + "\n\n"
            resposta += "📭 *Nenhuma transação hoje*\n\n"
            resposta += "💡 *Use o comando /total para ver os totais*"
            keyboard = [[InlineKeyboardButton("💰 VER TOTAIS", callback_data="total_dia")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            if hasattr(query, 'edit_message_text'):
                await query.edit_message_text(resposta, parse_mode='Markdown', reply_markup=reply_markup)
            elif processing_msg:
                await context.bot.edit_message_text(chat_id=processing_msg.chat_id, message_id=processing_msg.message_id, text=resposta, parse_mode='Markdown', reply_markup=reply_markup)
            else:
                await query.message.reply_text(resposta, parse_mode='Markdown', reply_markup=reply_markup)
            return
        stats_dia = calcular_estatisticas(transacoes_dia, "HOJE")
        resposta = criar_cabecalho("RELATÓRIO DO DIA", 40)
        resposta += f"\n📅 {data_str}\n\n"
        caixa = ""
        caixa += "+" + ("-" * 28) + "+\n"
        caixa += f"|{criar_linha_tabela('RECEITAS:', formatar_moeda(stats_dia['total_receitas'], negrito=False), True, '', largura=28)}|\n"
        caixa += f"|{criar_linha_tabela('DESPESAS:', formatar_moeda(stats_dia['total_despesas'], negrito=False), True, '', largura=28)}|\n"
        caixa += f"|{criar_linha_tabela('SALDO:', formatar_moeda(stats_dia['saldo'], negrito=False), True, '', largura=28)}|\n"
        caixa += "+" + ("-" * 28) + "+"
        resposta += wrap_code_block(caixa) + "\n"
        saldo_geral = obter_saldo_geral(get_cliente_id(query))
        resposta += f"💹 Saldo atual real: {formatar_moeda(saldo_geral, negrito=True)}\n"
        keyboard = [
            [InlineKeyboardButton("🏠 MENU", callback_data="menu")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        if hasattr(query, 'edit_message_text'):
            await query.edit_message_text(resposta, parse_mode='Markdown', reply_markup=reply_markup)
        elif processing_msg:
            await context.bot.edit_message_text(chat_id=processing_msg.chat_id, message_id=processing_msg.message_id, text=resposta, parse_mode='Markdown', reply_markup=reply_markup)
        else:
            await query.message.reply_text(resposta, parse_mode='Markdown', reply_markup=reply_markup)
    except:
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 Tentar novamente", callback_data="relatorio_dia")],
            [InlineKeyboardButton("🏠 MENU", callback_data="menu")]
        ])
        err_txt = "⚠️ Erro ao gerar relatório do dia. Tente novamente."
        try:
            if hasattr(query, 'edit_message_text'):
                await query.edit_message_text(err_txt, parse_mode='Markdown', reply_markup=kb)
            elif processing_msg:
                await context.bot.edit_message_text(chat_id=processing_msg.chat_id, message_id=processing_msg.message_id, text=err_txt, parse_mode='Markdown', reply_markup=kb)
            else:
                await query.message.reply_text(err_txt, parse_mode='Markdown', reply_markup=kb)
        except:
            pass

# ===== ANÁLISE MENSAL =====
async def analise_mensal(query, context):
    """Gera análise mensal detalhada."""
    hoje = _now_sp()
    data_str = _mes_ano_pt(hoje)
    dias_no_mes = calendar.monthrange(hoje.year, hoje.month)[1]
    dias_decorridos = hoje.day
    
    processing_msg = None
    try:
        if hasattr(query, 'edit_message_text'):
            await query.edit_message_text("🔄 Gerando análise mensal...", parse_mode='Markdown')
        else:
            processing_msg = await query.message.reply_text("🔄 Gerando análise mensal...", parse_mode='Markdown')
    except:
        processing_msg = None
    try:
        cid = get_cliente_id(query)
        mkey = hoje.strftime("%Y-%m")
        url_total = f"{API_URL}/total/mes?{build_cliente_query_params(query)}"
        url_sum = f"{API_URL}/saldo/atual?mes={mkey}&{build_cliente_query_params(query)}"
        url_cats_group = f"{API_URL}/saldo/atual?mes={mkey}&group_by=categoria&{build_cliente_query_params(query)}"
        consistency_url = f"{API_URL}/health/consistency?{build_cliente_query_params(query)}"
        try:
            total_api, sum_api, cats_group_api, cons_api = await asyncio.gather(
                _req_json_cached_async(url_total, f"month-total:{cid}:{mkey}", ttl=15, timeout=4),
                _req_json_cached_async(url_sum, f"month-sum:{cid}:{mkey}", ttl=15, timeout=4),
                _req_json_cached_async(url_cats_group, f"monthcatgrp:{cid}:{mkey}", ttl=15, timeout=4),
                _req_json_cached_async(consistency_url, f"consistency:{cid}:{mkey}", ttl=10, timeout=4),
            )
        except:
            total_api = {}
            sum_api = {}
            cats_group_api = {}
            cons_api = {}
        tm = total_api.get("total", {"receitas": 0, "despesas": 0, "saldo": 0, "ajustes": 0, "estornos": 0}) if total_api.get("sucesso") else {"receitas": 0, "despesas": 0, "saldo": 0, "ajustes": 0, "estornos": 0}
        categorias_desp = {}
        estornos_mes = tm.get("estornos", 0) if total_api.get("sucesso") else (float((cats_group_api.get("total", {}) or {}).get("estornos", 0) or 0) if cats_group_api.get("sucesso") else 0)
        try:
            incons = False
            try:
                incons = not bool(cons_api.get("mes", {}).get("consistente_totais", True))
            except:
                incons = False
            candidates = []
            if total_api.get("sucesso"):
                candidates.append(("mes", {
                    "receitas": float(tm.get('receitas', 0) or 0),
                    "despesas": float(tm.get('despesas', 0) or 0),
                    "saldo": float(tm.get('saldo', 0) or 0),
                    "estornos": float(tm.get('estornos', 0) or 0),
                    "ajustes": float(tm.get('ajustes', 0) or 0),
                }))
            if cons_api.get("sucesso"):
                soma = cons_api.get("mes", {}).get("soma_dias", {}) or {}
                candidates.append(("soma", {
                    "receitas": float(soma.get('receitas', 0) or 0),
                    "despesas": float(soma.get('despesas', 0) or 0),
                    "saldo": float(soma.get('saldo', 0) or 0),
                    "estornos": float(soma.get('estornos', 0) or 0),
                    "ajustes": float(soma.get('ajustes', 0) or 0),
                }))
            if sum_api.get("sucesso"):
                st = sum_api.get("total", {}) or {}
                candidates.append(("sum", {
                    "receitas": float(st.get('receitas', 0) or 0),
                    "despesas": float(st.get('despesas', 0) or 0),
                    "saldo": float(st.get('saldo', 0) or 0),
                    "estornos": float(st.get('estornos', 0) or 0),
                    "ajustes": float(st.get('ajustes', 0) or 0),
                }))
            if cats_group_api.get("sucesso"):
                cats_grp = cats_group_api.get("categorias", {}) if cats_group_api.get("sucesso") else {}
                ce = cats_grp.get("despesas", {}) if isinstance(cats_grp, dict) else {}
                ci = cats_grp.get("receitas", {}) if isinstance(cats_grp, dict) else {}
                adj = float(cats_group_api.get("total", {}).get("ajustes", 0) or 0) if cats_group_api.get("sucesso") else 0.0
                est = float(cats_group_api.get("total", {}).get("estornos", 0) or 0) if cats_group_api.get("sucesso") else 0.0
                rsum = sum(float(v or 0) for v in ci.values())
                dsum = sum(float(v or 0) for v in ce.values())
                candidates.append(("grp", {
                    "receitas": rsum,
                    "despesas": dsum,
                    "saldo": rsum - dsum + adj,
                    "estornos": est,
                    "ajustes": adj,
                }))
            best = None
            best_name = None
            best_score = -1e9
            for name, t in candidates:
                s = 0.0
                if float(t.get("receitas", 0) or 0) > 0:
                    s += 2.0
                if float(t.get("despesas", 0) or 0) > 0:
                    s += 2.0
                if float(t.get("ajustes", 0) or 0) > 0:
                    s += 1.0
                if float(t.get("estornos", 0) or 0) > 0:
                    s += 0.5
                if name == "mes" and incons:
                    s -= 3.0
                if s > best_score:
                    best_score = s
                    best = t
                    best_name = name
            if best:
                tm = best
                estornos_mes = float(tm.get("estornos", 0) or 0)
                if best_name in ("soma", "sum", "grp"):
                    try:
                        cats_grp2 = cats_group_api.get("categorias", {}) if cats_group_api.get("sucesso") else {}
                        despesas_g = cats_grp2.get("despesas", {}) if isinstance(cats_grp2, dict) else {}
                        estornos_g = cats_grp2.get("estornos", {}) if isinstance(cats_grp2, dict) else {}
                        categorias_desp = {k: float(despesas_g.get(k, 0) or 0) - float(estornos_g.get(k, 0) or 0) for k in set(list(despesas_g.keys()) + list(estornos_g.keys()))}
                        categorias_desp = {k: float(v or 0) for k, v in categorias_desp.items() if float(v or 0) > 0}
                    except:
                        categorias_desp = {}
                else:
                    try:
                        cats_grp2 = cats_group_api.get("categorias", {}) if cats_group_api.get("sucesso") else {}
                        despesas_g = cats_grp2.get("despesas", {}) if isinstance(cats_grp2, dict) else {}
                        estornos_g = cats_grp2.get("estornos", {}) if isinstance(cats_grp2, dict) else {}
                        categorias_desp = {k: float(despesas_g.get(k, 0) or 0) - float(estornos_g.get(k, 0) or 0) for k in set(list(despesas_g.keys()) + list(estornos_g.keys()))}
                        categorias_desp = {k: float(v or 0) for k, v in categorias_desp.items() if float(v or 0) > 0}
                    except:
                        categorias_desp = {}
        except:
            pass
        # tm já está baseado em /total/mes, que grava os valores corretos no documento do mês
        stats_mes = {
            'periodo': "ESTE MÊS",
            'total_receitas': tm.get('receitas', 0),
            'total_despesas': tm.get('despesas', 0),
            'saldo': tm.get('saldo', 0),
            'categorias_despesas': categorias_desp
        }
        try:
            _sum_cat = sum(float(v or 0) for v in (stats_mes.get('categorias_despesas', {}) or {}).values())
            _tot_des = float(stats_mes.get('total_despesas', 0) or 0)
            if _sum_cat > 0 and _tot_des > 0 and abs(_sum_cat - _tot_des) > 1e-6:
                _factor = _tot_des / _sum_cat
                stats_mes['categorias_despesas'] = {k: float(v or 0) * _factor for k, v in (stats_mes.get('categorias_despesas', {}) or {}).items()}
        except:
            pass
        
        resposta = criar_cabecalho("ANÁLISE MENSAL", 40)
        resposta += f"\n📅 {data_str}\n\n"
        caixa = ""
        caixa += "+" + ("-" * 28) + "+\n"
        caixa += f"|{criar_linha_tabela('RECEITAS:', formatar_moeda(stats_mes['total_receitas'], negrito=False), True, '', largura=28)}|\n"
        caixa += f"|{criar_linha_tabela('DESPESAS:', formatar_moeda(stats_mes['total_despesas'], negrito=False), True, '', largura=28)}|\n"
        caixa += f"|{criar_linha_tabela('SALDO:', formatar_moeda(stats_mes['saldo'], negrito=False), True, '', largura=28)}|\n"
        caixa += "+" + ("-" * 28) + "+"
        resposta += wrap_code_block(caixa) + "\n"
        try:
            saldo_geral = float((sum_api.get("total", {}) or {}).get("saldo_real", (sum_api.get("total", {}) or {}).get("saldo", 0)) or 0) if sum_api.get("sucesso") else obter_saldo_geral(get_cliente_id(query))
        except:
            saldo_geral = obter_saldo_geral(get_cliente_id(query))
        resposta += f"💹 Saldo atual real: {formatar_moeda(saldo_geral, negrito=True)}\n"
        
        keyboard = [
            [InlineKeyboardButton("📊 POR CATEGORIA (MÊS)", callback_data="analise_mes_categorias")],
            [InlineKeyboardButton("🏠 MENU PRINCIPAL", callback_data="menu")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        if hasattr(query, 'edit_message_text'):
            await query.edit_message_text(resposta, parse_mode='Markdown', reply_markup=reply_markup)
        elif processing_msg:
            await context.bot.edit_message_text(chat_id=processing_msg.chat_id, message_id=processing_msg.message_id, text=resposta, parse_mode='Markdown', reply_markup=reply_markup)
        else:
            await query.message.reply_text(resposta, parse_mode='Markdown', reply_markup=reply_markup)
    except:
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 Tentar novamente", callback_data="analise_mes")],
            [InlineKeyboardButton("🏠 MENU", callback_data="menu")]
        ])
        err_txt = "⚠️ Erro ao gerar análise. Tente novamente."
        try:
            if hasattr(query, 'edit_message_text'):
                await query.edit_message_text(err_txt, parse_mode='Markdown', reply_markup=kb)
            elif processing_msg:
                await context.bot.edit_message_text(chat_id=processing_msg.chat_id, message_id=processing_msg.message_id, text=err_txt, parse_mode='Markdown', reply_markup=kb)
            else:
                await query.message.reply_text(err_txt, parse_mode='Markdown', reply_markup=kb)
        except:
            pass
async def categorias_dia(query, context):
    hoje = _now_sp()
    dkey = _day_key_sp()
    data_str = hoje.strftime("%d/%m/%Y")
    processing_msg = None
    try:
        if hasattr(query, 'edit_message_text'):
            await query.edit_message_text("🔄 Gerando categorias do dia...", parse_mode='Markdown')
        else:
            processing_msg = await query.message.reply_text("🔄 Gerando categorias do dia...", parse_mode='Markdown')
    except:
        processing_msg = None
    try:
        qs = build_cliente_query_params(query)
        cid = get_cliente_id(query)
        geral_url = f"{API_URL}/saldo/atual?{qs}"
        cat_url = f"{API_URL}/saldo/atual?inicio={dkey}&fim={dkey}&group_by=categoria&{qs}"
        extrato_url = f"{API_URL}/extrato/hoje?include_transacoes=true&{qs}"
        try:
            geral_api, cat_api, extrato_api = await asyncio.gather(
                _req_json_cached_async(geral_url, f"geral:{cid}", ttl=20, timeout=4),
                _req_json_cached_async(cat_url, f"daycats:{cid}:{dkey}", ttl=10, timeout=4),
                _req_json_cached_async(extrato_url, f"extrato:{cid}:{dkey}", ttl=8, timeout=5),
            )
        except:
            geral_api = {}
            cat_api = {}
            extrato_api = {}
        transacoes = extrato_api.get("transacoes", []) if extrato_api.get("sucesso") else []
        grupos = {}
        for t in transacoes or []:
            if t.get('estornado'):
                continue
            tp_raw = str(t.get('tipo', '')).strip().lower()
            if tp_raw not in ('0', 'despesa', 'saida', '1', 'receita', 'entrada'):
                continue
            cat = str(t.get('categoria', 'outros') or 'outros').strip().lower()
            grupos.setdefault(cat, []).append({
                "tipo": ('saida' if tp_raw in ('0', 'despesa', 'saida') else 'entrada'),
                "valor": float(t.get('valor', 0) or 0),
                "descricao": str(t.get('descricao', '') or '')
            })
        def _tot_cat(lst):
            d = sum(float(it.get('valor', 0) or 0) for it in lst if it.get('tipo') == 'saida')
            r = sum(float(it.get('valor', 0) or 0) for it in lst if it.get('tipo') == 'entrada')
            return r - d
        ordenadas = sorted(((k, v) for k, v in grupos.items()), key=lambda x: (-sum(float(it.get('valor', 0) or 0) for it in x[1] if it.get('tipo') == 'saida'), x[0]))
        ordenadas = ordenadas[:6] if len(ordenadas) > 6 else ordenadas
        resposta = criar_cabecalho("CATEGORIAS DO DIA", 40)
        resposta += f"\n📅 {data_str}\n\n"
        tot_periodo = extrato_api.get("total", {}) if extrato_api.get("sucesso") else {}
        tot_despesas = float(tot_periodo.get("despesas", 0) or 0)
        tot_receitas = float(tot_periodo.get("receitas", 0) or 0)
        def _pct(v, tot):
            try:
                return f"{(float(v or 0) / float(tot or 1)) * 100:.1f}%"
            except:
                return "0.0%"
        for k, lst in ordenadas:
            pass
        mapa_desp = {}
        mapa_rec = {}
        for k, lst in grupos.items():
            dd = sum(float(it.get('valor', 0) or 0) for it in lst if it.get('tipo') == 'saida')
            rr = sum(float(it.get('valor', 0) or 0) for it in lst if it.get('tipo') == 'entrada')
            if dd > 0:
                mapa_desp[k] = dd
            if rr > 0:
                mapa_rec[k] = rr
        desp_sorted = sorted(mapa_desp.items(), key=lambda x: -x[1])
        rec_sorted = sorted(mapa_rec.items(), key=lambda x: -x[1])
        labels = [CATEGORY_NAMES.get(k, k) for k, _ in (desp_sorted + rec_sorted)]
        max_label = max((len(x) for x in labels), default=12)
        FS = "\u2007"
        def pad(label):
            return label + (FS * max(0, max_label - len(label)))
        resposta += "DESPESAS\n"
        for k, v in desp_sorted:
            label = CATEGORY_NAMES.get(k, k)
            resposta += f"  {pad(label)}{formatar_moeda(v, negrito=False)}\n"
        resposta += "\nRECEITAS\n"
        for k, v in rec_sorted:
            label = CATEGORY_NAMES.get(k, k)
            resposta += f"  {pad(label)}+{formatar_moeda(v, negrito=False)}\n"
        saldo_dia = tot_receitas - tot_despesas
        resposta += f"\nSaldo do dia: {formatar_moeda(saldo_dia, negrito=True)}\n"
        try:
            tot_geral = geral_api.get("total", {}) if geral_api.get("sucesso") else {}
            saldo_geral = float(tot_geral.get("saldo_real", tot_geral.get("saldo", 0)) or 0)
        except:
            saldo_geral = obter_saldo_geral(get_cliente_id(query))
        resposta += f"💹 Saldo atual real: {formatar_moeda(saldo_geral, negrito=True)}\n"
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("⬇️ Expandir detalhes", callback_data="catdia_expand")],
            [InlineKeyboardButton("⬅️ Voltar", callback_data="total_dia")],
            [InlineKeyboardButton("🏠 MENU", callback_data="menu")],
        ])
        if hasattr(query, 'edit_message_text'):
            await query.edit_message_text(resposta, parse_mode='Markdown', reply_markup=kb)
        elif processing_msg:
            await context.bot.edit_message_text(chat_id=processing_msg.chat_id, message_id=processing_msg.message_id, text=resposta, parse_mode='Markdown', reply_markup=kb)
        else:
            await query.message.reply_text(resposta, parse_mode='Markdown', reply_markup=kb)
    except:
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 Tentar novamente", callback_data="total_dia_categorias")],
            [InlineKeyboardButton("🏠 MENU", callback_data="menu")],
        ])
        err_txt = "⚠️ Erro ao carregar categorias do dia."
        try:
            if hasattr(query, 'edit_message_text'):
                await query.edit_message_text(err_txt, parse_mode='Markdown', reply_markup=kb)
            elif processing_msg:
                await context.bot.edit_message_text(chat_id=processing_msg.chat_id, message_id=processing_msg.message_id, text=err_txt, parse_mode='Markdown', reply_markup=kb)
            else:
                await query.message.reply_text(err_txt, parse_mode='Markdown', reply_markup=kb)
        except:
            pass
async def categorias_mes(query, context):
    hoje = _now_sp()
    mkey = _month_key_sp()
    data_str = _mes_ano_pt(hoje)
    processing_msg = None
    try:
        if hasattr(query, 'edit_message_text'):
            await query.edit_message_text("🔄 Gerando categorias do mês...", parse_mode='Markdown')
        else:
            processing_msg = await query.message.reply_text("🔄 Gerando categorias do mês...", parse_mode='Markdown')
    except:
        processing_msg = None
    try:
        qs = build_cliente_query_params(query)
        cid = get_cliente_id(query)
        db = get_db()
        root = db.collection('clientes').document(str(cid))
        try:
            ano, m = mkey.split("-")
        except:
            ano, m = hoje.strftime("%Y"), hoje.strftime("%m")
        dt_ini = f"{ano}-{m}-01"
        dt_fim = f"{int(ano)+1}-01-01" if m == "12" else f"{ano}-{int(m)+1:02d}-01"
        transacoes = []
        try:
            q = root.collection('transacoes').where('data_referencia', '>=', dt_ini).where('data_referencia', '<', dt_fim)
            docs = []
            try:
                docs = q.stream()
            except:
                docs = []
            idx = {}
            tl = []
            for d in docs:
                o = d.to_dict() or {}
                k = str(o.get('ref_id') or '') or (str(o.get('tipo', '')) + '|' + str(float(o.get('valor', 0) or 0)) + '|' + str(o.get('categoria', '')) + '|' + str(o.get('descricao', '')) + '|' + str(o.get('timestamp_criacao', '')))
                if not idx.get(k):
                    idx[k] = 1
                    tl.append(o)
            if not tl:
                cur = datetime.strptime(dt_ini, "%Y-%m-%d")
                end = datetime.strptime(dt_fim, "%Y-%m-%d")
                while cur < end:
                    dkey = cur.strftime("%Y-%m-%d")
                    try:
                        items = root.collection('transacoes').document(dkey).collection('items').stream()
                    except:
                        items = []
                    try:
                        tops = root.collection('transacoes').where('data_referencia', '==', dkey).stream()
                    except:
                        tops = []
                    for d in items:
                        o = d.to_dict() or {}
                        k = str(o.get('ref_id') or '') or (str(o.get('tipo', '')) + '|' + str(float(o.get('valor', 0) or 0)) + '|' + str(o.get('categoria', '')) + '|' + str(o.get('descricao', '')) + '|' + str(o.get('timestamp_criacao', '')))
                        if not idx.get(k):
                            idx[k] = 1
                            tl.append(o)
                    for d in tops:
                        o = d.to_dict() or {}
                        k = str(o.get('ref_id') or '') or (str(o.get('tipo', '')) + '|' + str(float(o.get('valor', 0) or 0)) + '|' + str(o.get('categoria', '')) + '|' + str(o.get('descricao', '')) + '|' + str(o.get('timestamp_criacao', '')))
                        if not idx.get(k):
                            idx[k] = 1
                            tl.append(o)
                    cur = cur + timedelta(days=1)
            transacoes = tl
        except:
            transacoes = []
        despesas = {}
        receitas = {}
        for t in transacoes or []:
            if t.get('estornado'):
                continue
            tp_raw = str(t.get('tipo', '')).strip().lower()
            if tp_raw not in ('0', 'despesa', 'saida', '1', 'receita', 'entrada'):
                continue
            cat = str(t.get('categoria', 'outros') or 'outros').strip().lower()
            val = float(t.get('valor', 0) or 0)
            if tp_raw in ('0', 'despesa', 'saida'):
                despesas[cat] = float(despesas.get(cat, 0) or 0) + val
            else:
                receitas[cat] = float(receitas.get(cat, 0) or 0) + val
        despesas = {k: float(v or 0) for k, v in despesas.items() if float(v or 0) > 0}
        receitas = {k: float(v or 0) for k, v in receitas.items() if float(v or 0) > 0}
        saldo_mes = 0.0
        try:
            total_mes_url = f"{API_URL}/total/mes?mes={mkey}&{qs}"
            tm_api = await _req_json_cached_async(total_mes_url, f"tmes:{cid}:{mkey}", ttl=15, timeout=5)
            tot_all = tm_api.get("total", {}) if tm_api.get("sucesso") else {}
            saldo_mes = float(tot_all.get("saldo", (sum(receitas.values()) - sum(despesas.values()))) or (sum(receitas.values()) - sum(despesas.values())))
        except:
            saldo_mes = sum(receitas.values()) - sum(despesas.values())
        resposta = "📊 *RELATÓRIO DE CATEGORIAS*\n"
        resposta += f"📅 {data_str}\n\n"
        if not despesas and not receitas and (saldo_mes == 0):
            err_msg = "Nenhum dado encontrado para este mês."
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("⬅️ Voltar", callback_data="analise_mes")],
                [InlineKeyboardButton("🏠 MENU", callback_data="menu")],
            ])
            if hasattr(query, 'edit_message_text'):
                await query.edit_message_text(err_msg, parse_mode='Markdown', reply_markup=kb)
            elif processing_msg:
                await context.bot.edit_message_text(chat_id=processing_msg.chat_id, message_id=processing_msg.message_id, text=err_msg, parse_mode='Markdown', reply_markup=kb)
            else:
                await query.message.reply_text(err_msg, parse_mode='Markdown', reply_markup=kb)
            return
        desp_sorted = sorted(despesas.items(), key=lambda x: -x[1])
        rec_sorted = sorted(receitas.items(), key=lambda x: -x[1])
        largura = 28
        tabela = ""
        tabela += "DESPESAS\n"
        tabela += ("-" * 3) + "\n"
        for k, v in desp_sorted:
            label = CATEGORY_NAMES.get(k, k).upper()
            linha = criar_linha_tabela(f"{label}", formatar_moeda(v, negrito=False), True, "", largura=largura)
            tabela += f"{linha}\n"
        if rec_sorted:
            tabela += "\nRECEITAS\n"
            tabela += ("-" * 3) + "\n"
            for k, v in rec_sorted:
                label = CATEGORY_NAMES.get(k, k).upper()
                linha = criar_linha_tabela(f"{label}", f"+{formatar_moeda(v, negrito=False)}", True, "", largura=largura)
                tabela += f"{linha}\n"
        linha_saldo = criar_linha_tabela("SALDO DO MÊS:", formatar_moeda(saldo_mes, negrito=False), True, "", largura=largura)
        tabela += f"\n{linha_saldo}"
        resposta += wrap_code_block(tabela) + "\n"
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("⬇️ Expandir detalhes", callback_data="catmes_expand")],
            [InlineKeyboardButton("⬅️ Voltar", callback_data="analise_mes")],
            [InlineKeyboardButton("🏠 MENU", callback_data="menu")],
        ])
        if hasattr(query, 'edit_message_text'):
            await query.edit_message_text(resposta, parse_mode='Markdown', reply_markup=kb)
        elif processing_msg:
            await context.bot.edit_message_text(chat_id=processing_msg.chat_id, message_id=processing_msg.message_id, text=resposta, parse_mode='Markdown', reply_markup=kb)
        else:
            await query.message.reply_text(resposta, parse_mode='Markdown', reply_markup=kb)
    except:
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 Tentar novamente", callback_data="analise_mes_categorias")],
            [InlineKeyboardButton("🏠 MENU", callback_data="menu")],
        ])
        err_txt = "⚠️ Erro ao carregar categorias do mês."
        try:
            if hasattr(query, 'edit_message_text'):
                await query.edit_message_text(err_txt, parse_mode='Markdown', reply_markup=kb)
            elif processing_msg:
                await context.bot.edit_message_text(chat_id=processing_msg.chat_id, message_id=processing_msg.message_id, text=err_txt, parse_mode='Markdown', reply_markup=kb)
            else:
                await query.message.reply_text(err_txt, parse_mode='Markdown', reply_markup=kb)
        except:
            pass

async def detalhar_categoria_dia(query, context, categoria_key: str, off_saida: int = 0, off_entrada: int = 0):
    hoje = _now_sp()
    dkey = _day_key_sp()
    data_str = hoje.strftime("%d/%m/%Y")
    try:
        qs = build_cliente_query_params(query)
        cid = get_cliente_id(query)
        geral_url = f"{API_URL}/saldo/atual?{qs}"
        extrato_url = f"{API_URL}/extrato/hoje?include_transacoes=true&{qs}"
        try:
            geral_api, extrato_api = await asyncio.gather(
                _req_json_cached_async(geral_url, f"geral:{cid}", ttl=20, timeout=4),
                _req_json_cached_async(extrato_url, f"extrato:{cid}:{dkey}", ttl=8, timeout=5),
            )
        except:
            geral_api = {}
            extrato_api = {}
        transacoes = extrato_api.get("transacoes", []) if extrato_api.get("sucesso") else []
        tot_periodo = extrato_api.get("total", {}) if extrato_api.get("sucesso") else {}
        tot_despesas = float(tot_periodo.get("despesas", 0) or 0)
        tot_receitas = float(tot_periodo.get("receitas", 0) or 0)
        cslug = str(categoria_key or "outros").strip().lower()
        itens = []
        for t in transacoes or []:
            if t.get("estornado"):
                continue
            tp_raw = str(t.get("tipo", "")).strip().lower()
            if tp_raw not in ("0","despesa","saida","1","receita","entrada"):
                continue
            cat = str(t.get("categoria","outros") or "outros").strip().lower()
            if cat != cslug:
                continue
            itens.append({
                "tipo": ("saida" if tp_raw in ("0","despesa","saida") else "entrada"),
                "valor": float(t.get("valor",0) or 0),
                "descricao": str(t.get("descricao","") or "")
            })
        saida = [x for x in itens if x['tipo'] == 'saida']
        entrada = [x for x in itens if x['tipo'] == 'entrada']
        label = CATEGORY_NAMES.get(cslug, cslug)
        desp_cat = sum(float(it.get('valor', 0) or 0) for it in saida)
        rec_cat = sum(float(it.get('valor', 0) or 0) for it in entrada)
        def _pct(v, tot):
            try:
                return f"{(float(v or 0) / float(tot or 1)) * 100:.1f}%"
            except:
                return "0.0%"
        resposta = criar_cabecalho("DETALHE DA CATEGORIA (DIA)", 40)
        resposta += f"\n📅 {data_str}\n"
        resposta += f"📌 *{label}*\n\n"
        resposta += f"🔴 Despesas: {formatar_moeda(desp_cat, negrito=True)} — {_pct(desp_cat, tot_despesas)} do dia\n"
        PAGE = 10
        s_sorted = sorted(saida, key=lambda x: -float(x['valor']))
        e_sorted = sorted(entrada, key=lambda x: -float(x['valor']))
        for it in s_sorted[off_saida:off_saida+PAGE]:
            resposta += f"   🔴 {formatar_moeda(it['valor'])} — {md_escape(it['descricao'])}\n"
        resposta += "\n"
        resposta += f"🟢 Receitas: {formatar_moeda(rec_cat, negrito=True)} — {_pct(rec_cat, tot_receitas)} do dia\n"
        for it in e_sorted[off_entrada:off_entrada+PAGE]:
            resposta += f"   🟢 {formatar_moeda(it['valor'])} — {md_escape(it['descricao'])}\n"
        resposta += "\n"
        qtd_s = len(saida)
        qtd_e = len(entrada)
        media_s = (desp_cat / qtd_s) if qtd_s else 0.0
        media_e = (rec_cat / qtd_e) if qtd_e else 0.0
        max_s = max([float(x['valor']) for x in saida], default=0.0)
        max_e = max([float(x['valor']) for x in entrada], default=0.0)
        resposta += f"📈 Estatísticas\n"
        resposta += f"   🔴 Itens: {qtd_s} | Média: {formatar_moeda(media_s)} | Máx: {formatar_moeda(max_s)}\n"
        resposta += f"   🟢 Itens: {qtd_e} | Média: {formatar_moeda(media_e)} | Máx: {formatar_moeda(max_e)}\n\n"
        try:
            tot_geral = geral_api.get("total", {}) if geral_api.get("sucesso") else {}
            saldo_geral = float(tot_geral.get("saldo_real", tot_geral.get("saldo", 0)) or 0)
        except:
            saldo_geral = obter_saldo_geral(get_cliente_id(query))
        resposta += f"💹 Saldo atual real: {formatar_moeda(saldo_geral, negrito=True)}\n"
        btns = []
        more_s = off_saida + PAGE < len(saida)
        more_e = off_entrada + PAGE < len(entrada)
        row = []
        if more_s:
            rem_s = len(saida) - (off_saida + PAGE)
            row.append(InlineKeyboardButton(f"Ver mais despesas (+{rem_s})", callback_data=f"catdia_more_saida:{cslug}:{off_saida+PAGE}:{off_entrada}"))
        if more_e:
            rem_e = len(entrada) - (off_entrada + PAGE)
            row.append(InlineKeyboardButton(f"Ver mais receitas (+{rem_e})", callback_data=f"catdia_more_entrada:{cslug}:{off_saida}:{off_entrada+PAGE}"))
        if row:
            btns.append(row)
        btns.append([InlineKeyboardButton("⬅️ Voltar categorias (dia)", callback_data="total_dia_categorias")])
        btns.append([InlineKeyboardButton("💰 TOTAIS DO DIA", callback_data="total_dia")])
        btns.append([InlineKeyboardButton("🏠 MENU", callback_data="menu")])
        kb = InlineKeyboardMarkup(btns)
        await query.edit_message_text(resposta, parse_mode='Markdown', reply_markup=kb)
    except:
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("⬅️ Voltar categorias (dia)", callback_data="total_dia_categorias")],
            [InlineKeyboardButton("🏠 MENU", callback_data="menu")],
        ])
        await query.edit_message_text("⚠️ Erro ao carregar detalhes da categoria.", parse_mode='Markdown', reply_markup=kb)

async def expandir_categorias_dia(query, context, limit: int = 6):
    hoje = _now_sp()
    dkey = _day_key_sp()
    data_str = hoje.strftime("%d/%m/%Y")
    try:
        qs = build_cliente_query_params(query)
        cid = get_cliente_id(query)
        geral_url = f"{API_URL}/saldo/atual?{qs}"
        extrato_url = f"{API_URL}/extrato/hoje?include_transacoes=true&{qs}"
        try:
            geral_api, extrato_api = await asyncio.gather(
                _req_json_cached_async(geral_url, f"geral:{cid}", ttl=20, timeout=4),
                _req_json_cached_async(extrato_url, f"extrato:{cid}:{dkey}", ttl=8, timeout=5),
            )
        except:
            geral_api = {}
            extrato_api = {}
        transacoes = extrato_api.get("transacoes", []) if extrato_api.get("sucesso") else []
        grupos = {}
        for t in transacoes or []:
            if t.get('estornado'):
                continue
            tp_raw = str(t.get('tipo', '')).strip().lower()
            if tp_raw not in ('0', 'despesa', 'saida', '1', 'receita', 'entrada'):
                continue
            cat = str(t.get('categoria', 'outros') or 'outros').strip().lower()
            grupos.setdefault(cat, []).append({
                "tipo": ('saida' if tp_raw in ('0', 'despesa', 'saida') else 'entrada'),
                "valor": float(t.get('valor', 0) or 0),
                "descricao": str(t.get('descricao', '') or '')
            })
        resposta = criar_cabecalho("CATEGORIAS (DIA) • DETALHES", 40)
        resposta += f"\n📅 {data_str}\n\n"
        for k, lst in sorted(grupos.items(), key=lambda x: x[0]):
            label = CATEGORY_NAMES.get(k, k)
            saida = sorted([x for x in lst if x['tipo'] == 'saida'], key=lambda x: -float(x['valor']))
            entrada = sorted([x for x in lst if x['tipo'] == 'entrada'], key=lambda x: -float(x['valor']))
            resposta += f"{label}\n"
            if saida:
                resposta += "  🔴 Despesas\n"
                for it in saida[:limit]:
                    desc = md_escape(it['descricao'])
                    val = formatar_moeda(it['valor'])
                    resposta += f"  • {desc} — {val}\n"
            if entrada:
                resposta += "  🟢 Receitas\n"
                for it in entrada[:limit]:
                    desc = md_escape(it['descricao'])
                    val = formatar_moeda(it['valor'])
                    resposta += f"  • {desc} — +{val}\n"
            resposta += "\n"
        try:
            tot_geral = geral_api.get("total", {}) if geral_api.get("sucesso") else {}
            saldo_geral = float(tot_geral.get("saldo_real", tot_geral.get("saldo", 0)) or 0)
        except:
            saldo_geral = obter_saldo_geral(get_cliente_id(query))
        resposta += f"💹 Saldo atual real: {formatar_moeda(saldo_geral, negrito=True)}\n"
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("⬅️ Voltar", callback_data="total_dia_categorias")],
            [InlineKeyboardButton("🏠 MENU", callback_data="menu")],
        ])
        await query.edit_message_text(resposta, parse_mode='Markdown', reply_markup=kb)
    except:
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("⬅️ Voltar", callback_data="total_dia_categorias")],
            [InlineKeyboardButton("🏠 MENU", callback_data="menu")],
        ])
        await query.edit_message_text("⚠️ Erro ao carregar detalhes.", parse_mode='Markdown', reply_markup=kb)

async def detalhar_categoria_mes(query, context, categoria_key: str, off_saida: int = 0, off_entrada: int = 0):
    hoje = _now_sp()
    mkey = _month_key_sp()
    data_str = _mes_ano_pt(hoje)
    try:
        qs = build_cliente_query_params(query)
        cid = get_cliente_id(query)
        geral_url = f"{API_URL}/saldo/atual?{qs}"
        try:
            geral_api = await _req_json_cached_async(geral_url, f"geral:{cid}", ttl=20, timeout=4)
        except:
            geral_api = {}
        cslug = str(categoria_key or "outros").strip().lower()
        extrato_mes_url = f"{API_URL}/extrato/mes?mes={mkey}&categoria={cslug}&limit=200&{qs}"
        try:
            extrato_api = await _req_json_cached_async(extrato_mes_url, f"xmes:{cid}:{mkey}:{cslug}", ttl=12, timeout=5)
        except:
            extrato_api = {}
        itens = []
        try:
            for t in (extrato_api.get("matches", []) if extrato_api.get("sucesso") else []):
                tp_raw = str(t.get("tipo", "")).strip().lower()
                if tp_raw not in ("saida","entrada"):
                    continue
                itens.append({
                    "tipo": tp_raw,
                    "valor": float(t.get("valor",0) or 0),
                    "descricao": str(t.get("descricao","") or "")
                })
        except:
            itens = []
        try:
            total_mes_url = f"{API_URL}/total/mes?mes={mkey}&{qs}"
            tm_api = await _req_json_cached_async(total_mes_url, f"tmes:{cid}:{mkey}", ttl=15, timeout=4)
            tot_all = tm_api.get("total", {}) if tm_api.get("sucesso") else {}
            tot_despesas = float(tot_all.get("despesas", 0) or 0)
            tot_receitas = float(tot_all.get("receitas", 0) or 0)
        except:
            tot_despesas = 0.0
            tot_receitas = 0.0
        saida = [x for x in itens if x['tipo'] == 'saida']
        entrada = [x for x in itens if x['tipo'] == 'entrada']
        label = CATEGORY_NAMES.get(cslug, cslug)
        desp_cat = sum(float(it.get('valor', 0) or 0) for it in saida)
        rec_cat = sum(float(it.get('valor', 0) or 0) for it in entrada)
        def _pct(v, tot):
            try:
                return f"{(float(v or 0) / float(tot or 1)) * 100:.1f}%"
            except:
                return "0.0%"
        resposta = criar_cabecalho("DETALHE DA CATEGORIA (MÊS)", 40)
        resposta += f"\n📅 {data_str}\n"
        resposta += f"📌 *{label}*\n\n"
        resposta += f"🔴 Despesas: {formatar_moeda(desp_cat, negrito=True)} — {_pct(desp_cat, tot_despesas)} do mês\n"
        PAGE = 12
        s_sorted = sorted(saida, key=lambda x: -float(x['valor']))
        e_sorted = sorted(entrada, key=lambda x: -float(x['valor']))
        for it in s_sorted[off_saida:off_saida+PAGE]:
            resposta += f"   🔴 {formatar_moeda(it['valor'])} — {md_escape(it['descricao'])}\n"
        resposta += "\n"
        resposta += f"🟢 Receitas: {formatar_moeda(rec_cat, negrito=True)} — {_pct(rec_cat, tot_receitas)} do mês\n"
        for it in e_sorted[off_entrada:off_entrada+PAGE]:
            resposta += f"   🟢 {formatar_moeda(it['valor'])} — {md_escape(it['descricao'])}\n"
        resposta += "\n"
        qtd_s = len(saida)
        qtd_e = len(entrada)
        media_s = (desp_cat / qtd_s) if qtd_s else 0.0
        media_e = (rec_cat / qtd_e) if qtd_e else 0.0
        max_s = max([float(x['valor']) for x in saida], default=0.0)
        max_e = max([float(x['valor']) for x in entrada], default=0.0)
        resposta += f"📈 Estatísticas\n"
        resposta += f"   🔴 Itens: {qtd_s} | Média: {formatar_moeda(media_s)} | Máx: {formatar_moeda(max_s)}\n"
        resposta += f"   🟢 Itens: {qtd_e} | Média: {formatar_moeda(media_e)} | Máx: {formatar_moeda(max_e)}\n\n"
        try:
            tot_geral = geral_api.get("total", {}) if geral_api.get("sucesso") else {}
            saldo_geral = float(tot_geral.get("saldo_real", tot_geral.get("saldo", 0)) or 0)
        except:
            saldo_geral = obter_saldo_geral(get_cliente_id(query))
        resposta += f"💹 Saldo atual real: {formatar_moeda(saldo_geral, negrito=True)}\n"
        btns = []
        more_s = off_saida + PAGE < len(saida)
        more_e = off_entrada + PAGE < len(entrada)
        row = []
        if more_s:
            rem_s = len(saida) - (off_saida + PAGE)
            row.append(InlineKeyboardButton(f"Ver mais despesas (+{rem_s})", callback_data=f"catmes_more_saida:{cslug}:{off_saida+PAGE}:{off_entrada}"))
        if more_e:
            rem_e = len(entrada) - (off_entrada + PAGE)
            row.append(InlineKeyboardButton(f"Ver mais receitas (+{rem_e})", callback_data=f"catmes_more_entrada:{cslug}:{off_saida}:{off_entrada+PAGE}"))
        if row:
            btns.append(row)
        btns.append([InlineKeyboardButton("⬅️ Voltar categorias (mês)", callback_data="analise_mes_categorias")])
        btns.append([InlineKeyboardButton("📅 TOTAIS DO MÊS", callback_data="analise_mes")])
        btns.append([InlineKeyboardButton("🏠 MENU", callback_data="menu")])
        kb = InlineKeyboardMarkup(btns)
        await query.edit_message_text(resposta, parse_mode='Markdown', reply_markup=kb)
    except:
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("⬅️ Voltar categorias (mês)", callback_data="analise_mes_categorias")],
            [InlineKeyboardButton("🏠 MENU", callback_data="menu")],
        ])
        await query.edit_message_text("⚠️ Erro ao carregar detalhes da categoria.", parse_mode='Markdown', reply_markup=kb)

async def expandir_categorias_mes(query, context, limit: int = 6):
    hoje = _now_sp()
    mkey = _month_key_sp()
    data_str = _mes_ano_pt(hoje)
    processing_msg = None
    try:
        if hasattr(query, 'edit_message_text'):
            await query.edit_message_text("🔄 Gerando detalhes do mês...", parse_mode='Markdown')
        else:
            processing_msg = await query.message.reply_text("🔄 Gerando detalhes do mês...", parse_mode='Markdown')
    except:
        processing_msg = None
    try:
        qs = build_cliente_query_params(query)
        cid = get_cliente_id(query)
        geral_url = f"{API_URL}/saldo/atual?{qs}"
        cat_group_url = f"{API_URL}/saldo/atual?mes={mkey}&group_by=categoria&{qs}"
        try:
            geral_api, cat_api = await asyncio.gather(
                _req_json_cached_async(geral_url, f"geral:{cid}", ttl=20, timeout=4),
                _req_json_cached_async(cat_group_url, f"catgrp:{cid}:{mkey}", ttl=10, timeout=5),
            )
        except:
            geral_api = {}
            cat_api = {}
        transacoes = []
        try:
            db = get_db()
            root = db.collection('clientes').document(str(cid))
            ano, m = mkey.split("-")
            dt_ini = f"{ano}-{m}-01"
            if m == "12":
                dt_fim = f"{int(ano)+1}-01-01"
            else:
                dt_fim = f"{ano}-{int(m)+1:02d}-01"
            q = root.collection('transacoes').where('data_referencia', '>=', dt_ini).where('data_referencia', '<', dt_fim)
            docs = []
            try:
                docs = q.stream()
            except:
                docs = []
            idx = {}
            tl = []
            for d in docs:
                o = d.to_dict() or {}
                k = str(o.get('ref_id') or '') or (str(o.get('tipo', '')) + '|' + str(float(o.get('valor', 0) or 0)) + '|' + str(o.get('categoria', '')) + '|' + str(o.get('descricao', '')) + '|' + str(o.get('timestamp_criacao', '')))
                if not idx.get(k):
                    idx[k] = 1
                    tl.append(o)
            if not tl:
                cur = datetime.strptime(dt_ini, "%Y-%m-%d")
                end = datetime.strptime(dt_fim, "%Y-%m-%d")
                while cur < end:
                    dkey = cur.strftime("%Y-%m-%d")
                    try:
                        items = root.collection('transacoes').document(dkey).collection('items').stream()
                    except:
                        items = []
                    try:
                        tops = root.collection('transacoes').where('data_referencia', '==', dkey).stream()
                    except:
                        tops = []
                    for d in items:
                        o = d.to_dict() or {}
                        k = str(o.get('ref_id') or '') or (str(o.get('tipo', '')) + '|' + str(float(o.get('valor', 0) or 0)) + '|' + str(o.get('categoria', '')) + '|' + str(o.get('descricao', '')) + '|' + str(o.get('timestamp_criacao', '')))
                        if not idx.get(k):
                            idx[k] = 1
                            tl.append(o)
                    for d in tops:
                        o = d.to_dict() or {}
                        k = str(o.get('ref_id') or '') or (str(o.get('tipo', '')) + '|' + str(float(o.get('valor', 0) or 0)) + '|' + str(o.get('categoria', '')) + '|' + str(o.get('descricao', '')) + '|' + str(o.get('timestamp_criacao', '')))
                        if not idx.get(k):
                            idx[k] = 1
                            tl.append(o)
                    cur = cur + timedelta(days=1)
            transacoes = tl
        except:
            transacoes = []
        grupos = {}
        for t in transacoes or []:
            if t.get('estornado'):
                continue
            tp_raw = str(t.get('tipo', '')).strip().lower()
            if tp_raw not in ('0', 'despesa', 'saida', '1', 'receita', 'entrada'):
                continue
            cat = str(t.get('categoria', 'outros') or 'outros').strip().lower()
            grupos.setdefault(cat, []).append({
                "tipo": ('saida' if tp_raw in ('0', 'despesa', 'saida') else 'entrada'),
                "valor": float(t.get('valor', 0) or 0),
                "descricao": str(t.get('descricao', '') or '')
            })
        resposta = criar_cabecalho("CATEGORIAS (MÊS) • DETALHES", 40)
        resposta += f"\n📅 {data_str}\n\n"
        for k, lst in sorted(grupos.items(), key=lambda x: x[0]):
            label = CATEGORY_NAMES.get(k, k)
            saida = sorted([x for x in lst if x['tipo'] == 'saida'], key=lambda x: -float(x['valor']))
            entrada = sorted([x for x in lst if x['tipo'] == 'entrada'], key=lambda x: -float(x['valor']))
            resposta += f"{label}\n"
            if saida:
                resposta += "  🔴 Despesas\n"
                for it in saida[:limit]:
                    desc = md_escape(it['descricao'])
                    val = formatar_moeda(it['valor'])
                    resposta += f"  • {desc} — {val}\n"
            if entrada:
                resposta += "  🟢 Receitas\n"
                for it in entrada[:limit]:
                    desc = md_escape(it['descricao'])
                    val = formatar_moeda(it['valor'])
                    resposta += f"  • {desc} — +{val}\n"
            resposta += "\n"
        if not grupos:
            if cat_api.get("sucesso") and isinstance(cat_api.get("categorias"), dict):
                try:
                    mapa_desp = dict((cat_api.get("categorias") or {}).get("despesas") or {})
                    mapa_rec = dict((cat_api.get("categorias") or {}).get("receitas") or {})
                except:
                    mapa_desp = {}
                    mapa_rec = {}
                labels = [CATEGORY_NAMES.get(k, k) for k in (list(mapa_desp.keys()) + list(mapa_rec.keys()))]
                max_label = max((len(x) for x in labels), default=12)
                FS = "\u2007"
                def pad(label):
                    return label + (FS * max(0, max_label - len(label)))
                resposta += "📭 Nenhum lançamento detalhado no período.\n"
                resposta += "DESPESAS\n"
                for k, v in sorted(mapa_desp.items(), key=lambda x: -float(x[1])):
                    label = CATEGORY_NAMES.get(k, k)
                    resposta += f"  {pad(label)}{formatar_moeda(float(v or 0), negrito=False)}\n"
                resposta += "\nRECEITAS\n"
                for k, v in sorted(mapa_rec.items(), key=lambda x: -float(x[1])):
                    label = CATEGORY_NAMES.get(k, k)
                    resposta += f"  {pad(label)}+{formatar_moeda(float(v or 0), negrito=False)}\n"
            else:
                resposta += "📭 Nenhum lançamento no período.\n\n"
        try:
            tot_geral = geral_api.get("total", {}) if geral_api.get("sucesso") else {}
            saldo_geral = float(tot_geral.get("saldo_real", tot_geral.get("saldo", 0)) or 0)
        except:
            saldo_geral = obter_saldo_geral(get_cliente_id(query))
        resposta += f"💹 Saldo atual real: {formatar_moeda(saldo_geral, negrito=True)}\n"
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("⬅️ Voltar", callback_data="analise_mes_categorias")],
            [InlineKeyboardButton("🏠 MENU", callback_data="menu")],
        ])
        if hasattr(query, 'edit_message_text'):
            await query.edit_message_text(resposta, parse_mode='Markdown', reply_markup=kb)
        elif processing_msg:
            await context.bot.edit_message_text(chat_id=processing_msg.chat_id, message_id=processing_msg.message_id, text=resposta, parse_mode='Markdown', reply_markup=kb)
        else:
            await query.message.reply_text(resposta, parse_mode='Markdown', reply_markup=kb)
    except:
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("⬅️ Voltar", callback_data="analise_mes_categorias")],
            [InlineKeyboardButton("🏠 MENU", callback_data="menu")],
        ])
        await query.edit_message_text("⚠️ Erro ao carregar detalhes.", parse_mode='Markdown', reply_markup=kb)
async def resumo_hoje(query, context):
    hoje = _now_sp()
    data_str = hoje.strftime("%d/%m/%Y")
    processing_msg = None
    try:
        if hasattr(query, 'edit_message_text'):
            await query.edit_message_text("🔄 Gerando resumo de hoje...", parse_mode='Markdown')
        else:
            processing_msg = await query.message.reply_text("🔄 Gerando resumo de hoje...", parse_mode='Markdown')
    except:
        processing_msg = None
    try:
        try:
            extrato = requests.get(f"{API_URL}/extrato/hoje?{build_cliente_query_params(query)}", timeout=5).json()
        except:
            extrato = {"sucesso": False}
        transacoes_dia = extrato.get("transacoes", []) if extrato.get("sucesso") else []
        resposta = criar_cabecalho("RESUMO DE HOJE", 40)
        resposta += f"\n\n📅 *Data:* {data_str}\n"
        resposta += "=" * 40 + "\n\n"
        if not transacoes_dia:
            resposta += "📭 *Nenhuma transação hoje*\n\n"
            keyboard = [[InlineKeyboardButton("💰 VER TOTAIS", callback_data="total_dia")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            if hasattr(query, 'edit_message_text'):
                await query.edit_message_text(resposta, parse_mode='Markdown', reply_markup=reply_markup)
            elif processing_msg:
                await context.bot.edit_message_text(chat_id=processing_msg.chat_id, message_id=processing_msg.message_id, text=resposta, parse_mode='Markdown', reply_markup=reply_markup)
            else:
                await query.message.reply_text(resposta, parse_mode='Markdown', reply_markup=reply_markup)
            return
        stats_dia = calcular_estatisticas(transacoes_dia, "HOJE")
        tot = extrato.get("total", {"receitas": 0, "despesas": 0, "saldo": 0})
        resposta += "💰 *TOTAIS RÁPIDOS*\n"
        caixa = ""
        caixa += "+" + ("-" * 28) + "+\n"
        caixa += f"|{criar_linha_tabela('RECEITAS:', formatar_moeda(tot.get('receitas', 0), negrito=False), True, '', largura=28)}|\n"
        caixa += f"|{criar_linha_tabela('DESPESAS:', formatar_moeda(tot.get('despesas', 0), negrito=False), True, '', largura=28)}|\n"
        caixa += f"|{criar_linha_tabela('SALDO:', formatar_moeda(tot.get('saldo', 0), negrito=False), True, '', largura=28)}|\n"
        caixa += "+" + ("-" * 28) + "+"
        resposta += wrap_code_block(caixa) + "\n\n"
        saldo_geral = obter_saldo_geral(get_cliente_id(query))
        resposta += f"💹 *SALDO ATUAL REAL:* {formatar_moeda(saldo_geral, negrito=True)}\n\n"
        if tot.get('estornos', 0) > 0:
            resposta += f"🔁 Estornos do dia: {formatar_moeda(tot.get('estornos', 0), negrito=True)}\n\n"
        resposta += f"📋 *Transações:* {stats_dia['quantidade']['total']}\n"
        if stats_dia['maiores']['despesa']:
            maior_d = stats_dia['maiores']['despesa']
            resposta += f"🔴 Maior despesa: {formatar_moeda(maior_d['valor'])} — {maior_d['descricao']}\n"
        if stats_dia['maiores']['receita']:
            maior_r = stats_dia['maiores']['receita']
            resposta += f"🟢 Maior receita: {formatar_moeda(maior_r['valor'])} — {maior_r['descricao']}\n"
        resposta += "\n"
        keyboard = [
            [InlineKeyboardButton("📊 VER DETALHADO", callback_data="relatorio_dia"),
             InlineKeyboardButton("💰 TOTAIS DO DIA", callback_data="total_dia")],
            [InlineKeyboardButton("🗓️ TOTAIS DA SEMANA", callback_data="total_semana"),
             InlineKeyboardButton("📈 ANÁLISE MENSAL", callback_data="analise_mes")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        if hasattr(query, 'edit_message_text'):
            await query.edit_message_text(resposta, parse_mode='Markdown', reply_markup=reply_markup)
        elif processing_msg:
            await context.bot.edit_message_text(chat_id=processing_msg.chat_id, message_id=processing_msg.message_id, text=resposta, parse_mode='Markdown', reply_markup=reply_markup)
        else:
            await query.message.reply_text(resposta, parse_mode='Markdown', reply_markup=reply_markup)
    except:
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 Tentar novamente", callback_data="relatorio_dia")],
            [InlineKeyboardButton("💰 Totais do dia", callback_data="total_dia")],
            [InlineKeyboardButton("🏠 MENU", callback_data="menu")]
        ])
        err_txt = "⚠️ Erro ao gerar resumo de hoje. Tente novamente."
        try:
            if hasattr(query, 'edit_message_text'):
                await query.edit_message_text(err_txt, parse_mode='Markdown', reply_markup=kb)
            elif processing_msg:
                await context.bot.edit_message_text(chat_id=processing_msg.chat_id, message_id=processing_msg.message_id, text=err_txt, parse_mode='Markdown', reply_markup=kb)
            else:
                await query.message.reply_text(err_txt, parse_mode='Markdown', reply_markup=kb)
        except:
            pass
async def total_semana(query, context):
    hoje = _now_sp()
    processing_msg = None
    try:
        if hasattr(query, 'edit_message_text'):
            await query.edit_message_text("🔄 Gerando totais da semana...", parse_mode='Markdown')
        else:
            processing_msg = await query.message.reply_text("🔄 Gerando totais da semana...", parse_mode='Markdown')
    except:
        processing_msg = None
    try:
        try:
            total_sem = requests.get(f"{API_URL}/total/semana?{build_cliente_query_params(query)}", timeout=5).json()
        except:
            total_sem = {"sucesso": False}
        resposta = criar_cabecalho("TOTAIS DA SEMANA", 40)
        resposta += f"\n\n📅 *Semana:* {hoje.strftime('%U/%Y')}\n"
        resposta += "=" * 40 + "\n\n"
        tot = total_sem.get("total", {"receitas": 0, "despesas": 0, "saldo": 0}) if total_sem.get("sucesso") else {"receitas": 0, "despesas": 0, "saldo": 0}
        resposta += "💰 *ACUMULADO*\n"
        caixa = ""
        caixa += "+" + ("-" * 28) + "+\n"
        caixa += f"|{criar_linha_tabela('RECEITAS:', formatar_moeda(tot.get('receitas', 0), negrito=False), True, '', largura=28)}|\n"
        caixa += f"|{criar_linha_tabela('DESPESAS:', formatar_moeda(tot.get('despesas', 0), negrito=False), True, '', largura=28)}|\n"
        caixa += f"|{criar_linha_tabela('SALDO:', formatar_moeda(tot.get('saldo', 0), negrito=False), True, '', largura=28)}|\n"
        caixa += "+" + ("-" * 28) + "+"
        resposta += wrap_code_block(caixa) + "\n\n"
        if tot.get('estornos', 0) > 0:
            resposta += f"🔁 Estornos na semana: {formatar_moeda(tot.get('estornos', 0), negrito=True)}\n\n"
        keyboard = [
            [InlineKeyboardButton("💰 TOTAIS DO DIA", callback_data="total_dia"),
             InlineKeyboardButton("📈 ANÁLISE MENSAL", callback_data="analise_mes")],
            [InlineKeyboardButton("🏠 MENU PRINCIPAL", callback_data="menu")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        if hasattr(query, 'edit_message_text'):
            await query.edit_message_text(resposta, parse_mode='Markdown', reply_markup=reply_markup)
        elif processing_msg:
            await context.bot.edit_message_text(chat_id=processing_msg.chat_id, message_id=processing_msg.message_id, text=resposta, parse_mode='Markdown', reply_markup=reply_markup)
        else:
            await query.message.reply_text(resposta, parse_mode='Markdown', reply_markup=reply_markup)
    except:
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 Tentar novamente", callback_data="total_semana")],
            [InlineKeyboardButton("🏠 MENU", callback_data="menu")]
        ])
        err_txt = "⚠️ Erro ao gerar totais da semana. Tente novamente."
        try:
            if hasattr(query, 'edit_message_text'):
                await query.edit_message_text(err_txt, parse_mode='Markdown', reply_markup=kb)
            elif processing_msg:
                await context.bot.edit_message_text(chat_id=processing_msg.chat_id, message_id=processing_msg.message_id, text=err_txt, parse_mode='Markdown', reply_markup=kb)
            else:
                await query.message.reply_text(err_txt, parse_mode='Markdown', reply_markup=kb)
        except:
            pass
# ===== FUNÇÕES RESTANTES (mantidas mas simplificadas) =====
async def relatorio_categorias(query, context):
    """Relatório por categoria."""
    await query.edit_message_text("🏷️ Use os botões de Totais do dia ou Totais do mês para ver categorias.", parse_mode='Markdown')

async def extrato_detalhado(query, context):
    try:
        cid = get_cliente_id(query)
        qs = build_cliente_query_params(query)
        mk = _month_key_sp()
        url_dia = f"{API_URL}/extrato/hoje?include_transacoes=true&{qs}"
        url_mes = f"{API_URL}/saldo/atual?mes={mk}&{qs}"
        url_geral = f"{API_URL}/saldo/atual?{qs}"
        try:
            day_api = requests.get(url_dia, timeout=6).json()
        except:
            day_api = {"sucesso": False}
        try:
            mes_api = requests.get(url_mes, timeout=6).json()
        except:
            mes_api = {"sucesso": False}
        try:
            geral_api = requests.get(url_geral, timeout=6).json()
        except:
            geral_api = {"sucesso": False}
        d_tot = (day_api or {}).get("total") or {}
        m_tot = (mes_api or {}).get("total") or {}
        g_tot = (geral_api or {}).get("total") or {}
        receitas_dia = float(d_tot.get("receitas", 0) or 0)
        despesas_dia = float(d_tot.get("despesas", 0) or 0)
        saldo_dia = float(d_tot.get("saldo", receitas_dia - despesas_dia) or (receitas_dia - despesas_dia))
        receitas_mes = float(m_tot.get("receitas", 0) or 0)
        despesas_mes = float(m_tot.get("despesas", 0) or 0)
        saldo_mes = float(m_tot.get("saldo", receitas_mes - despesas_mes) or (receitas_mes - despesas_mes))
        saldo_real = float(g_tot.get("saldo_real", g_tot.get("saldo", 0)) or 0)
        titulo = criar_cabecalho("EXTRATO DETALHADO", 40) + "\n\n"
        largura = 32
        caixa_dia = ""
        caixa_dia += "+" + ("-" * largura) + "+\n"
        caixa_dia += f"|{criar_linha_tabela('HOJE', '', False, '', largura=largura)}|\n"
        caixa_dia += "+" + ("-" * largura) + "+\n"
        caixa_dia += f"|{criar_linha_tabela('RECEITAS:', formatar_moeda(receitas_dia, negrito=False), True, '', largura=largura)}|\n"
        caixa_dia += f"|{criar_linha_tabela('DESPESAS:', formatar_moeda(despesas_dia, negrito=False), True, '', largura=largura)}|\n"
        caixa_dia += f"|{criar_linha_tabela('SALDO:', formatar_moeda(saldo_dia, negrito=False), True, '', largura=largura)}|\n"
        caixa_dia += "+" + ("-" * largura) + "+\n"
        caixa_mes = ""
        caixa_mes += "+" + ("-" * largura) + "+\n"
        caixa_mes += f"|{criar_linha_tabela('ESTE MÊS', '', False, '', largura=largura)}|\n"
        caixa_mes += "+" + ("-" * largura) + "+\n"
        caixa_mes += f"|{criar_linha_tabela('RECEITAS:', formatar_moeda(receitas_mes, negrito=False), True, '', largura=largura)}|\n"
        caixa_mes += f"|{criar_linha_tabela('DESPESAS:', formatar_moeda(despesas_mes, negrito=False), True, '', largura=largura)}|\n"
        caixa_mes += f"|{criar_linha_tabela('SALDO:', formatar_moeda(saldo_mes, negrito=False), True, '', largura=largura)}|\n"
        caixa_mes += "+" + ("-" * largura) + "+\n"
        bloco = wrap_code_block(caixa_dia) + "\n" + wrap_code_block(caixa_mes)
        lista_tx = []
        try:
            for t in (day_api.get("matches") or [])[:10]:
                tp = str(t.get('tipo', '')).strip().lower()
                emoji = "🔴" if tp in ('0', 'saida', 'despesa') else ("🟢" if tp in ('1', 'entrada', 'receita') else "⚙️")
                v = float(t.get('valor', 0) or 0)
                desc = str(t.get('descricao', '') or '')
                lista_tx.append(f"{emoji} {formatar_moeda(v)} — `{md_escape(desc)}`")
        except:
            lista_tx = []
        linhas = "\n".join(lista_tx) if lista_tx else "📭 Nenhuma transação hoje"
        rodape = f"\n💹 SALDO ATUAL REAL: {formatar_moeda(saldo_real, negrito=True)}\n\n"
        texto = titulo + bloco + wrap_code_block(linhas) + rodape
        await query.edit_message_text(texto, parse_mode='Markdown')
    except:
        await query.edit_message_text("⚠️ Erro ao carregar extrato.", parse_mode='Markdown')

# ===== HANDLERS DE COMANDOS =====
async def comando_total(update: Update, context: CallbackContext) -> None:
    """Handler para comando /total."""
    await relatorio_total(update, context)

async def comando_hoje(update: Update, context: CallbackContext) -> None:
    """Handler para comando /hoje."""
    await resumo_hoje(update, context)

async def comando_semana(update: Update, context: CallbackContext) -> None:
    """Handler para comando /semana."""
    await total_semana(update, context)
async def comando_mes(update: Update, context: CallbackContext) -> None:
    """Handler para comando /mes."""
    await analise_mensal(update, context)

async def comando_estornar(update: Update, context: CallbackContext) -> None:
    """Handler para comando /estornar."""
    try:
        txt = (update.message.text or "").strip()
        args = txt.split()[1:]
        if not args:
            await update.message.reply_text(
                "Uso: /estornar <id_da_transacao>\nOu: /estornar <valor> <data>\nExemplos:\n• /estornar abc123\n• /estornar 59,90 28/12/2025",
                parse_mode='Markdown'
            )
            return
        if len(args) == 1:
            ref_id = args[0]
            has_letters = any(ch.isalpha() for ch in ref_id)
            if not has_letters:
                v, dr, tipo = extrair_campos_estorno(txt)
                if not v or v <= 0:
                    await update.message.reply_text("⚠️ Informe um valor válido para estornar.", parse_mode='Markdown')
                    return
                if not dr:
                    dr = _day_key_sp()
                await iniciar_fluxo_estorno_por_valor(update, context, v, dr, tipo=tipo, processamento=None)
                return
            payload = {
                "cliente_id": str(update.effective_chat.id),
                "referencia_id": ref_id,
                "motivo": "Estorno solicitado via bot"
            }
            try:
                r = requests.post(f"{API_URL}/ajustes/estornar", json=payload, timeout=8)
                data = r.json() if r.ok else {"sucesso": False}
            except:
                data = {"sucesso": False}
            if not data.get("sucesso"):
                err = str(data.get("erro", "Falha ao estornar. Verifique o ID."))
                kb = InlineKeyboardMarkup([[InlineKeyboardButton("🏠 MENU", callback_data="menu")]])
                await update.message.reply_text(f"⚠️ {md_escape(err)}", parse_mode='Markdown', reply_markup=kb)
                return
            dd = data.get("totais_dia", {})
            mm = data.get("totais_mes", {})
            resp = criar_cabecalho("ESTORNO REGISTRADO", 40)
            caixa = ""
            caixa += "+" + ("-" * 28) + "+\n"
            caixa += f"|{criar_linha_tabela('DIA - SALDO:', formatar_moeda(dd.get('saldo', 0), negrito=False), True, '', largura=28)}|\n"
            caixa += f"|{criar_linha_tabela('DIA - DESPESAS:', formatar_moeda(dd.get('despesas', 0), negrito=False), True, '', largura=28)}|\n"
            caixa += f"|{criar_linha_tabela('DIA - RECEITAS:', formatar_moeda(dd.get('receitas', 0), negrito=False), True, '', largura=28)}|\n"
            caixa += "+" + ("-" * 28) + "+\n"
            caixa += f"|{criar_linha_tabela('MÊS - SALDO:', formatar_moeda(mm.get('saldo', 0), negrito=False), True, '', largura=28)}|\n"
            caixa += f"|{criar_linha_tabela('MÊS - DESPESAS:', formatar_moeda(mm.get('despesas', 0), negrito=False), True, '', largura=28)}|\n"
            caixa += f"|{criar_linha_tabela('MÊS - RECEITAS:', formatar_moeda(mm.get('receitas', 0), negrito=False), True, '', largura=28)}|\n"
            caixa += "+" + ("-" * 28) + "+"
            resp += wrap_code_block(caixa) + "\n\n"
            if 'estornos' in dd or 'estornos' in mm:
                resp += f"🔁 Estornos do dia: {formatar_moeda(dd.get('estornos', 0), negrito=True)}\n"
                resp += f"🔁 Estornos do mês: {formatar_moeda(mm.get('estornos', 0), negrito=True)}\n"
            resp += "📊 Use /total para ver os totais atualizados"
            await update.message.reply_text(resp, parse_mode='Markdown')
            return
        raw_val = args[0]
        raw_dt = args[1]
        desc_hint = " ".join(args[2:]).strip() if len(args) > 2 else ""
        v_parsed, dr_parsed, tipo_parsed = extrair_campos_estorno(txt)
        v = v_parsed if v_parsed else (parse_value(raw_val.replace("+", "").replace("R$", "").strip()) or 0.0)
        dr = dr_parsed if dr_parsed else normalizar_data(raw_dt)
        payload = {
            "cliente_id": str(update.effective_chat.id),
            "valor": float(v),
            "data_referencia": dr
        }
        if desc_hint:
            payload["descricao_contains"] = desc_hint.lower()
        if tipo_parsed:
            payload["tipo"] = tipo_parsed
        try:
            r = requests.post(f"{API_URL}/ajustes/buscar_por_valor", json=payload, timeout=8)
            data = r.json() if r.ok else {"sucesso": False}
        except:
            data = {"sucesso": False}
        if not data.get("sucesso"):
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("🏠 MENU", callback_data="menu")]])
            await update.message.reply_text("⚠️ Erro ao buscar transações.", parse_mode='Markdown', reply_markup=kb)
            return
        q = int(data.get("quantidade", 0) or 0)
        ms = data.get("matches", []) or []
        omit = int(data.get("omitidos_estornados", 0) or 0)
        avisos = data.get("avisos", []) or []
        if q == 0:
            msg = "Não há transações para este dia e valor."
            if omit > 0:
                msg += f"\n⚠️ {omit} transação(ões) já estão estornadas e foram omitidas."
            await update.message.reply_text(msg, parse_mode='Markdown')
            return
        if q == 1:
            t = ms[0]
            tp = str(t.get('tipo', '')).strip().lower()
            emoji = "🔴" if tp in ('0', 'saida') else "🟢"
            cat_raw = t.get('categoria', 'outros')
            cat_nome = md_escape(CATEGORY_NAMES.get(cat_raw, cat_raw))
            desc_json = str(t.get('descricao', ''))
            texto = criar_cabecalho("CONFIRMAR ESTORNO", 40)
            texto += f"\n{emoji} {formatar_moeda(float(t.get('valor', 0)))}\n"
            texto += f"`{desc_json}`\n"
            texto += f"Categoria: {cat_nome}\n"
            ts_raw = str(t.get('timestamp_criacao', '') or '')
            fmt_ts = formatar_data_hora_local(ts_raw)
            if fmt_ts:
                texto += f"Data de criação: {fmt_ts}\n"
            origem = str(t.get('origem', '') or '')
            if origem:
                texto += f"Origem: {md_escape(origem)}\n"
            if omit > 0:
                texto += f"\n⚠️ {omit} transação(ões) já estornadas foram omitidas."
            for a in avisos[:2]:
                texto += f"\n⚠️ {md_escape(a)}"
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Confirmar", callback_data=f"estornar_confirmar:{t.get('id')}")],
                [InlineKeyboardButton("❌ Cancelar", callback_data="estornar_cancelar")]
            ])
            await update.message.reply_text(texto, parse_mode='Markdown', reply_markup=kb)
            return
        texto = criar_cabecalho("SELECIONE PARA ESTORNO", 40)
        linhas = []
        for t in ms[:6]:
            tp = str(t.get('tipo', '')).strip().lower()
            emoji = "🔴" if tp in ('0', 'saida') else "🟢"
            desc_json = str(t.get('descricao', ''))
            linhas.append(f"{emoji} {formatar_moeda(float(t.get('valor', 0)))} — `{desc_json}`")
        texto += "\n" + "\n".join(linhas) + "\n"
        kb_rows = []
        kb_rows.append([
            InlineKeyboardButton("🟢 Receita", callback_data=f"estornar_filtrar:entrada:{v}:{dr}"),
            InlineKeyboardButton("🔴 Despesa", callback_data=f"estornar_filtrar:saida:{v}:{dr}")
        ])
        kb_rows.extend([[InlineKeyboardButton(f"{i+1}", callback_data=f"estornar_escolher:{t.get('id')}")] for i, t in enumerate(ms[:6])])
        kb_rows.append([InlineKeyboardButton("❌ Cancelar", callback_data="estornar_cancelar")])
        kb = InlineKeyboardMarkup(kb_rows)
        await update.message.reply_text(texto, parse_mode='Markdown', reply_markup=kb)
    except:
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🏠 MENU", callback_data="menu")]])
        try:
            await update.message.reply_text("⚠️ Erro ao estornar. Tente novamente.", parse_mode='Markdown', reply_markup=kb)
        except:
            pass
async def comando_resumo(update: Update, context: CallbackContext) -> None:
    """Handler para comando /resumo."""
    await resumo_financeiro(update, context)

async def comando_extrato(update: Update, context: CallbackContext) -> None:
    """Handler para comando /extrato."""
    await extrato_detalhado(update, context)

async def comando_analise(update: Update, context: CallbackContext) -> None:
    """Handler para comando /analise."""
    await analise_mensal(update, context)

async def comando_categorias(update: Update, context: CallbackContext) -> None:
    await relatorio_categorias(update, context)
async def comando_descricoes(update: Update, context: CallbackContext) -> None:
    txt = str(update.message.text or "").strip()
    tail = re.sub(r'^/(descricoes|descricao)\b', '', txt, flags=re.IGNORECASE).strip()
    if not tail:
        msg = "Uso: /descricoes <termo> [mês]\nExemplos:\n• /descricoes internet\n• /descricoes moto em janeiro"
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🏠 MENU", callback_data="menu")]])
        await update.message.reply_text(msg, parse_mode='Markdown', reply_markup=kb)
        return
    termo = tail
    mk = _mes_key_from_text(tail) or _month_key_sp()
    try:
        url = f"{API_URL}/compromissos/mes?mes={mk}&{build_cliente_query_params(update)}"
        data = requests.get(url, timeout=6).json()
    except:
        logging.exception("Falha ao carregar compromissos para descrições")
        data = {"sucesso": False}
    if not data.get("sucesso"):
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🏠 MENU", callback_data="menu")]])
        await update.message.reply_text("⚠️ Não foi possível carregar os compromissos.", parse_mode='Markdown', reply_markup=kb)
        return
    vencidos = data.get("vencidos", []) or []
    a_vencer = data.get("a_vencer", []) or []
    todos = (vencidos or []) + (a_vencer or [])
    termo_l = termo.lower()
    filtrados = [it for it in todos if termo_l in str(it.get('descricao','')).lower()]
    titulo = criar_cabecalho(f"DESCRIÇÕES • {_mes_label(mk)} • {md_escape(termo)}", 40)
    if not filtrados:
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🏠 MENU", callback_data="menu")]])
        await update.message.reply_text(titulo + "\n\n📭 Nenhum compromisso com essa descrição.", parse_mode='Markdown', reply_markup=kb)
        return
    cx = _render_descricoes_only(filtrados, compact=True)
    resposta = titulo + "\n\n" + wrap_code_block(cx)
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("🏠 MENU", callback_data="menu")]])
    await update.message.reply_text(resposta, parse_mode='MarkdownV2', reply_markup=kb)
def _add_months(dt: datetime, n: int) -> datetime:
    y = dt.year + ((dt.month - 1 + n) // 12)
    m = ((dt.month - 1 + n) % 12) + 1
    return datetime(y, m, 1)
def _mes_label(mkey: str) -> str:
    try:
        y, m = mkey.split("-")
        nomes = ["", "Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho", "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"]
        return f"{nomes[int(m)]} {y}"
    except:
        return mkey
async def _menu_debitos(obj, context):
    meses = []
    try:
        url = f"{API_URL}/compromissos/meses?{build_cliente_query_params(obj)}"
        data = requests.get(url, timeout=6).json()
        if data.get("sucesso"):
            meses = data.get("meses", []) or []
    except:
        meses = []
    if not meses:
        hoje = _now_sp()
        for n in (-1, 0, 1):
            mk = _add_months(hoje, n).strftime("%Y-%m")
            meses.append(mk)
    rows = []
    for mk in meses:
        rows.append([InlineKeyboardButton(_mes_label(mk), callback_data=f"debitos_mes:{mk}")])
    rows.append([InlineKeyboardButton("🏠 MENU", callback_data="menu")])
    keyboard = rows
    reply_markup = InlineKeyboardMarkup(keyboard)
    texto = criar_cabecalho("COMPROMISSOS", 40) + "\n\nSelecione o mês:"
    if hasattr(obj, 'message') and getattr(obj, 'message', None):
        await obj.message.reply_text(texto, parse_mode='Markdown', reply_markup=reply_markup)
    else:
        await obj.edit_message_text(texto, parse_mode='Markdown', reply_markup=reply_markup)
async def comando_debitos(update: Update, context: CallbackContext) -> None:
    await _menu_debitos(update, context)
async def _menu_projetados(obj, context):
    meses = []
    try:
        url = f"{API_URL}/metas/meses?{build_cliente_query_params(obj)}"
        data = requests.get(url, timeout=6).json()
        if data.get("sucesso"):
            meses = data.get("meses", []) or []
    except:
        meses = []
    if not meses:
        hoje = _now_sp()
        for n in (0, 1, 2):
            mk = _add_months(hoje, n).strftime("%Y-%m")
            meses.append(mk)
    rows = []
    for mk in meses:
        rows.append([InlineKeyboardButton(_mes_label(mk), callback_data=f"projetados_mes:{mk}")])
    rows.append([InlineKeyboardButton("🏠 MENU", callback_data="menu")])
    reply_markup = InlineKeyboardMarkup(rows)
    texto = criar_cabecalho("METAS", 40) + "\n\nSelecione o mês:"
    if hasattr(obj, 'message') and getattr(obj, 'message', None):
        await obj.message.reply_text(texto, parse_mode='Markdown', reply_markup=reply_markup)
    else:
        await obj.edit_message_text(texto, parse_mode='Markdown', reply_markup=reply_markup)
async def comando_projetados(update: Update, context: CallbackContext) -> None:
    await _menu_projetados(update, context)
async def comando_compromisso(update: Update, context: CallbackContext) -> None:
    txt = str(update.message.text or "").strip()
    tail = re.sub(r'^/(compromisso|compromiso)\b', '', txt, flags=re.IGNORECASE).strip()
    if not tail:
        msg = "Como lançar: /compromisso descrição valor dia X\nExemplo: /compromisso Internet 120,00 dia 10"
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🏠 MENU", callback_data="menu")]])
        await update.message.reply_text(msg, parse_mode='Markdown', reply_markup=kb)
        return
    val = _extract_first_value(tail)
    m_dia = re.search(r'\bdia\s+(\d{1,2})\b', tail, re.IGNORECASE)
    dia = None
    if m_dia:
        try:
            dia = int(m_dia.group(1))
        except:
            dia = None
    mk = _mes_key_from_text(tail) or _month_key_sp()
    y, m = mk.split("-")
    try:
        d = dia if dia and 1 <= dia <= 31 else 1
        vencimento = f"{y}-{int(m):02d}-{int(d):02d}"
    except:
        vencimento = _day_key_sp()
    desc = tail
    desc = re.sub(r'(?:R?\$?\s*)\d{1,3}(?:[.\s]\d{3})*(?:,\d{2}|\b)', '', desc)
    desc = re.sub(r'\bdia\s+\d{1,2}\b', '', desc, flags=re.IGNORECASE)
    desc = desc.strip()
    try:
        db = get_db()
        cid = str(update.effective_chat.id)
        ensure_cliente(cid, nome=get_cliente_nome(update), username=get_cliente_username(update))
        root = db.collection('clientes').document(cid)
        status = "v"
        try:
            hj = _day_key_sp()
            if vencimento < hj:
                status = "st"
        except:
            pass
        doc = {
            "descricao": desc,
            "valor_total": float(val or 0),
            "vencimento_iso": vencimento,
            "mes": vencimento[:7],
            "instituicao": "",
            "status": status,
            "timestamp_criacao": firestore.SERVER_TIMESTAMP,
            "origem": "bot-compromisso",
        }
        root.collection('compromissos').add(doc)
        titulo = criar_cabecalho("COMPROMISSO ADICIONADO", 40)
        caixa = ""
        caixa += "+" + ("-" * 40) + "+\n"
        caixa += f"|{criar_linha_tabela('Descrição:', md_escape(desc), True, '', largura=40)}|\n"
        caixa += f"|{criar_linha_tabela('Valor:', formatar_moeda(val, negrito=False), True, '', largura=40)}|\n"
        caixa += f"|{criar_linha_tabela('Vencimento:', md_escape(vencimento), True, '', largura=40)}|\n"
        caixa += f"|{criar_linha_tabela('Mês:', md_escape(mk), True, '', largura=40)}|\n"
        caixa += "+" + ("-" * 40) + "+\n"
        resp = titulo + "\n\n" + wrap_code_block(caixa) + "\n"
        resp += "Use /debitos para visualizar."
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("📅 COMPROMISSOS DO MÊS", callback_data="debitos_menu")]])
        await update.message.reply_text(resp, parse_mode='Markdown', reply_markup=kb)
    except:
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🏠 MENU", callback_data="menu")]])
        await update.message.reply_text("⚠️ Erro ao salvar compromisso.", parse_mode='Markdown', reply_markup=kb)
async def comando_meta(update: Update, context: CallbackContext) -> None:
    try:
        txt = str(update.message.text or "").strip()
        tail = re.sub(r'^/meta\b', '', txt, flags=re.IGNORECASE).strip()
        if not tail:
            msg = "Como lançar: /meta descrição valor\nExemplo: /meta Internet 120,00"
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("🏠 MENU", callback_data="menu")]])
            await update.message.reply_text(msg, parse_mode='Markdown', reply_markup=kb)
            return
        val = _extract_first_value(tail) or 0.0
        mk = _mes_key_from_text(tail) or _month_key_sp()
        desc = tail
        desc = re.sub(r'(?:R?\$?\s*)\d{1,3}(?:[.\s]\d{3})*(?:,\d{2}|\b)', '', desc)
        desc = re.sub(r'\bem\s+(janeiro|fevereiro|mar[cç]o|abril|maio|junho|julho|agosto|setembro|outubro|novembro|dezembro)(?:\s+de\s+\d{4})?\b', '', desc, flags=re.IGNORECASE)
        desc = re.sub(r'\b\d{4}[-/]\d{1,2}\b', '', desc)
        desc = desc.strip()
        payload = {
            "cliente_id": get_cliente_id(update),
            "cliente_nome": get_cliente_nome(update),
            "username": get_cliente_username(update),
            "mes": mk,
            "descricao": desc,
            "valor": val,
        }
        try:
            r = requests.post(f"{API_URL}/metas/adicionar", json=payload, timeout=8)
            data = r.json() if r.ok else {"sucesso": False}
        except:
            data = {"sucesso": False}
        if not data.get("sucesso"):
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("🏠 MENU", callback_data="menu")]])
            await update.message.reply_text("⚠️ Não consegui registrar a meta. Tente novamente.", parse_mode='Markdown', reply_markup=kb)
            return
        try:
            url = f"{API_URL}/metas/mes?mes={mk}&{build_cliente_query_params(update)}"
            resm = requests.get(url, timeout=6).json()
        except:
            resm = {"sucesso": False}
        titulo = criar_cabecalho(f"META REGISTRADA • {_mes_label(mk)}", 40)
        resposta = titulo + "\n\n"
        resposta += f"✅ {md_escape(desc)} — {formatar_moeda(val, negrito=True)}\n\n"
        if resm.get("sucesso"):
            metas = resm.get("metas", []) or []
            tot = float(resm.get("total", 0) or 0)
            header = f"{'DESCRIÇÃO':<24}  {'VALOR':>12}\n"
            linhas = []
            for it in sorted(metas, key=lambda x: x.get("descricao", "")):
                dsc = str(it.get('descricao', '') or '')[:24]
                vv = formatar_moeda(float(it.get('valor', 0) or 0))[:12]
                linhas.append(f"{dsc:<24}  {vv:>12}")
            cx = header + "\n".join(linhas) if linhas else "Nenhuma meta cadastrada"
            resposta += wrap_code_block(cx) + "\n"
            resposta += f"Total de metas: {formatar_moeda(tot, negrito=True)}\n"
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🏠 MENU", callback_data="menu")]
        ])
        await update.message.reply_text(resposta, parse_mode='Markdown', reply_markup=kb)
    except:
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🏠 MENU", callback_data="menu")]])
        await update.message.reply_text("⚠️ Erro ao registrar meta.", parse_mode='Markdown', reply_markup=kb)
async def mostrar_projetados_mes(obj, context, mes: str):
    try:
        url = f"{API_URL}/metas/mes?mes={mes}&{build_cliente_query_params(obj)}"
        try:
            data = requests.get(url, timeout=6).json()
        except:
            data = {"sucesso": False}
        if not data.get("sucesso"):
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Voltar", callback_data="projetados_menu")]])
            if hasattr(obj, 'edit_message_text'):
                await obj.edit_message_text("⚠️ Não foi possível carregar as metas.", parse_mode='Markdown', reply_markup=kb)
            else:
                await obj.message.reply_text("⚠️ Não foi possível carregar as metas.", parse_mode='Markdown', reply_markup=kb)
            return
        metas = data.get("metas", []) or []
        tot = float(data.get("total", 0) or 0)
        titulo = criar_cabecalho(f"METAS • {_mes_label(mes)}", 40)
        resposta = titulo + "\n\n"
        header = f"{'DESCRIÇÃO':<24}  {'VALOR':>12}\n"
        linhas = []
        for it in sorted(metas, key=lambda x: x.get("descricao", "")):
            desc = str(it.get('descricao', '') or '')[:24]
            val = formatar_moeda(float(it.get('valor', 0) or 0))[:12]
            linhas.append(f"{desc:<24}  {val:>12}")
        cx = header + "\n".join(linhas) if linhas else "Nenhuma meta cadastrada"
        resposta += wrap_code_block(cx) + "\n"
        resposta += f"Total de metas: {formatar_moeda(tot, negrito=True)}\n"
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("⬅️ Voltar", callback_data="projetados_menu")],
            [InlineKeyboardButton("🏠 MENU", callback_data="menu")],
        ])
        if hasattr(obj, 'edit_message_text'):
            await obj.edit_message_text(resposta, parse_mode='MarkdownV2', reply_markup=kb)
        else:
            await obj.message.reply_text(resposta, parse_mode='MarkdownV2', reply_markup=kb)
    except:
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Voltar", callback_data="projetados_menu")]])
        if hasattr(obj, 'edit_message_text'):
            await obj.edit_message_text("⚠️ Erro ao montar metas.", parse_mode='Markdown', reply_markup=kb)
        else:
            await obj.message.reply_text("⚠️ Erro ao montar metas.", parse_mode='Markdown', reply_markup=kb)
async def mostrar_debitos_mes(obj, context, mes: str, compact: bool = False):
    try:
        url = f"{API_URL}/compromissos/mes?mes={mes}&{build_cliente_query_params(obj)}"
        try:
            data = requests.get(url, timeout=6).json()
        except:
            logging.exception("Falha ao carregar compromissos")
            data = {"sucesso": False}
        if not data.get("sucesso"):
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Voltar", callback_data="debitos_menu")]])
            if hasattr(obj, 'edit_message_text'):
                await obj.edit_message_text("⚠️ Não foi possível carregar os compromissos.", parse_mode='Markdown', reply_markup=kb)
            else:
                await obj.message.reply_text("⚠️ Não foi possível carregar os compromissos.", parse_mode='Markdown', reply_markup=kb)
            return
        vencidos = data.get("vencidos", []) or []
        a_vencer = data.get("a_vencer", []) or []
        tot = data.get("totais", {}) or {}
        titulo = criar_cabecalho(f"COMPROMISSOS • {_mes_label(mes)}", 40)
        largura = 80
        # calcular larguras compactas uma vez usando ambos conjuntos
        all_items = list(vencidos) + list(a_vencer)
        target_len = 36
        val_w_cap = 14
        if all_items:
            vals_fmt = []
            for it in all_items:
                v = float(it.get('valor', 0) or 0)
                vv = formatar_moeda(v).replace("R$ ", "R$")
                vals_fmt.append(vv)
            try:
                val_w = max(len("VALOR"), min(max((len(x) for x in vals_fmt), default=len("VALOR")), val_w_cap))
            except:
                val_w = len("VALOR")
            desc_w = max(len("DESCRIÇÃO"), target_len - (3 + val_w))
        else:
            val_w = 12
            desc_w = 21
        # preparar blocos separados para possível envio em partes
        bloco_venc = ""
        bloco_av = ""
        if vencidos:
            bloco_venc += criar_secao("Vencidos")
            cx = _render_grouped_fixed(vencidos, compact=True, desc_w=desc_w, val_w=val_w)
            bloco_venc += wrap_code_block(cx) + "\n"
        else:
            bloco_venc += "✅ Sem vencidos no período\n\n"
        if a_vencer:
            bloco_av += criar_secao("A vencer")
            cx2 = _render_grouped_fixed(a_vencer, compact=True, desc_w=desc_w, val_w=val_w)
            bloco_av += wrap_code_block(cx2) + "\n"
        else:
            bloco_av += "📭 Nenhum compromisso a vencer\n\n"
        totais_resumo = ""
        totais_resumo += f"🔴 Vencidos: {formatar_moeda(tot.get('vencidos', 0), negrito=True)}\n"
        totais_resumo += f"🟡 A vencer: {formatar_moeda(tot.get('a_vencer', 0), negrito=True)}\n"
        totais_resumo += f"💳 Total: {formatar_moeda(tot.get('total', 0), negrito=True)}\n"
        resposta = titulo + "\n\n" + bloco_venc + bloco_av + totais_resumo
        kb = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("Vencidos", callback_data=f"debitos_tipo:{mes}:vencidos"),
                InlineKeyboardButton("A vencer", callback_data=f"debitos_tipo:{mes}:a_vencer"),
            ],
            [InlineKeyboardButton("⬅️ Voltar", callback_data="debitos_menu")],
            [InlineKeyboardButton("🏠 MENU", callback_data="menu")],
        ])
        # enviar em partes se a resposta ficar muito longa
        if len(resposta) > 3800:
            primeira = titulo + "\n\n" + bloco_venc
            segunda = bloco_av + totais_resumo
            if hasattr(obj, 'edit_message_text'):
                await obj.edit_message_text(primeira, parse_mode='Markdown', reply_markup=kb)
                await obj.message.reply_text(segunda, parse_mode='Markdown')
            else:
                await obj.message.reply_text(primeira, parse_mode='Markdown', reply_markup=kb)
                await obj.message.reply_text(segunda, parse_mode='Markdown')
        else:
            if hasattr(obj, 'edit_message_text'):
                await obj.edit_message_text(resposta, parse_mode='Markdown', reply_markup=kb)
            else:
                await obj.message.reply_text(resposta, parse_mode='Markdown', reply_markup=kb)
    except:
        logging.exception("Erro ao montar compromissos")
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Voltar", callback_data="debitos_menu")]])
        if hasattr(obj, 'edit_message_text'):
            await obj.edit_message_text("⚠️ Erro ao montar compromissos.", parse_mode='Markdown', reply_markup=kb)
        else:
            await obj.message.reply_text("⚠️ Erro ao montar compromissos.", parse_mode='Markdown', reply_markup=kb)
async def comando_ajuste(update: Update, context: CallbackContext) -> None:
    try:
        txt = (update.message.text or "").strip()
        args = txt.split()[1:]
        if not args:
            await update.message.reply_text(
                "Uso: /ajuste <valor> [alvo]\nExemplos:\n• /ajuste 100 saldo\n• /ajuste -50 saida\n• /ajuste +75 entrada",
                parse_mode='Markdown'
            )
            return
        raw_val = args[0]
        op = "somar"
        if raw_val.startswith("-"):
            op = "subtrair"
        v = parse_value(raw_val.replace("+", "").replace("R$", "").strip()) or 0.0
        alvo = "saldo"
        if len(args) > 1:
            a1 = args[1].strip().lower()
            if a1 in ("entrada", "receita"):
                alvo = "entrada"
            elif a1 in ("saida", "despesa"):
                alvo = "saida"
            elif a1 in ("saldo",):
                alvo = "saldo"
        payload = {
            "cliente_id": str(update.effective_chat.id),
            "valor": float(abs(v)),
            "operacao": op,
            "alvo": alvo,
            "descricao": "Ajuste manual do usuário"
        }
        try:
            r = requests.post(f"{API_URL}/ajustes/adicionar", json=payload, timeout=8)
            data = r.json() if r.ok else {"sucesso": False}
        except:
            data = {"sucesso": False}
        if not data.get("sucesso"):
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("🏠 MENU", callback_data="menu")]])
            await update.message.reply_text("⚠️ Falha ao aplicar ajuste.", parse_mode='Markdown', reply_markup=kb)
            return
        dd = data.get("totais_dia", {})
        mm = data.get("totais_mes", {})
        resp = criar_cabecalho("AJUSTE APLICADO", 40)
        resp += f"\n💰 Valor: {formatar_moeda(float(abs(v)))}\n"
        resp += f"⚙️ Operação: {'somar' if op=='somar' else 'subtrair'}\n"
        resp += f"🎯 Alvo: {md_escape(alvo)}\n\n"
        caixa = ""
        caixa += "+" + ("-" * 28) + "+\n"
        caixa += f"|{criar_linha_tabela('DIA - SALDO:', formatar_moeda(dd.get('saldo', 0), negrito=False), True, '', largura=28)}|\n"
        caixa += f"|{criar_linha_tabela('DIA - DESPESAS:', formatar_moeda(dd.get('despesas', 0), negrito=False), True, '', largura=28)}|\n"
        caixa += f"|{criar_linha_tabela('DIA - RECEITAS:', formatar_moeda(dd.get('receitas', 0), negrito=False), True, '', largura=28)}|\n"
        caixa += "+" + ("-" * 28) + "+\n"
        caixa += f"|{criar_linha_tabela('MÊS - SALDO:', formatar_moeda(mm.get('saldo', 0), negrito=False), True, '', largura=28)}|\n"
        caixa += f"|{criar_linha_tabela('MÊS - DESPESAS:', formatar_moeda(mm.get('despesas', 0), negrito=False), True, '', largura=28)}|\n"
        caixa += f"|{criar_linha_tabela('MÊS - RECEITAS:', formatar_moeda(mm.get('receitas', 0), negrito=False), True, '', largura=28)}|\n"
        caixa += "+" + ("-" * 28) + "+"
        resp += wrap_code_block(caixa) + "\n\n"
        resp += "📊 Use /total para ver os totais atualizados"
        await update.message.reply_text(resp, parse_mode='Markdown')
    except:
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🏠 MENU", callback_data="menu")]])
        try:
            await update.message.reply_text("⚠️ Erro ao aplicar ajuste. Tente novamente.", parse_mode='Markdown', reply_markup=kb)
        except:
            pass

# ===== PROCESSAMENTO DE TRANSAÇÕES =====
async def processar_mensagem_texto(update: Update, context: CallbackContext):
    """Processa mensagens de texto normais."""
    texto = update.message.text
    if texto.startswith('/'):
        return
    try:
        cat_ctx = context.user_data.get("cat_input")
    except:
        cat_ctx = None
    if cat_ctx:
        ref_id = str(cat_ctx.get("ref_id") or "")
        tipo = str(cat_ctx.get("tipo") or "")
        cat_txt = str(texto or "").strip()
        cat_norm = _normalize_ascii(cat_txt)
        use_cat = cat_norm if len(cat_norm) >= 3 else "outros"
        payload = {
            "cliente_id": get_cliente_id(update),
            "referencia_id": ref_id,
            "nova_categoria": use_cat,
        }
        try:
            r = requests.post(f"{API_URL}/transacoes/atualizar_categoria", json=payload, timeout=8)
            data = r.json() if r.ok else {"sucesso": False}
        except:
            data = {"sucesso": False}
        try:
            context.user_data.pop("cat_input", None)
        except:
            pass
        if not data.get("sucesso"):
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("🏠 MENU", callback_data="menu")]])
            await update.message.reply_text("⚠️ Não foi possível atualizar a categoria.", parse_mode='Markdown', reply_markup=kb)
            return
        up = data.get("atualizacao", {}) or {}
        rid = str(up.get("ref_id") or ref_id or "")
        nova = str(up.get("categoria_nova") or use_cat)
        try:
            pend = context.user_data.get("pending_confirmations") or {}
            if rid:
                pend.pop(rid, None)
            context.user_data["pending_confirmations"] = pend
        except:
            pass
        ltb = context.user_data.get("last_tx_block") or {}
        items = list(ltb.get("items", []) or [])
        for it in items:
            if str(it.get("ref_id") or "") == rid:
                it["categoria"] = nova
                try:
                    key = _normalize_ascii(re.sub(r'\s+', ' ', str(it.get("descricao", ""))).strip())
                    mem = context.user_data.get("cat_memory", {}) or {}
                    mem[key] = nova
                    context.user_data["cat_memory"] = mem
                    try:
                        await asyncio.to_thread(atualizar_memoria_categoria, get_cliente_id(update), key, nova)
                    except:
                        pass
                except:
                    pass
        resposta = criar_cabecalho("TRANSAÇÃO REGISTRADA", 40)
        resposta += f"\n✅ *{len(items)} transação(ões) registrada(s)*\n\n"
        for it in items:
            tp = str(it.get('tipo', '')).strip().lower()
            emoji = "🔴" if tp in ('saida', '0') else "🟢"
            tipo = "DESPESA" if tp in ('saida', '0') else "RECEITA"
            cat_nome = md_escape(CATEGORY_NAMES.get(it.get('categoria', 'outros'), it.get('categoria', 'outros')))
            desc_json = str(it.get('descricao', ''))
            resposta += f"{emoji} *{tipo}:* {formatar_moeda(float(it.get('valor', 0)))}\n"
            resposta += f"   `{desc_json}`\n"
            resposta += f"   Categoria: {cat_nome}\n\n"
        chat_id = ltb.get("chat_id")
        message_id = ltb.get("message_id")
        if chat_id and message_id:
            await context.bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=resposta, parse_mode='Markdown')
        try:
            pmid = context.user_data.get("last_cat_prompt_message_id")
            pchat = context.user_data.get("last_cat_prompt_chat_id")
            if pmid and pchat:
                await context.bot.delete_message(chat_id=pchat, message_id=pmid)
        except:
            pass
        try:
            context.user_data.pop("last_cat_prompt_message_id", None)
            context.user_data.pop("last_cat_prompt_chat_id", None)
        except:
            pass
        return
    
    intent = _detectar_intencao(texto)
    if intent:
        processing_msg = await update.message.reply_text("✅ Recebi, estou processando...")
        try:
            if intent.get("tipo") == "debitos_mes":
                mes = intent.get("mes")
                await mostrar_debitos_mes(update, context, mes)
                return
            if intent.get("tipo") == "compra_viabilidade":
                mes = intent.get("mes")
                valor = float(intent.get("valor", 0) or 0)
                try:
                    url_s = f"{API_URL}/saldo/atual?mes={mes}&{build_cliente_query_params(update)}"
                    data_s = await _req_json_async(url_s, timeout=6)
                except:
                    data_s = {"sucesso": False}
                try:
                    url_c = f"{API_URL}/compromissos/mes?mes={mes}&{build_cliente_query_params(update)}"
                    data_c = await _req_json_async(url_c, timeout=6)
                except:
                    data_c = {"sucesso": False}
                saldo = 0.0
                if data_s.get("sucesso"):
                    tot = data_s.get("total", {})
                    saldo = float(tot.get("saldo", 0) or 0)
                a_vencer = 0.0
                if data_c.get("sucesso"):
                    tv = data_c.get("totais", {})
                    a_vencer = float(tv.get("a_vencer", 0) or 0)
                disponivel = saldo - a_vencer
                pode = disponivel >= valor and valor > 0
                titulo = criar_cabecalho(f"VIABILIDADE DE COMPRA • {_mes_label(mes)}", 40)
                caixa = ""
                caixa += "+" + ("-" * 40) + "+\n"
                caixa += f"|{criar_linha_tabela('Preço:', formatar_moeda(valor, negrito=False), True, '', largura=40)}|\n"
                caixa += f"|{criar_linha_tabela('Saldo previsto:', formatar_moeda(saldo, negrito=False), True, '', largura=40)}|\n"
                caixa += f"|{criar_linha_tabela('Compromissos a vencer:', formatar_moeda(a_vencer, negrito=False), True, '', largura=40)}|\n"
                caixa += f"|{criar_linha_tabela('Disponível:', formatar_moeda(disponivel, negrito=False), True, '', largura=40)}|\n"
                caixa += "+" + ("-" * 40) + "+\n"
                resposta = titulo + "\n\n" + wrap_code_block(caixa) + "\n"
                resposta += ("✅ Pode comprar." if pode else "⚠️ Não recomendado agora.") + "\n"
                kb = InlineKeyboardMarkup([[InlineKeyboardButton("🏠 MENU", callback_data="menu")]])
                await context.bot.edit_message_text(
                    chat_id=processing_msg.chat_id,
                    message_id=processing_msg.message_id,
                    text=resposta,
                    parse_mode='Markdown',
                    reply_markup=kb
                )
                return
            if intent.get("tipo") == "buscar_descricao":
                termo = intent.get("termo", "").strip()
                mes = intent.get("mes")
                try:
                    url_c = f"{API_URL}/compromissos/mes?mes={mes}&{build_cliente_query_params(update)}"
                    data_c = requests.get(url_c, timeout=6).json()
                except:
                    data_c = {"sucesso": False}
                if not data_c.get("sucesso"):
                    kb = InlineKeyboardMarkup([[InlineKeyboardButton("🏠 MENU", callback_data="menu")]])
                    await context.bot.edit_message_text(
                        chat_id=processing_msg.chat_id,
                        message_id=processing_msg.message_id,
                        text="⚠️ Não foi possível buscar compromissos.",
                        parse_mode='Markdown',
                        reply_markup=kb
                    )
                    return
                vencidos = data_c.get("vencidos", []) or []
                a_vencer = data_c.get("a_vencer", []) or []
                todos = (vencidos or []) + (a_vencer or [])
                termo_l = termo.lower()
                filtrados = [it for it in todos if termo_l in str(it.get('descricao','')).lower()]
                if not filtrados:
                    kb = InlineKeyboardMarkup([[InlineKeyboardButton("🏠 MENU", callback_data="menu")]])
                    await context.bot.edit_message_text(
                        chat_id=processing_msg.chat_id,
                        message_id=processing_msg.message_id,
                        text="📭 Nenhum compromisso com essa descrição.",
                        parse_mode='Markdown',
                        reply_markup=kb
                    )
                    return
                titulo = criar_cabecalho(f"DESCRIÇÕES • {_mes_label(mes)} • {md_escape(termo)}", 40)
                cx = _render_descricoes_only(filtrados, compact=True)
                resposta = titulo + "\n\n" + wrap_code_block(cx)
                kb = InlineKeyboardMarkup([[InlineKeyboardButton("🏠 MENU", callback_data="menu")]])
                await context.bot.edit_message_text(
                    chat_id=processing_msg.chat_id,
                    message_id=processing_msg.message_id,
                    text=resposta,
                    parse_mode='MarkdownV2',
                    reply_markup=kb
                )
                return
        except:
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("🏠 MENU", callback_data="menu")]])
            try:
                await context.bot.edit_message_text(
                    chat_id=processing_msg.chat_id,
                    message_id=processing_msg.message_id,
                    text="⚠️ Erro ao analisar solicitação.",
                    parse_mode='Markdown',
                    reply_markup=kb
                )
            except:
                pass
            return
    processing_msg = await update.message.reply_text("✅ Recebi, estou processando...")
    
    try:
        await _bg_semaphore.acquire()
        salvas_coletadas = []
        transacoes = []
        arq = None
        try:
            from app.services.extractor import extrair_informacoes_financeiras
            transacoes = (await asyncio.to_thread(extrair_informacoes_financeiras, texto)) if _gemini_ok() else []
        except:
            transacoes = []
        try:
            cid = str(update.effective_chat.id)
            await asyncio.to_thread(ensure_cliente, cid, nome=get_cliente_nome(update), username=get_cliente_username(update))
            salvas_coletadas = await asyncio.to_thread(salvar_transacao_cliente, transacoes, cliente_id=cid, origem="bot")
        except:
            arq = None
        try:
            baixa_conf = any(
                (str(it.get('categoria', 'outros')).strip().lower() in ('outros', 'duvida')) or
                (float(it.get('confidence_score', 0.0) or 0.0) < 0.95)
                for it in transacoes
            )
        except:
            baixa_conf = True
        if baixa_conf and _gemini_ok():
            try:
                from app.services.extractor import extrair_informacoes_financeiras
                ai_trans2 = await asyncio.to_thread(extrair_informacoes_financeiras, texto) or []
                ai_idx2 = {}
                for ai in ai_trans2 or []:
                    k = (str(ai.get('tipo', '')).strip(), float(ai.get('valor', 0)))
                    ai_idx2[k] = ai
                for it in transacoes or []:
                    try:
                        k = (str(it.get('tipo', '')).strip(), float(it.get('valor', 0)))
                        cand = ai_idx2.get(k)
                        if cand and str(cand.get('categoria', 'outros')).strip().lower() not in ('outros', 'duvida'):
                            it['categoria'] = str(cand.get('categoria')).strip().lower()
                            try:
                                conf = float(cand.get('confidence_score', 0.95) or 0.95)
                                it['confidence_score'] = conf
                                it['pendente_confirmacao'] = False if conf >= 0.95 else True
                            except:
                                pass
                            try:
                                raw_d = str(it.get('descricao', ''))
                                cat_d = str(it.get('categoria', ''))
                                ai_d = sintetizar_descricao_curta(raw_d, categoria=cat_d)
                                if ai_d:
                                    it['descricao'] = ai_d
                            except:
                                pass
                    except:
                        pass
            except:
                pass
        dedup = {}
        for item in transacoes:
            tipo_n = str(item.get('tipo')).strip()
            valor_n = float(item.get('valor', 0))
            desc_raw = str(item.get('descricao', ''))
            cat_n = str(item.get('categoria', '')).strip().lower()
            desc_final = re.sub(r'\s+', ' ', desc_raw or '').strip()
            toks = desc_final.split()
            if len(toks) > 8:
                desc_final = ' '.join(toks[:8])
            k = (tipo_n, valor_n, cat_n)
            cur = dedup.get(k)
            if cur is None or len(desc_final) < len(str(cur.get('descricao', ''))):
                novo = dict(item)
                novo['descricao'] = desc_final
                novo['categoria'] = cat_n
                dedup[k] = novo
        transacoes = list(dedup.values())
        
        # Se ficou vazio após dedup/reconciliação, tentar fallback via API
        if not transacoes:
            try:
                from app.services.finance_api import processar_mensagem
                data2 = await asyncio.to_thread(processar_mensagem, texto, timeout=5, cliente_id=str(update.effective_chat.id))
            except:
                data2 = {"sucesso": False}
            if data2 and data2.get("sucesso"):
                transacoes = data2.get("transacoes", [])
                arq = data2.get("arquivo")
        
        # Se ainda vazio, responder comando inválido
        if not transacoes:
            try:
                kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton("🏠 MENU", callback_data="menu")],
                    [InlineKeyboardButton("💰 TOTAIS DO DIA", callback_data="total_dia")]
                ])
                await context.bot.edit_message_text(
                    chat_id=processing_msg.chat_id,
                    message_id=processing_msg.message_id,
                    text="⚠️ Comando inválido. Não identifiquei transações.\n\nDica: informe algo como:\n• gastei 50 no mercado\n• recebi 1000 de salário\n• transferi 300 para a mãe",
                    parse_mode='Markdown',
                    reply_markup=kb
                )
                _bg_semaphore.release()
            except:
                pass
            return
        
        # Reconciliar descrições com o JSON salvo/retornado pela API
        if arq:
            try:
                with open(arq, 'r', encoding='utf-8') as f:
                    dados_json = json.load(f)
                idx = {}
                for it in dados_json.get('transacoes', []):
                    k = (str(it.get('tipo')).strip(), float(it.get('valor', 0)), str(it.get('categoria', '')).strip().lower())
                    idx[k] = it
                transacoes_exib = []
                for it in transacoes:
                    k = (str(it.get('tipo')).strip(), float(it.get('valor', 0)), str(it.get('categoria', '')).strip().lower())
                    transacoes_exib.append(idx.get(k, it))
                transacoes = transacoes_exib
            except:
                pass
        
        resposta = criar_cabecalho("TRANSAÇÃO REGISTRADA", 40)
        resposta += f"\n✅ *{len(transacoes)} transação(ões) registrada(s)*\n\n"
        
        for transacao in transacoes:
            tp = str(transacao.get('tipo', '')).strip().lower()
            emoji = "🔴" if tp in ('0', 'saida') else "🟢"
            tipo = "DESPESA" if tp in ('0', 'saida') else "RECEITA"
            cat_raw = transacao.get('categoria', 'outros')
            cat_nome = md_escape(CATEGORY_NAMES.get(cat_raw, cat_raw))
            desc_json = transacao['descricao']
            resposta += f"{emoji} *{tipo}:* {formatar_moeda(transacao['valor'])}\n"
            resposta += f"   `{desc_json}`\n"
            resposta += f"   Categoria: {cat_nome}\n\n"
        if arq:
            safe_arq = md_escape(arq)
            resposta += f"💾 Salvo em: `{safe_arq}`\n\n"
        await context.bot.edit_message_text(
            chat_id=processing_msg.chat_id,
            message_id=processing_msg.message_id,
            text=resposta,
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("💰 TOTAIS DO DIA", callback_data="total_dia"), InlineKeyboardButton("📅 TOTAIS DO MÊS", callback_data="analise_mes")]])
        )
        try:
            idx = {}
            for s in salvas_coletadas or []:
                k = (("saida" if str(s.get("tipo")).strip() in ("0", "despesa", "saida") else "entrada"), float(s.get("valor", 0)), str(s.get("categoria", "")).strip().lower())
                idx[k] = s
            items = []
            for it in transacoes:
                tp_txt = "saida" if str(it.get("tipo")).strip() in ("0", "despesa", "saida") else "entrada"
                k = (tp_txt, float(it.get("valor", 0)), str(it.get("categoria", "")).strip().lower())
                rid = (idx.get(k) or {}).get("ref_id")
                items.append({
                    "ref_id": rid,
                    "tipo": tp_txt,
                    "valor": float(it.get("valor", 0)),
                    "categoria": str(it.get("categoria", "")).strip().lower(),
                    "descricao": str(it.get("descricao", ""))
                })
            context.user_data["last_tx_block"] = {
                "chat_id": processing_msg.chat_id,
                "message_id": processing_msg.message_id,
                "items": items
            }
        except:
            pass
        try:
            asyncio.create_task(_disparar_confirmacoes(update, context, transacoes, salvas_coletadas))
        except:
            pass
        _bg_semaphore.release()
    except:
        try:
            _bg_semaphore.release()
        except:
            pass
        await context.bot.edit_message_text(
            chat_id=processing_msg.chat_id,
            message_id=processing_msg.message_id,
            text="⚠️ *Erro ao processar. Tente novamente.*",
            parse_mode='Markdown'
        )

# ===== MAIN =====
def main() -> None:
    """Inicia o bot formatado."""
    if not _acquire_bot_lock():
        print("⚠️ Outra instância do bot está em execução. Encerrando.")
        return
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).concurrent_updates(32).build()
    
    # Handlers de comandos
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("total", comando_total))
    application.add_handler(CommandHandler("hoje", comando_hoje))
    application.add_handler(CommandHandler("semana", comando_semana))
    application.add_handler(CommandHandler("mes", comando_mes))
    application.add_handler(CommandHandler("resumo", comando_resumo))
    application.add_handler(CommandHandler("extrato", comando_extrato))
    application.add_handler(CommandHandler("analise", comando_analise))
    application.add_handler(CommandHandler("categorias", comando_categorias))
    application.add_handler(CommandHandler("projetados", comando_projetados))
    application.add_handler(CommandHandler("debitos", comando_debitos))
    application.add_handler(CommandHandler("compromissos", comando_debitos))
    application.add_handler(CommandHandler("compromisso", comando_compromisso))
    application.add_handler(CommandHandler("compromiso", comando_compromisso))
    application.add_handler(CommandHandler("descricoes", comando_descricoes))
    application.add_handler(CommandHandler("descricao", comando_descricoes))
    application.add_handler(CommandHandler("meta", comando_meta))
    application.add_handler(CommandHandler("ajuste", comando_ajuste))
    application.add_handler(CommandHandler("estornar", comando_estornar))
    application.add_handler(CommandHandler("estorno", comando_estornar))
    
    # Handler para botões
    application.add_handler(CallbackQueryHandler(button_handler))
    
    # Handler para mensagens de texto
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, processar_mensagem_texto))
    application.add_handler(MessageHandler(filters.PHOTO, processar_mensagem_imagem))
    application.add_handler(MessageHandler(filters.Document.ALL, processar_mensagem_documento))
    application.add_handler(MessageHandler(filters.VOICE, processar_mensagem_voz))
    application.add_handler(MessageHandler(filters.AUDIO, processar_mensagem_audio))
    
    print("=" * 60)
    print("🚀 BOT FINANCEIRO PROFISSIONAL INICIADO")
    print("=" * 60)
    print("✅ Comandos disponíveis:")
    print("   /start     - Menu principal")
    print("   /total     - Totais do DIA (diferenciação clara)")
    print("   /hoje      - Resumo detalhado do dia")
    print("   /semana    - Totais da semana atual")
    print("   /mes       - Totais do mês atual")
    print("   /resumo    - Resumo DIA vs MÊS")
    print("   /analise   - Análise mensal detalhada")
    print("   /extrato   - Extrato (em desenvolvimento)")
    print("   /categorias- Categorias (em desenvolvimento)")
    print("   /debitos   - Compromissos do mês (faturas/boletos)")
    print("   /compromissos - Compromissos do mês (atalho)")
    print("=" * 60)
    print("📊 Formatação profissional ativada")
    print("💹 Distinção clara: DIA vs PERÍODO")
    print("📡 Aguardando transações...")
    
    try:
        async def _err(update, context):
            try:
                logging.error(f"Erro: {getattr(context, 'error', None)}")
            except Exception:
                pass
        application.add_error_handler(_err)
    except Exception:
        pass
    try:
        import threading as _th
        t_sched = _th.Thread(target=_scheduler_thread, kwargs={"interval_seconds": 3600}, daemon=True)
        t_sched.start()
    except Exception:
        pass
    application.run_polling(drop_pending_updates=True)

async def processar_mensagem_imagem(update: Update, context: CallbackContext):
    msg = update.message
    processing_msg = await msg.reply_text("✅ Recebi, estou processando imagem...")
    try:
        await _bg_semaphore.acquire()
        if not msg.photo:
            await context.bot.edit_message_text(
                chat_id=processing_msg.chat_id,
                message_id=processing_msg.message_id,
                text="⚠️ Nenhuma foto recebida.",
                parse_mode='Markdown'
            )
            _bg_semaphore.release()
            return
        foto = msg.photo[-1]
        arquivo = await context.bot.get_file(foto.file_id)
        image_bytes = b""
        try:
            image_bytearray = await arquivo.download_as_bytearray()
            image_bytes = bytes(image_bytearray)
        except:
            try:
                url = f"https://api.telegram.org/file/bot{TELEGRAM_BOT_TOKEN}/{arquivo.file_path}"
                r = requests.get(url, timeout=15)
                if r.status_code == 200:
                    image_bytes = bytes(r.content)
            except:
                image_bytes = b""
        if not image_bytes:
            await context.bot.edit_message_text(
                chat_id=processing_msg.chat_id,
                message_id=processing_msg.message_id,
                text="⚠️ Erro ao baixar a imagem.",
                parse_mode='Markdown'
            )
            _bg_semaphore.release()
            return
        transacoes = await asyncio.to_thread(extrair_informacoes_da_imagem, image_bytes) or []
        if not transacoes:
            await context.bot.edit_message_text(
                chat_id=processing_msg.chat_id,
                message_id=processing_msg.message_id,
                text="Não consegui identificar os dados financeiros nesta imagem. Verifique se o valor está visível",
                parse_mode='Markdown'
            )
            _bg_semaphore.release()
            return
        try:
            data = await _post_json_async(
                f"{API_URL}/processar",
                {
                    "transacoes": transacoes,
                    "texto_original": "imagem:telegram",
                    "cliente_id": str(update.effective_chat.id),
                    "cliente_nome": get_cliente_nome(update),
                    "username": get_cliente_username(update),
                },
                timeout=10
            )
        except:
            data = {"sucesso": False}
        if data.get("sucesso"):
            transacoes = data.get("transacoes", transacoes)
            arq = data.get("arquivo")
        else:
            arq = None
        try:
            dedup = {}
            for item in transacoes:
                tipo_n = str(item.get('tipo')).strip()
                valor_n = float(item.get('valor', 0))
                desc_raw = str(item.get('descricao', ''))
                cat_n = str(item.get('categoria', '')).strip().lower()
                desc_final = re.sub(r'\s+', ' ', desc_raw or '').strip()
                try:
                    ai_d = sintetizar_descricao_curta(desc_final, categoria=cat_n)
                    if ai_d:
                        desc_final = ai_d
                except:
                    pass
                k = (tipo_n, valor_n, cat_n)
                cur = dedup.get(k)
                if cur is None or len(desc_final) <= len(str(cur.get('descricao', ''))):
                    novo = dict(item)
                    novo['descricao'] = desc_final
                    novo['categoria'] = cat_n
                    dedup[k] = novo
            transacoes = list(dedup.values())
        except:
            pass
        try:
            mem = context.user_data.get("cat_memory", {})
        except:
            mem = {}
        try:
            cid = str(update.effective_chat.id)
            mem_db = await asyncio.to_thread(get_categoria_memoria, cid)
            if isinstance(mem_db, dict) and mem_db:
                for k, v in mem_db.items():
                    if k and v and v not in ("outros", "duvida"):
                        mem[k] = v
            context.user_data["cat_memory"] = mem
        except:
            pass
        try:
            idx_u = {}
            for s in (data.get("salvas", []) if isinstance(data, dict) else []):
                k = (("saida" if str(s.get("tipo")).strip() in ("0", "despesa", "saida") else "entrada"), float(s.get("valor", 0)), str(s.get("categoria", "")).strip().lower())
                idx_u[k] = s
            for t in transacoes:
                try:
                    key = _normalize_ascii(re.sub(r'\s+', ' ', str(t.get("descricao", ""))).strip())
                    mc = mem.get(key)
                    if mc and mc not in ("outros", "duvida"):
                        tp_txt = "saida" if str(t.get("tipo")).strip() in ("0", "despesa", "saida") else "entrada"
                        k = (tp_txt, float(t.get("valor", 0)), str(t.get("categoria", "")).strip().lower())
                        sref = idx_u.get(k)
                        rid = sref.get("ref_id") if sref else None
                        if rid:
                            payload = {"cliente_id": get_cliente_id(update), "referencia_id": rid, "nova_categoria": mc}
                            try:
                                requests.post(f"{API_URL}/transacoes/atualizar_categoria", json=payload, timeout=6)
                            except:
                                pass
                        t["categoria"] = mc
                except:
                    pass
        except:
            pass
        resposta = criar_cabecalho("TRANSAÇÃO REGISTRADA (IMAGEM)", 40)
        resposta += f"\n✅ *{len(transacoes)} transação(ões) registrada(s)*\n\n"
        for t in transacoes:
            emoji = "🔴" if str(t.get('tipo')) == '0' else "🟢"
            tipo = "DESPESA" if str(t.get('tipo')) == '0' else "RECEITA"
            cat_raw = t.get('categoria', 'outros')
            cat_nome = md_escape(CATEGORY_NAMES.get(cat_raw, cat_raw))
            desc_json = str(t.get('descricao', ''))
            resposta += f"{emoji} *{tipo}:* {formatar_moeda(float(t.get('valor', 0)))}\n"
            resposta += f"   `{desc_json}`\n"
            resposta += f"   Categoria: {cat_nome}\n\n"
        if arq:
            safe_arq = md_escape(arq)
            resposta += f"💾 Salvo em: `{safe_arq}`\n\n"
        await context.bot.edit_message_text(
            chat_id=processing_msg.chat_id,
            message_id=processing_msg.message_id,
            text=resposta,
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("💰 TOTAIS DO DIA", callback_data="total_dia"), InlineKeyboardButton("📅 TOTAIS DO MÊS", callback_data="analise_mes")]])
        )
        try:
            salvas_img = (data.get("salvas", []) if isinstance(data, dict) else []) or []
            asyncio.create_task(_disparar_confirmacoes(update, context, transacoes, salvas_img))
        except:
            pass
        try:
            idx = {}
            for s in salvas_img or []:
                k = (("saida" if str(s.get("tipo")).strip() in ("0", "despesa", "saida") else "entrada"), float(s.get("valor", 0)), str(s.get("categoria", "")).strip().lower())
                idx[k] = s
            items = []
            for it in transacoes:
                tp_txt = "saida" if str(it.get("tipo")).strip() in ("0", "despesa", "saida") else "entrada"
                k = (tp_txt, float(it.get("valor", 0)), str(it.get("categoria", "")).strip().lower())
                rid = (idx.get(k) or {}).get("ref_id")
                items.append({
                    "ref_id": rid,
                    "tipo": tp_txt,
                    "valor": float(it.get("valor", 0)),
                    "categoria": str(it.get("categoria", "")).strip().lower(),
                    "descricao": str(it.get("descricao", ""))
                })
            context.user_data["last_tx_block"] = {
                "chat_id": processing_msg.chat_id,
                "message_id": processing_msg.message_id,
                "items": items
            }
        except:
            pass
        _bg_semaphore.release()
    except:
        try:
            _bg_semaphore.release()
        except:
            pass
        await context.bot.edit_message_text(
            chat_id=processing_msg.chat_id,
            message_id=processing_msg.message_id,
            text="⚠️ *Erro ao processar imagem. Tente novamente.*",
            parse_mode='Markdown'
        )
async def processar_mensagem_documento(update: Update, context: CallbackContext):
    msg = update.message
    processing_msg = await msg.reply_text("🔄 Processando documento de imagem...")
    try:
        await _bg_semaphore.acquire()
        doc = msg.document
        if not doc:
            await context.bot.edit_message_text(
                chat_id=processing_msg.chat_id,
                message_id=processing_msg.message_id,
                text="⚠️ Documento ausente.",
                parse_mode='Markdown'
            )
            _bg_semaphore.release()
            return
        if str(doc.mime_type or "").lower().startswith("application/pdf"):
            await context.bot.edit_message_text(
                chat_id=processing_msg.chat_id,
                message_id=processing_msg.message_id,
                text="🔄 Processando PDF...",
                parse_mode='Markdown'
            )
            try:
                arquivo = await context.bot.get_file(doc.file_id)
                cid = get_cliente_id(update)
                nm = get_cliente_nome(update) or get_cliente_username(update)
                try:
                    safe_nm = re.sub(r'[^A-Za-z0-9._-]+', '_', nm).strip('_')
                except:
                    safe_nm = str(nm or '')
                local_path = os.path.join(os.getcwd(), f"__tmp_{cid}_{safe_nm}_{doc.file_id}.pdf")
                try:
                    await arquivo.download_to_drive(local_path)
                except:
                    url = f"https://api.telegram.org/file/bot{TELEGRAM_BOT_TOKEN}/{arquivo.file_path}"
                    r = await asyncio.to_thread(requests.get, url, timeout=30)
                    if getattr(r, "status_code", 0) == 200:
                        with open(local_path, "wb") as f:
                            f.write(r.content)
                try:
                    data = await _post_json_async(
                        f"{API_URL}/processar_pdf_totais",
                        {
                            "arquivo": local_path,
                            "cliente_id": str(update.effective_chat.id),
                            "cliente_nome": get_cliente_nome(update),
                            "username": get_cliente_username(update),
                        },
                        timeout=30
                    )
                except:
                    data = {"sucesso": False}
                transacoes = data.get("transacoes", []) if data.get("sucesso") else []
                if not transacoes:
                    try:
                        from app.services.pdf_extractor import extrair_totais_a_pagar_de_pdf
                        tot = await asyncio.to_thread(extrair_totais_a_pagar_de_pdf, None, path=local_path)
                        tot = tot or {}
                        transacoes = []
                        if isinstance(tot.get("total_a_pagar"), (int, float)):
                            transacoes.append({
                                "tipo": "0",
                                "valor": float(tot["total_a_pagar"]),
                                "categoria": "fatura",
                                "descricao": "Fatura - Total a pagar",
                                "moeda": "BRL",
                            })
                        if isinstance(tot.get("pagamento"), (int, float)):
                            transacoes.append({
                                "tipo": "0",
                                "valor": float(tot["pagamento"]),
                                "categoria": "pagamento-fatura",
                                "descricao": "Fatura - Pagamento efetuado",
                                "moeda": "BRL",
                            })
                    except:
                        transacoes = []
                if not transacoes:
                    msg_erro = "Não identifiquei transações neste PDF."
                    if isinstance(data, dict) and data.get("erro"):
                        msg_erro = f"⚠️ {data.get('erro')}"
                    await context.bot.edit_message_text(
                        chat_id=processing_msg.chat_id,
                        message_id=processing_msg.message_id,
                        text=msg_erro,
                        parse_mode='Markdown'
                    )
                    try:
                        if os.path.exists(local_path):
                            os.remove(local_path)
                    except:
                        pass
                    _bg_semaphore.release()
                    return
                tot = data.get("totais") if data.get("sucesso") else None
                resposta = criar_cabecalho("FATURA (TOTAIS EXTRAÍDOS)", 40)
                if tot:
                    ta = tot.get("total_a_pagar")
                    pg = tot.get("pagamento")
                    vc = tot.get("vencimento")
                    inst = tot.get("instituicao")
                    bd = tot.get("bandeira")
                    tp = tot.get("doc_tipo")
                    if tp == "cartao":
                        resposta += "🪪 *Tipo:* Fatura de cartão\n"
                    if inst:
                        resposta += f"🏦 *Instituição:* {md_escape(inst)}\n"
                    if bd:
                        resposta += f"💳 *Bandeira:* {md_escape(bd)}\n"
                    if isinstance(ta, (int, float)):
                        resposta += f"\n💳 *Total a pagar:* {formatar_moeda(float(ta))}\n"
                    if isinstance(pg, (int, float)):
                        resposta += f"💸 *Pagamento:* {formatar_moeda(float(pg))}\n"
                    if vc:
                        resposta += f"📅 *Vencimento:* {md_escape(vc)}\n"
                    resposta += "\n"
                resposta += f"✅ *{len(transacoes)} transação(ões) registrada(s)*\n\n"
                for t in transacoes:
                    emoji = "🔴" if str(t.get('tipo')) in ('0', 'saida') else "🟢"
                    tipo = "DESPESA" if str(t.get('tipo')) in ('0', 'saida') else "RECEITA"
                    cat_raw = t.get('categoria', 'outros')
                    cat_nome = md_escape(CATEGORY_NAMES.get(cat_raw, cat_raw))
                    desc_json = str(t.get('descricao', ''))
                    resposta += f"{emoji} *{tipo}:* {formatar_moeda(float(t.get('valor', 0)))}\n"
                    resposta += f"   `{desc_json}`\n"
                    resposta += f"   Categoria: {cat_nome}\n\n"
                if isinstance(data, dict) and data.get("erro_salvar"):
                    resposta += "⚠️ Não consegui salvar no Firestore.\n\n"
                try:
                    extrato = await _req_json_async(f"{API_URL}/extrato/hoje?{build_cliente_query_params(update)}", timeout=5)
                    if extrato.get("sucesso"):
                        tot = extrato.get("total", {})
                        resposta += "💰 *TOTAIS DO DIA*\n"
                        caixa = ""
                        caixa += "+" + ("-" * 28) + "+\n"
                        caixa += f"|{criar_linha_tabela('RECEITAS:', formatar_moeda(tot.get('receitas', 0), negrito=False), True, '', largura=28)}|\n"
                        caixa += f"|{criar_linha_tabela('DESPESAS:', formatar_moeda(tot.get('despesas', 0), negrito=False), True, '', largura=28)}|\n"
                        caixa += f"|{criar_linha_tabela('SALDO:', formatar_moeda(tot.get('saldo', 0), negrito=False), True, '', largura=28)}|\n"
                        caixa += "+" + ("-" * 28) + "+"
                        resposta += wrap_code_block(caixa) + "\n\n"
                except:
                    pass
                try:
                    if os.path.exists(local_path):
                        os.remove(local_path)
                except:
                    pass
                keyboard = [
                    [InlineKeyboardButton("💰 TOTAIS DO DIA", callback_data="total_dia"),
                     InlineKeyboardButton("📊 RESUMO", callback_data="resumo")],
                    [InlineKeyboardButton("🏠 MENU", callback_data="menu")]
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)
                await context.bot.edit_message_text(
                    chat_id=processing_msg.chat_id,
                    message_id=processing_msg.message_id,
                    text=resposta,
                    parse_mode='Markdown',
                    reply_markup=reply_markup
                )
                try:
                    salvas_pdf = (data.get("salvas", []) if isinstance(data, dict) else []) or []
                    asyncio.create_task(_disparar_confirmacoes(update, context, transacoes, salvas_pdf))
                except:
                    pass
                _bg_semaphore.release()
                return
            except:
                try:
                    _bg_semaphore.release()
                except:
                    pass
                await context.bot.edit_message_text(
                    chat_id=processing_msg.chat_id,
                    message_id=processing_msg.message_id,
                    text="⚠️ *Erro ao processar PDF. Tente novamente.*",
                    parse_mode='Markdown'
                )
                return
        if not str(doc.mime_type or "").startswith("image/"):
            await context.bot.edit_message_text(
                chat_id=processing_msg.chat_id,
                message_id=processing_msg.message_id,
                text="⚠️ Documento não é uma imagem suportada.",
                parse_mode='Markdown'
            )
            _bg_semaphore.release()
            return
        arquivo = await context.bot.get_file(doc.file_id)
        image_bytes = b""
        try:
            image_bytearray = await arquivo.download_as_bytearray()
            image_bytes = bytes(image_bytearray)
        except:
            try:
                url = f"https://api.telegram.org/file/bot{TELEGRAM_BOT_TOKEN}/{arquivo.file_path}"
                r = await asyncio.to_thread(requests.get, url, timeout=20)
                if getattr(r, "status_code", 0) == 200:
                    image_bytes = bytes(r.content)
            except:
                image_bytes = b""
        if not image_bytes:
            await context.bot.edit_message_text(
                chat_id=processing_msg.chat_id,
                message_id=processing_msg.message_id,
                text="⚠️ Erro ao baixar a imagem do documento.",
                parse_mode='Markdown'
            )
            _bg_semaphore.release()
            return
        transacoes = await asyncio.to_thread(extrair_informacoes_da_imagem, image_bytes) or []
        if not transacoes:
            await context.bot.edit_message_text(
                chat_id=processing_msg.chat_id,
                message_id=processing_msg.message_id,
                text="Não consegui identificar os dados financeiros nesta imagem. Verifique se o valor está visível",
                parse_mode='Markdown'
            )
            _bg_semaphore.release()
            return
        try:
            data = await _post_json_async(
                f"{API_URL}/processar",
                {
                    "transacoes": transacoes,
                    "texto_original": "documento:telegram",
                    "cliente_id": str(update.effective_chat.id),
                    "cliente_nome": get_cliente_nome(update),
                    "username": get_cliente_username(update),
                },
                timeout=10
            )
        except:
            data = {"sucesso": False}
        if data.get("sucesso"):
            transacoes = data.get("transacoes", transacoes)
            arq = data.get("arquivo")
        else:
            arq = None
        try:
            dedup = {}
            for item in transacoes:
                tipo_n = str(item.get('tipo')).strip()
                valor_n = float(item.get('valor', 0))
                desc_raw = str(item.get('descricao', ''))
                cat_n = str(item.get('categoria', '')).strip().lower()
                desc_final = re.sub(r'\s+', ' ', desc_raw or '').strip()
                try:
                    ai_d = sintetizar_descricao_curta(desc_final, categoria=cat_n)
                    if ai_d:
                        desc_final = ai_d
                except:
                    pass
                k = (tipo_n, valor_n, cat_n)
                cur = dedup.get(k)
                if cur is None or len(desc_final) <= len(str(cur.get('descricao', ''))):
                    novo = dict(item)
                    novo['descricao'] = desc_final
                    novo['categoria'] = cat_n
                    dedup[k] = novo
            transacoes = list(dedup.values())
        except:
            pass
        resposta = criar_cabecalho("TRANSAÇÃO REGISTRADA (DOCUMENTO)", 40)
        resposta += f"\n✅ *{len(transacoes)} transação(ões) registrada(s)*\n\n"
        for t in transacoes:
            emoji = "🔴" if str(t.get('tipo')) == '0' else "🟢"
            tipo = "DESPESA" if str(t.get('tipo')) == '0' else "RECEITA"
            cat_raw = t.get('categoria', 'outros')
            cat_nome = md_escape(CATEGORY_NAMES.get(cat_raw, cat_raw))
            desc_json = str(t.get('descricao', ''))
            resposta += f"{emoji} *{tipo}:* {formatar_moeda(float(t.get('valor', 0)))}\n"
            resposta += f"   `{desc_json}`\n"
            resposta += f"   Categoria: {cat_nome}\n\n"
        try:
            extrato = requests.get(f"{API_URL}/extrato/hoje?{build_cliente_query_params(update)}", timeout=4).json()
            if extrato.get("sucesso"):
                tot = extrato.get("total", {})
                resposta += "💰 *TOTAIS DO DIA*\n"
                caixa = ""
                caixa += "+" + ("-" * 28) + "+\n"
                caixa += f"|{criar_linha_tabela('RECEITAS:', formatar_moeda(tot.get('receitas', 0), negrito=False), True, '', largura=28)}|\n"
                caixa += f"|{criar_linha_tabela('DESPESAS:', formatar_moeda(tot.get('despesas', 0), negrito=False), True, '', largura=28)}|\n"
                caixa += f"|{criar_linha_tabela('SALDO:', formatar_moeda(tot.get('saldo', 0), negrito=False), True, '', largura=28)}|\n"
                caixa += "+" + ("-" * 28) + "+"
                resposta += wrap_code_block(caixa) + "\n\n"
        except:
            pass
        saldo_geral = obter_saldo_geral(str(update.effective_chat.id))
        resposta += f"💹 *SALDO ATUAL REAL:* {formatar_moeda(saldo_geral, negrito=True)}\n\n"
        if arq:
            safe_arq = md_escape(arq)
            resposta += f"💾 Salvo em: `{safe_arq}`\n\n"
        resposta += "📊 *Use /total para ver os totais atualizados*"
        keyboard = [
            [InlineKeyboardButton("💰 TOTAIS DO DIA", callback_data="total_dia"),
             InlineKeyboardButton("📅 TOTAIS DO MÊS", callback_data="analise_mes")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await context.bot.edit_message_text(
            chat_id=processing_msg.chat_id,
            message_id=processing_msg.message_id,
            text=resposta,
            parse_mode='Markdown',
            reply_markup=reply_markup
        )
        try:
            salvas_doc = (data.get("salvas", []) if isinstance(data, dict) else []) or []
            asyncio.create_task(_disparar_confirmacoes(update, context, transacoes, salvas_doc))
        except:
            pass
        _bg_semaphore.release()
    except:
        try:
            _bg_semaphore.release()
        except:
            pass
        await context.bot.edit_message_text(
            chat_id=processing_msg.chat_id,
            message_id=processing_msg.message_id,
            text="⚠️ *Erro ao processar documento. Tente novamente.*",
            parse_mode='Markdown'
        )
async def processar_mensagem_voz(update: Update, context: CallbackContext):
    msg = update.message
    processing_msg = await msg.reply_text("🔄 Processando áudio (voz)...")
    await _bg_semaphore.acquire()
    try:
        v = msg.voice
        if not v:
            await context.bot.edit_message_text(
                chat_id=processing_msg.chat_id,
                message_id=processing_msg.message_id,
                text="⚠️ Nenhuma mensagem de voz recebida.",
                parse_mode='Markdown'
            )
            return
        arquivo = await context.bot.get_file(v.file_id)
        audio_bytes = b""
        try:
            audio_bytearray = await arquivo.download_as_bytearray()
            audio_bytes = bytes(audio_bytearray)
        except:
            try:
                url = f"https://api.telegram.org/file/bot{TELEGRAM_BOT_TOKEN}/{arquivo.file_path}"
                r = await asyncio.to_thread(requests.get, url, timeout=20)
                if getattr(r, "status_code", 0) == 200:
                    audio_bytes = bytes(r.content)
            except:
                audio_bytes = b""
        if not audio_bytes:
            await context.bot.edit_message_text(
                chat_id=processing_msg.chat_id,
                message_id=processing_msg.message_id,
                text="⚠️ Erro ao baixar o áudio.",
                parse_mode='Markdown'
            )
            return
        from audio_processor import audio_processor
        texto = await asyncio.to_thread(audio_processor.transcribe_audio_file, audio_bytes, format='ogg')
        if not texto:
            try:
                err_msg = "Não consegui transcrever o áudio de voz. Envie como ÁUDIO (MP3/WAV) ou tente novamente."
                if getattr(audio_processor, "rate_limited", False):
                    err_msg = "Não consegui transcrever agora (cota de IA atingida). Envie como texto ou como ÁUDIO (MP3/WAV) e tente novamente em alguns minutos."
                await context.bot.edit_message_text(
                    chat_id=processing_msg.chat_id,
                    message_id=processing_msg.message_id,
                    text=err_msg,
                    parse_mode='Markdown'
                )
            except:
                pass
            return
        if re.search(r'\bestornar\b', texto, re.IGNORECASE):
            v, dr, tipo = extrair_campos_estorno(texto)
            if v and dr:
                await iniciar_fluxo_estorno_por_valor(update, context, v, dr, tipo=tipo, processamento=processing_msg)
                return
        try:
            data = await _post_json_async(
                f"{API_URL}/processar",
                {
                    "mensagem": texto,
                    "cliente_id": str(update.effective_chat.id),
                    "cliente_nome": get_cliente_nome(update),
                    "username": get_cliente_username(update),
                },
                timeout=6
            )
        except:
            data = {"sucesso": False}
        transacoes = []
        arq = None
        if data.get("sucesso"):
            transacoes = data.get("transacoes", [])
            arq = data.get("arquivo")
            try:
                salvas_voz = data.get("salvas", []) or []
            except:
                salvas_voz = []
        else:
            try:
                from app.services.extractor import extrair_informacoes_financeiras
                transacoes = (await asyncio.to_thread(extrair_informacoes_financeiras, texto)) if _gemini_ok() else []
                cid = str(update.effective_chat.id)
                await asyncio.to_thread(ensure_cliente, cid, nome=get_cliente_nome(update), username=get_cliente_username(update))
                salvas_voz = await asyncio.to_thread(salvar_transacao_cliente, transacoes, cliente_id=cid, origem="bot-audio")
            except:
                arq = None
        if not transacoes:
            try:
                from app.services.extractor import extrair_informacoes_financeiras
                transacoes = (await asyncio.to_thread(extrair_informacoes_financeiras, texto)) if _gemini_ok() else []
            except:
                transacoes = []
        if transacoes:
            try:
                def _desc_confusa(s):
                    t = str(s or '').strip()
                    if not t:
                        return True
                    tl = t.lower()
                    if tl in {'despesa', 'receita'}:
                        return True
                    if len(t.split()) < 3:
                        return True
                    if re.search(r'(?:\b(?:de|da|do|das|dos|para)\s*)$', tl):
                        return True
                    return False
                precisa_ai = any(_desc_confusa(str(it.get('descricao', ''))) for it in transacoes)
                if precisa_ai and _gemini_ok():
                    from app.services.extractor import extrair_informacoes_financeiras
                    ai_trans = extrair_informacoes_financeiras(texto) or []
                    transacoes = (transacoes or []) + ai_trans
                dedup = {}
                for item in transacoes:
                    tipo_n = str(item.get('tipo')).strip()
                    valor_n = float(item.get('valor', 0))
                    desc_raw = str(item.get('descricao', ''))
                    cat_n = str(item.get('categoria', '')).strip().lower()
                    desc_final = re.sub(r'\s+', ' ', desc_raw or '').strip()
                    toks = desc_final.split()
                    if len(toks) > 8:
                        desc_final = ' '.join(toks[:8])
                    k = (tipo_n, valor_n, cat_n)
                    cur = dedup.get(k)
                    if cur is None or len(desc_final) < len(str(cur.get('descricao', ''))):
                        novo = dict(item)
                        novo['descricao'] = desc_final
                        novo['categoria'] = cat_n
                        dedup[k] = novo
                transacoes = list(dedup.values())
            except:
                pass
        if not transacoes:
            try:
                kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton("🏠 MENU", callback_data="menu")],
                    [InlineKeyboardButton("💰 TOTAIS DO DIA", callback_data="total_dia")]
                ])
                await context.bot.edit_message_text(
                    chat_id=processing_msg.chat_id,
                    message_id=processing_msg.message_id,
                    text="⚠️ Comando inválido. Não identifiquei transações.\n\nDica: informe algo como:\n• gastei 50 no mercado\n• recebi 1000 de salário\n• transferi 300 para a mãe",
                    parse_mode='Markdown',
                    reply_markup=kb
                )
            except:
                pass
            return
        resposta = criar_cabecalho("TRANSAÇÃO REGISTRADA (ÁUDIO)", 40)
        resposta += f"\n✅ *{len(transacoes)} transação(ões) registrada(s)*\n\n"
        for t in transacoes:
            emoji = "🔴" if str(t.get('tipo')) == '0' else "🟢"
            tipo = "DESPESA" if str(t.get('tipo')) == '0' else "RECEITA"
            cat_raw = t.get('categoria', 'outros')
            cat_nome = md_escape(CATEGORY_NAMES.get(cat_raw, cat_raw))
            desc_json = str(t.get('descricao', ''))
            resposta += f"{emoji} *{tipo}:* {formatar_moeda(float(t.get('valor', 0)))}\n"
            resposta += f"   `{desc_json}`\n"
            resposta += f"   Categoria: {cat_nome}\n\n"
        if arq:
            safe_arq = md_escape(arq)
            resposta += f"💾 Salvo em: `{safe_arq}`\n\n"
        resposta += "📊 *Use /total para ver os totais atualizados*"
        keyboard = [
            [InlineKeyboardButton("💰 TOTAIS DO DIA", callback_data="total_dia"),
             InlineKeyboardButton("📅 TOTAIS DO MÊS", callback_data="analise_mes")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await context.bot.edit_message_text(
            chat_id=processing_msg.chat_id,
            message_id=processing_msg.message_id,
            text=resposta,
            parse_mode='Markdown',
            reply_markup=reply_markup
        )
        try:
            asyncio.create_task(_disparar_confirmacoes(update, context, transacoes, salvas_voz))
        except:
            pass
        async def _enviar_totais_audio():
            try:
                qs = build_cliente_query_params(update)
                t1 = asyncio.create_task(_req_json_async(f"{API_URL}/extrato/hoje?include_transacoes=false&{qs}", timeout=3))
                t2 = asyncio.create_task(_req_json_async(f"{API_URL}/saldo/atual?{qs}", timeout=3))
                extrato, sj = await asyncio.gather(t1, t2)
                if extrato.get("sucesso"):
                    tot = extrato.get("total", {})
                    caixa = ""
                    caixa += "+" + ("-" * 28) + "+\n"
                    caixa += f"|{criar_linha_tabela('RECEITAS:', formatar_moeda(tot.get('receitas', 0), negrito=False), True, '', largura=28)}|\n"
                    caixa += f"|{criar_linha_tabela('DESPESAS:', formatar_moeda(tot.get('despesas', 0), negrito=False), True, '', largura=28)}|\n"
                    caixa += f"|{criar_linha_tabela('SALDO:', formatar_moeda(tot.get('saldo', 0), negrito=False), True, '', largura=28)}|\n"
                    caixa += "+" + ("-" * 28) + "+"
                    msg = "💰 *TOTAIS DO DIA*\n" + wrap_code_block(caixa)
                    try:
                        if sj.get("sucesso"):
                            s = float(sj.get("total", {}).get("saldo_real", sj.get("total", {}).get("saldo", 0)) or 0)
                            msg += f"\n\n💹 *SALDO ATUAL REAL:* {formatar_moeda(s, negrito=True)}\n\n"
                    except:
                        pass
                    msg += "📊 *Use /total para ver os totais atualizados*"
                    await update.message.reply_text(msg, parse_mode='Markdown', reply_markup=reply_markup)
            except:
                pass
        asyncio.create_task(_enviar_totais_audio())
    except:
        await context.bot.edit_message_text(
            chat_id=processing_msg.chat_id,
            message_id=processing_msg.message_id,
            text="⚠️ *Erro ao processar áudio. Tente novamente.*",
            parse_mode='Markdown'
        )
    finally:
        try:
            _bg_semaphore.release()
        except:
            pass
if __name__ == '__main__':
    main()
async def processar_mensagem_audio(update: Update, context: CallbackContext):
    msg = update.message
    processing_msg = await msg.reply_text("🔄 Processando áudio...")
    await _bg_semaphore.acquire()
    try:
        a = msg.audio
        if not a:
            await context.bot.edit_message_text(
                chat_id=processing_msg.chat_id,
                message_id=processing_msg.message_id,
                text="⚠️ Nenhum áudio recebido.",
                parse_mode='Markdown'
            )
            return
        arquivo = await context.bot.get_file(a.file_id)
        audio_bytes = b""
        try:
            audio_bytearray = await arquivo.download_as_bytearray()
            audio_bytes = bytes(audio_bytearray)
        except:
            try:
                url = f"https://api.telegram.org/file/bot{TELEGRAM_BOT_TOKEN}/{arquivo.file_path}"
                r = await asyncio.to_thread(requests.get, url, timeout=20)
                if getattr(r, "status_code", 0) == 200:
                    audio_bytes = bytes(r.content)
            except:
                audio_bytes = b""
        if not audio_bytes:
            await context.bot.edit_message_text(
                chat_id=processing_msg.chat_id,
                message_id=processing_msg.message_id,
                text="⚠️ Erro ao baixar o áudio.",
                parse_mode='Markdown'
            )
            return
        mt = str(a.mime_type or '').lower()
        fn = str(a.file_name or '').lower()
        if fn.endswith('.m4a') or 'mp4' in mt:
            fmt = 'm4a'
        elif fn.endswith('.aac') or 'aac' in mt:
            fmt = 'aac'
        elif fn.endswith('.mp3') or 'mpeg' in mt:
            fmt = 'mp3'
        elif fn.endswith('.ogg') or fn.endswith('.oga') or fn.endswith('.opus') or 'ogg' in mt or 'opus' in mt:
            fmt = 'ogg'
        else:
            fmt = 'ogg'
        from audio_processor import audio_processor
        texto = await asyncio.to_thread(audio_processor.transcribe_audio_file, audio_bytes, format=fmt)
        if not texto:
            try:
                err_msg = "Não consegui transcrever o áudio."
                if getattr(audio_processor, "rate_limited", False):
                    err_msg = "Não consegui transcrever agora (cota de IA atingida). Envie como texto ou como ÁUDIO (MP3/WAV) e tente novamente em alguns minutos."
                await context.bot.edit_message_text(
                    chat_id=processing_msg.chat_id,
                    message_id=processing_msg.message_id,
                    text=err_msg,
                    parse_mode='Markdown'
                )
            except:
                pass
            return
        if re.search(r'\bestornar\b', texto, re.IGNORECASE):
            v, dr, tipo = extrair_campos_estorno(texto)
            if v and dr:
                await iniciar_fluxo_estorno_por_valor(update, context, v, dr, tipo=tipo, processamento=processing_msg)
                return
        try:
            data = await _post_json_async(
                f"{API_URL}/processar",
                {
                    "mensagem": texto,
                    "cliente_id": str(update.effective_chat.id),
                    "cliente_nome": get_cliente_nome(update),
                    "username": get_cliente_username(update),
                },
                timeout=10
            )
        except:
            data = {"sucesso": False}
        transacoes = []
        arq = None
        if data.get("sucesso"):
            transacoes = data.get("transacoes", [])
            arq = data.get("arquivo")
            try:
                salvas_audio = data.get("salvas", []) or []
            except:
                salvas_audio = []
        else:
            try:
                from app.services.extractor import extrair_informacoes_financeiras
                transacoes = await asyncio.to_thread(extrair_informacoes_financeiras, texto) or []
                cid = str(update.effective_chat.id)
                await asyncio.to_thread(ensure_cliente, cid, nome=get_cliente_nome(update), username=get_cliente_username(update))
                salvas_audio = await asyncio.to_thread(salvar_transacao_cliente, transacoes, cliente_id=cid, origem="bot-audio")
            except:
                arq = None
        if not transacoes:
            try:
                from app.services.extractor import extrair_informacoes_financeiras
                transacoes = await asyncio.to_thread(extrair_informacoes_financeiras, texto) or []
            except:
                transacoes = []
        if transacoes:
            try:
                def _desc_confusa(s):
                    t = str(s or '').strip()
                    if not t:
                        return True
                    tl = t.lower()
                    if tl in {'despesa', 'receita'}:
                        return True
                    if len(t.split()) < 3:
                        return True
                    if re.search(r'(?:\b(?:de|da|do|das|dos|para)\s*)$', tl):
                        return True
                    return False
                precisa_ai = any(_desc_confusa(str(it.get('descricao', ''))) for it in transacoes)
                if precisa_ai:
                    from app.services.extractor import extrair_informacoes_financeiras
                    ai_trans = extrair_informacoes_financeiras(texto) or []
                    transacoes = (transacoes or []) + ai_trans
                    dedup = {}
                    for item in transacoes:
                        tipo_n = str(item.get('tipo')).strip()
                        valor_n = float(item.get('valor', 0))
                        desc_raw = str(item.get('descricao', ''))
                        cat_n = str(item.get('categoria', '')).strip().lower()
                        desc_final = re.sub(r'\s+', ' ', desc_raw or '').strip()
                        toks = desc_final.split()
                        if len(toks) > 8:
                            desc_final = ' '.join(toks[:8])
                        k = (tipo_n, valor_n, cat_n)
                        cur = dedup.get(k)
                        if cur is None or len(desc_final) < len(str(cur.get('descricao', ''))):
                            novo = dict(item)
                            novo['descricao'] = desc_final
                            novo['categoria'] = cat_n
                            dedup[k] = novo
                    transacoes = list(dedup.values())
            except:
                pass
        if not transacoes:
            try:
                kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton("🏠 MENU", callback_data="menu")],
                    [InlineKeyboardButton("💰 TOTAIS DO DIA", callback_data="total_dia")]
                ])
                await context.bot.edit_message_text(
                    chat_id=processing_msg.chat_id,
                    message_id=processing_msg.message_id,
                    text="⚠️ Comando inválido. Não identifiquei transações.\n\nDica: informe algo como:\n• gastei 50 no mercado\n• recebi 1000 de salário\n• transferi 300 para a mãe",
                    parse_mode='Markdown',
                    reply_markup=kb
                )
            except:
                pass
            return
        resposta = criar_cabecalho("TRANSAÇÃO REGISTRADA (ÁUDIO)", 40)
        resposta += f"\n✅ *{len(transacoes)} transação(ões) registrada(s)*\n\n"
        for t in transacoes:
            emoji = "🔴" if str(t.get('tipo')) == '0' else "🟢"
            tipo = "DESPESA" if str(t.get('tipo')) == '0' else "RECEITA"
            cat_raw = t.get('categoria', 'outros')
            cat_nome = md_escape(CATEGORY_NAMES.get(cat_raw, cat_raw))
            desc_json = str(t.get('descricao', ''))
            resposta += f"{emoji} *{tipo}:* {formatar_moeda(float(t.get('valor', 0)))}\n"
            resposta += f"   `{desc_json}`\n"
            resposta += f"   Categoria: {cat_nome}\n\n"
        if arq:
            safe_arq = md_escape(arq)
            resposta += f"💾 Salvo em: `{safe_arq}`\n\n"
        resposta += "📊 *Use /total para ver os totais atualizados*"
        keyboard = [
            [InlineKeyboardButton("💰 VER TOTAIS DO DIA", callback_data="total_dia")],
            [InlineKeyboardButton("📊 VER RESUMO", callback_data="resumo")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await context.bot.edit_message_text(
            chat_id=processing_msg.chat_id,
            message_id=processing_msg.message_id,
            text=resposta,
            parse_mode='Markdown',
            reply_markup=reply_markup
        )
        try:
            asyncio.create_task(_disparar_confirmacoes(update, context, transacoes, salvas_audio))
        except:
            pass
        async def _enviar_totais_audio2():
            try:
                qs = build_cliente_query_params(update)
                t1 = asyncio.create_task(_req_json_async(f"{API_URL}/extrato/hoje?include_transacoes=false&{qs}", timeout=3))
                t2 = asyncio.create_task(_req_json_async(f"{API_URL}/saldo/atual?{qs}", timeout=3))
                extrato, sj = await asyncio.gather(t1, t2)
                if extrato.get("sucesso"):
                    tot = extrato.get("total", {})
                    caixa = ""
                    caixa += "+" + ("-" * 28) + "+\n"
                    caixa += f"|{criar_linha_tabela('RECEITAS:', formatar_moeda(tot.get('receitas', 0), negrito=False), True, '', largura=28)}|\n"
                    caixa += f"|{criar_linha_tabela('DESPESAS:', formatar_moeda(tot.get('despesas', 0), negrito=False), True, '', largura=28)}|\n"
                    caixa += f"|{criar_linha_tabela('SALDO:', formatar_moeda(tot.get('saldo', 0), negrito=False), True, '', largura=28)}|\n"
                    caixa += "+" + ("-" * 28) + "+"
                    msg = "💰 *TOTAIS DO DIA*\n" + wrap_code_block(caixa)
                    try:
                        if sj.get("sucesso"):
                            s = float(sj.get("total", {}).get("saldo_real", sj.get("total", {}).get("saldo", 0)) or 0)
                            msg += f"\n\n💹 *SALDO ATUAL REAL:* {formatar_moeda(s, negrito=True)}\n\n"
                    except:
                        pass
                    msg += "📊 *Use /total para ver os totais atualizados*"
                    await update.message.reply_text(msg, parse_mode='Markdown', reply_markup=reply_markup)
            except:
                pass
        asyncio.create_task(_enviar_totais_audio2())
    except:
        await context.bot.edit_message_text(
            chat_id=processing_msg.chat_id,
            message_id=processing_msg.message_id,
            text="⚠️ *Erro ao processar áudio. Tente novamente.*",
            parse_mode='Markdown'
        )
    finally:
        try:
            _bg_semaphore.release()
        except:
            pass
