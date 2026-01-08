import argparse
import threading
import sys
import os
import time
from datetime import datetime, timedelta, timezone
from app.config import TELEGRAM_BOT_TOKEN, API_HOST, API_PORT, GEMINI_API_KEY, DEEPSEEK_API_KEY
import api_financeira
import telegram_bot
import json
import requests
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
def _now_sp():
    try:
        now_utc = datetime.now(timezone.utc)
        tz = None
        try:
            tz = ZoneInfo("America/Sao_Paulo") if ZoneInfo else None
        except Exception:
            tz = None
        return now_utc.astimezone(tz) if tz is not None else (now_utc + timedelta(hours=-3))
    except:
        return datetime.now()
def _month_key_sp():
    try:
        return _now_sp().strftime("%Y-%m")
    except:
        return datetime.now().strftime("%Y-%m")
try:
    from app.services.database import migrate_all_clientes, migrate_cliente_transacoes_to_nested, recompute_cliente_aggregates, get_db, firestore
except Exception:
    migrate_all_clientes = None
    migrate_cliente_transacoes_to_nested = None
    recompute_cliente_aggregates = None
    get_db = None
    firestore = None

def run_api():
    os.environ["FLASK_SKIP_DOTENV"] = "1"
    if not GEMINI_API_KEY and not DEEPSEEK_API_KEY:
        print("⚠️ Nenhuma IA configurada (GEMINI_API_KEY/DEEPSEEK_API_KEY). O endpoint /processar retornará vazio.")
    api_financeira.app.run(host=API_HOST, port=API_PORT, debug=True, use_reloader=False)
def _ensure_month_consistency(cliente_id: str, mes_atual: str):
    if get_db is None:
        return
    db = get_db()
    root = db.collection("clientes").document(cliente_id)
    try:
        mm_doc = root.collection("meses").document(mes_atual).get().to_dict() or {}
    except Exception:
        mm_doc = {}
    tmm = dict(mm_doc.get("totais_mes", {}) or {})
    td = float(tmm.get("total_saida", mm_doc.get("total_saida", 0)) or 0)
    tr = float(tmm.get("total_entrada", mm_doc.get("total_entrada", 0)) or 0)
    taj = float(tmm.get("total_ajuste", mm_doc.get("total_ajuste", 0)) or 0)
    tes = float(tmm.get("total_estorno", mm_doc.get("total_estorno", 0)) or 0)
    try:
        saldo = float((tmm.get("saldo_mes") if (tmm.get("saldo_mes") is not None) else mm_doc.get("saldo_mes")))
    except Exception:
        saldo = (tr - td + tes + taj)
    try:
        if (td + tr + taj + tes) > 0 and mm_doc.get("saldo_mes") is not None:
            return
    except Exception:
        pass
    ano, mes = mes_atual.split("-")
    dt_ini = f"{ano}-{mes}-01"
    if mes == "12":
        dt_fim = f"{int(ano)+1}-01-01"
    else:
        dt_fim = f"{ano}-{int(mes)+1:02d}-01"
    cur = datetime.strptime(dt_ini, "%Y-%m-%d")
    end = datetime.strptime(dt_fim, "%Y-%m-%d")
    s_td = s_tr = s_taj = s_tes = 0.0
    qv_sum = 0
    while cur < end:
        dkey = cur.strftime("%Y-%m-%d")
        try:
            dd = root.collection("dias").document(dkey).get().to_dict() or {}
            tdd = dict(dd.get("totais_dia", {}) or {})
            s_td += float(tdd.get("total_saida", dd.get("total_saida", 0)) or 0)
            s_tr += float(tdd.get("total_entrada", dd.get("total_entrada", 0)) or 0)
            s_taj += float(tdd.get("total_ajuste", dd.get("total_ajuste", 0)) or 0)
            s_tes += float(tdd.get("total_estorno", dd.get("total_estorno", 0)) or 0)
            qv_sum += int(dd.get("quantidade_transacoes_validas", 0) or 0)
        except Exception:
            pass
        cur = cur + timedelta(days=1)
    s_saldo = s_tr - s_td + s_tes + s_taj
    need_fix = False
    try:
        if abs(s_td - td) > 1e-6 or abs(s_tr - tr) > 1e-6 or abs(s_taj - taj) > 1e-6 or abs(s_tes - tes) > 1e-6 or abs(s_saldo - saldo) > 1e-6:
            need_fix = True
    except Exception:
        need_fix = True
    try:
        mm = {
            "totais_mes": {
                "total_entrada": float(s_tr or 0),
                "total_saida": float(s_td or 0),
                "total_ajuste": float(s_taj or 0),
                "total_estorno": float(s_tes or 0),
                "saldo_mes": float(s_saldo or 0),
            },
            "quantidade_transacoes_validas": int(qv_sum or 0),
            "atualizado_em": firestore.SERVER_TIMESTAMP,
        }
        if need_fix:
            root.collection("meses").document(mes_atual).set(mm, merge=True)
    except Exception:
        pass
def _consistency_monitor(interval_seconds: int = 300, recent_hours: int = 8):
    if get_db is None:
        return
    try:
        db = get_db()
    except Exception:
        return
    while True:
        try:
            now = _now_sp()
            mes_atual = _month_key_sp()
            try:
                clientes = list(db.collection("clientes").stream())
            except Exception:
                clientes = []
            for c in clientes:
                cid = c.id
                o = c.to_dict() or {}
                ls = str(o.get("last_seen") or "")
                ok = True
                try:
                    if ls:
                        dt = datetime.fromisoformat(ls.replace("Z", "+00:00"))
                        try:
                            dt_sp = dt.astimezone(_TZ_SP) if _TZ_SP is not None else dt.astimezone()
                        except Exception:
                            dt_sp = dt.astimezone()
                        now_naive = now.replace(tzinfo=None)
                        delta = now_naive - dt_sp.replace(tzinfo=None)
                        ok = delta.total_seconds() <= recent_hours * 3600
                except Exception:
                    ok = True
                if not ok:
                    continue
                _ensure_month_consistency(cid, mes_atual)
        except Exception:
            pass
        try:
            import time as _t
            _t.sleep(interval_seconds)
        except Exception:
            break

def run_bot():
    if not TELEGRAM_BOT_TOKEN:
        print("❌ TELEGRAM_BOT_TOKEN não configurado.")
        print("Defina no .env ou na sessão atual:")
        print("  .env -> TELEGRAM_BOT_TOKEN=SEU_TOKEN")
        print("  PowerShell -> $env:TELEGRAM_BOT_TOKEN=\"SEU_TOKEN\"")
        sys.exit(1)
    try:
        os.environ["BOT_DISABLE_LOCK"] = "1"
        os.environ["BOT_LOCK_TTL"] = "0"
    except Exception:
        pass
    try:
        import asyncio
        try:
            asyncio.get_event_loop()
        except:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
    except:
        pass
    telegram_bot.main()
_T_API = None
_T_BOT = None
_last_api_restart = 0.0
def _start_api_thread():
    global _T_API
    t = threading.Thread(target=run_api, daemon=True)
    t.start()
    _T_API = t
def _start_bot_thread():
    global _T_BOT
    t = threading.Thread(target=run_bot, daemon=True)
    t.start()
    _T_BOT = t
def _health_monitor(interval_seconds: int = 30):
    global _T_API, _last_api_restart
    while True:
        try:
            host = API_HOST if API_HOST and API_HOST != "0.0.0.0" else "127.0.0.1"
            url = f"http://{host}:{API_PORT}/health"
            ok = False
            try:
                r = requests.get(url, timeout=4)
                j = r.json() if r.status_code == 200 else {}
                ok = bool(j.get("status") == "online")
            except:
                ok = False
            if not ok:
                alive = (_T_API.is_alive() if _T_API else False)
                now = time.time()
                if (not alive) or (now - _last_api_restart >= 20):
                    _start_api_thread()
                    _last_api_restart = now
        except:
            pass
        try:
            time.sleep(interval_seconds)
        except:
            break
def _supervisor_loop(interval_seconds: int = 15):
    global _T_API, _T_BOT
    while True:
        try:
            if not (_T_API and _T_API.is_alive()):
                _start_api_thread()
            if not (_T_BOT and _T_BOT.is_alive()):
                _start_bot_thread()
        except:
            pass
        try:
            time.sleep(interval_seconds)
        except:
            break

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--service", choices=["api", "bot", "both"], default="api")
    parser.add_argument("--migrar_todos", action="store_true", help="Migrar transações para todos os clientes")
    parser.add_argument("--cliente_id", type=str, help="Migrar/Recomputar apenas este cliente")
    parser.add_argument("--delete_original", action="store_true", help="Apagar documentos antigos após migrar")
    parser.add_argument("--no_recompute", action="store_true", help="Não recomputar agregados após migrar")
    args = parser.parse_args()
    if args.migrar_todos or args.cliente_id:
        if migrate_all_clientes is None:
            print("❌ Dependências do Firestore não estão disponíveis.")
            sys.exit(1)
        recompute = not args.no_recompute
        if args.migrar_todos:
            res = migrate_all_clientes(delete_original=args.delete_original, recompute=recompute)
            print(json.dumps(res, ensure_ascii=False, indent=2))
            return
        if args.cliente_id:
            res_mig = migrate_cliente_transacoes_to_nested(args.cliente_id, delete_original=args.delete_original)
            res_rec = recompute_cliente_aggregates(args.cliente_id) if recompute else None
            print(json.dumps({"migracao": res_mig, "recompute": res_rec}, ensure_ascii=False, indent=2))
            return
    if args.service == "api":
        t_mon = threading.Thread(target=_consistency_monitor, kwargs={"interval_seconds": 300, "recent_hours": 8}, daemon=True)
        t_mon.start()
        run_api()
    elif args.service == "bot":
        run_bot()
    else:
        _start_api_thread()
        t_mon = threading.Thread(target=_consistency_monitor, kwargs={"interval_seconds": 300, "recent_hours": 8}, daemon=True)
        t_mon.start()
        t_h = threading.Thread(target=_health_monitor, kwargs={"interval_seconds": 30}, daemon=True)
        t_h.start()
        run_bot()

if __name__ == "__main__":
    main()
