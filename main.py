import argparse
import threading
import sys
import os
from datetime import datetime, timedelta, timezone
from app.config import TELEGRAM_BOT_TOKEN, API_HOST, API_PORT, GEMINI_API_KEY
import api_financeira
import telegram_bot
import json
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
    if not GEMINI_API_KEY:
        print("⚠️ GEMINI_API_KEY não configurada. O endpoint /processar retornará vazio.")
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
    td = float(mm_doc.get("total_saida", 0) or 0)
    tr = float(mm_doc.get("total_entrada", 0) or 0)
    taj = float(mm_doc.get("total_ajuste", 0) or 0)
    tes = float(mm_doc.get("total_estorno", 0) or 0)
    saldo = float(mm_doc.get("saldo_mes", (tr - td + taj)) or (tr - td + taj))
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
            s_td += float(dd.get("total_saida", 0) or 0)
            s_tr += float(dd.get("total_entrada", 0) or 0)
            s_taj += float(dd.get("total_ajuste", 0) or 0)
            s_tes += float(dd.get("total_estorno", 0) or 0)
            qv_sum += int(dd.get("quantidade_transacoes_validas", 0) or 0)
        except Exception:
            pass
        cur = cur + timedelta(days=1)
    s_saldo = s_tr - s_td + s_taj
    need_fix = False
    try:
        if abs(s_td - td) > 1e-6 or abs(s_tr - tr) > 1e-6 or abs(s_taj - taj) > 1e-6 or abs(s_tes - tes) > 1e-6 or abs(s_saldo - saldo) > 1e-6:
            need_fix = True
    except Exception:
        need_fix = True
    try:
        mm = {
            "total_entrada": float(s_tr or 0),
            "total_saida": float(s_td or 0),
            "total_ajuste": float(s_taj or 0),
            "total_estorno": float(s_tes or 0),
            "saldo_mes": float(s_saldo or 0),
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
    telegram_bot.main()

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
        t_api = threading.Thread(target=run_api, daemon=True)
        t_api.start()
        t_mon = threading.Thread(target=_consistency_monitor, kwargs={"interval_seconds": 300, "recent_hours": 8}, daemon=True)
        t_mon.start()
        run_bot()

if __name__ == "__main__":
    main()
