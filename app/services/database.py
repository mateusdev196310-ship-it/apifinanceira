import os
import re
from datetime import datetime
try:
    import firebase_admin
    from firebase_admin import credentials, firestore
except:
    firebase_admin = None
    credentials = None
    firestore = None
try:
    from app.services.rule_based import clean_desc, detect_category
except:
    def clean_desc(s):
        t = (s or "").strip()
        t = re.sub(r'^(?:hoje|amanha|amanhã|ontem)\s+', '', t, flags=re.IGNORECASE)
        t = re.sub(r'^(?:em|no|na|de|do|da|para|pra|por|me)\s+', '', t, flags=re.IGNORECASE)
        t = re.sub(r'^(?:ganhos?|gastos?|receitas?|receita)\s+(?:com|de|do|da)\s+', '', t, flags=re.IGNORECASE)
        t = re.sub(r'\bR\$\s*', '', t, flags=re.IGNORECASE)
        t = re.sub(r'\b(?:reais?|rs)\b', '', t, flags=re.IGNORECASE)
        return t.strip()
    def detect_category(text, verb=None):
        return 'outros'

_app = None
_db = None

def _cred_path():
    p1 = os.getenv("FIREBASE_CREDENTIALS")
    if p1:
        if os.path.exists(p1):
            return p1
        s1 = str(p1).strip()
        if s1.startswith("{"):
            runtime_path = os.path.join(os.getcwd(), "__runtime_firebase_cred.json")
            try:
                if not os.path.exists(runtime_path):
                    with open(runtime_path, "w", encoding="utf-8") as f:
                        f.write(s1)
                return runtime_path
            except:
                pass
    p0 = os.getenv("FIREBASE_CREDENTIALS_JSON")
    if p0:
        s0 = str(p0).strip()
        if s0.startswith("{"):
            runtime_path2 = os.path.join(os.getcwd(), "__runtime_firebase_cred.json")
            try:
                if not os.path.exists(runtime_path2):
                    with open(runtime_path2, "w", encoding="utf-8") as f:
                        f.write(s0)
                return runtime_path2
            except:
                pass
    p2 = os.path.join(os.getcwd(), "chave_firebase.json")
    if os.path.exists(p2):
        return p2
    p3 = os.path.join(os.getcwd(), "chave_firebase..json")
    if os.path.exists(p3):
        return p3
    p4 = os.path.join(os.getcwd(), "__restore_temp", "api_financeira", "chave_firebase.json")
    if os.path.exists(p4):
        return p4
    p5 = os.path.join(os.getcwd(), "__restore_temp", "api_financeira", "chave_firebase..json")
    if os.path.exists(p5):
        return p5
    p6 = os.path.join(os.path.dirname(__file__), "..", "..", "chave_firebase.json")
    if os.path.exists(p6):
        return p6
    p7 = os.path.join(os.path.dirname(__file__), "..", "..", "chave_firebase..json")
    if os.path.exists(p7):
        return p7
    return p2

def init_firebase():
    global _app, _db
    if _db is not None:
        return _db
    if firebase_admin is None or credentials is None or firestore is None:
        raise RuntimeError("firebase-admin não instalado")
    cred = credentials.Certificate(_cred_path())
    _app = firebase_admin.initialize_app(cred)
    _db = firestore.client()
    return _db

def get_db():
    return init_firebase()
def ensure_cliente(cliente_id, nome=None, username=None):
    db = get_db()
    root = _cliente_root(cliente_id)
    now = datetime.now().isoformat()
    try:
        doc = root.get()
        safe_nm = None
        try:
            base_nm = str(nome or username or "").strip()
            if base_nm:
                safe_nm = re.sub(r'[^A-Za-z0-9._-]+', '_', base_nm).strip('_')
        except:
            safe_nm = None
        label = f"{cliente_id}_{safe_nm}" if safe_nm else str(cliente_id)
        base = {
            "cliente_id": str(cliente_id),
            "cliente_nome": str(nome) if nome else None,
            "cliente_username": str(username) if username else None,
            "cliente_label": label,
            "cliente_display": (safe_nm or None),
            "last_seen": now,
        }
        if not doc.exists:
            base.update({"created_at": now})
            root.set({k: v for k, v in base.items() if v is not None}, merge=True)
        else:
            root.set({k: v for k, v in base.items() if v is not None}, merge=True)
    except:
        pass
    return True
def build_ref_id(dr: str, doc_id: str) -> str:
    dr_s = str(dr or "").strip()
    return f"{dr_s}__{str(doc_id)}"
def parse_ref_id(ref_id: str):
    s = str(ref_id or "").strip()
    if "__" in s:
        dr, did = s.split("__", 1)
        return dr, did
    return None, s
def _cliente_root(cliente_id):
    db = get_db()
    return db.collection("clientes").document(str(cliente_id or "default"))

_DESC_FIX = {
    'vndas': 'vendas',
    'vend': 'vendas',
    'vnedi': 'vendas',
    'receb': 'recebido',
    'salario': 'salário',
    'supermecado': 'supermercado',
    'mercadinho': 'mercado',
    'mercearia': 'mercado',
    'farmacia': 'farmácia',
    'uber': 'uber',
    'lanch': 'lanche',
    'onibus': 'ônibus',
    'combustivel': 'combustível',
    'taxi': 'táxi',
}
_EXP_CANON = [
    (r'\blanche\b', 'Lanche'),
    (r'\bpizza\b', 'Pizza'),
    (r'\bmercado\b', 'Mercado'),
    (r'\bsupermercado\b', 'Supermercado'),
    (r'\bfarm[áa]cia\b', 'Farmácia'),
    (r'\brestaurante\b', 'Restaurante'),
    (r'\buber\b', 'Uber'),
    (r'gasolina|combust[ií]vel', 'Gasolina'),
    (r'internet', 'Internet'),
    (r'streaming', 'Assinatura'),
    (r'assinatura', 'Assinatura'),
    (r'telefonia', 'Telefonia'),
    (r'aluguel', 'Aluguel'),
    (r'condom[ií]nio', 'Condomínio'),
    (r'energia', 'Energia'),
    (r'[áa]gua', 'Água'),
    (r'luz', 'Luz'),
]
_INC_CANON = [
    (r'sal[áa]ri', 'Salário'),
    (r'vend', 'Vendas'),
    (r'\bpix\b', 'Pix Recebido'),
    (r'transfer', 'Transferência'),
    (r'dep[óo]sito', 'Depósito'),
    (r'freela', 'Freela'),
    (r'servi[çc]o', 'Serviços'),
]

def _norm_valor(v):
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v or '').strip()
    s = s.replace('\u00A0', ' ').strip()
    s = s.replace('R$', '').strip()
    s = s.replace(' ', '')
    if ',' in s:
        s = s.replace('.', '').replace(',', '.')
    else:
        parts = s.split('.')
        if len(parts) > 1 and all(p.isdigit() for p in parts) and len(parts[-1]) != 2:
            s = ''.join(parts)
    try:
        return float(s)
    except:
        return 0.0

def _apply_fixes(s):
    t = (s or '').strip().lower()
    for k, v in _DESC_FIX.items():
        t = re.sub(r'\b' + re.escape(k) + r'\b', v, t, flags=re.IGNORECASE)
    return t

def _canon_desc(t, tipo, categoria):
    tl = (t or '').lower()
    arr = _INC_CANON if str(tipo).strip() == '1' else _EXP_CANON
    for pat, canon in arr:
        if re.search(pat, tl, re.IGNORECASE):
            return canon
    # fallback: título simples do texto limpo ou da categoria
    base = tl.strip()
    base = ' '.join(w for w in base.split() if not re.fullmatch(r'\d+(?:[.,]\d+)?', w))
    toks = []
    seen = set()
    for w in base.split():
        wl = w.lower()
        if wl in seen:
            continue
        seen.add(wl)
        toks.append(w)
        if len(toks) >= 3:
            break
    base = ' '.join(toks)
    if not base:
        if str(tipo).strip() == '1':
            return 'Receita' if (categoria or '') not in ('salario', 'vendas') else ('Salário' if categoria == 'salario' else 'Vendas')
        return 'Despesa'
    # Title-case básico respeitando acentos
    return ' '.join(x[:1].upper() + x[1:] for x in base.split())

def normalize_item_for_store(item):
    it = dict(item or {})
    tipo = str(it.get('tipo', '0')).strip()
    categoria = str(it.get('categoria', 'outros')).strip().lower()
    desc_raw = str(it.get('descricao', '')).strip()
    desc_clean = clean_desc(desc_raw)
    desc_fixed = _apply_fixes(desc_clean)
    desc_final = _canon_desc(desc_fixed, tipo, categoria)
    it['descricao'] = desc_final
    it['valor'] = _norm_valor(it.get('valor', 0))
    if not categoria or categoria == 'outros':
        categoria = detect_category(desc_final or desc_fixed)
    it['categoria'] = categoria
    return it

def salvar_no_firestore(dados):
    db = get_db()
    ts = datetime.now().isoformat()
    dr = datetime.now().strftime("%Y-%m-%d")
    arr = dados if isinstance(dados, list) else [dados]
    out = []
    for item in arr or []:
        payload = normalize_item_for_store(item or {})
        payload.setdefault("timestamp", ts)
        payload.setdefault("data_referencia", dr)
        db.collection("transacoes").add(payload)
        out.append(payload)
    return out
def salvar_transacao_cliente(dados, cliente_id="default", origem="api", referencia_id=None, tipo_operacao=None, motivo_ajuste=None):
    db = get_db()
    root = _cliente_root(cliente_id)
    dr = datetime.now().strftime("%Y-%m-%d")
    mr = datetime.now().strftime("%Y-%m")
    arr = dados if isinstance(dados, list) else [dados]
    out = []
    batch = db.batch()
    for item in arr or []:
        base = normalize_item_for_store(item or {})
        tp_raw = str(base.get("tipo", "0")).strip()
        tp_txt = "entrada" if tp_raw == "1" else "saida"
        if tipo_operacao in ("ajuste", "estorno"):
            tp_txt = tipo_operacao
        val = float(base.get("valor", 0) or 0)
        doc = {
            "valor": val,
            "tipo": tp_txt,
            "categoria": str(base.get("categoria", "outros")),
            "descricao": str(base.get("descricao", "")),
            "data_referencia": base.get("data_referencia") or dr,
            "timestamp_criacao": firestore.SERVER_TIMESTAMP,
            "moeda": str(item.get("moeda", "BRL")),
            "origem": origem,
            "referencia_id": referencia_id,
            "motivo_ajuste": motivo_ajuste,
            "imutavel": True,
        }
        day_ref = root.collection("transacoes").document(doc["data_referencia"])
        item_ref = day_ref.collection("items").document()
        doc["ref_id"] = build_ref_id(doc["data_referencia"], item_ref.id)
        batch.set(item_ref, doc)
        dref = root.collection("dias").document(doc["data_referencia"])
        mref = root.collection("meses").document(doc["data_referencia"][:7])
        inc_d = {"atualizado_em": firestore.SERVER_TIMESTAMP}
        inc_m = {"atualizado_em": firestore.SERVER_TIMESTAMP}
        cat = str(base.get("categoria", "outros"))
        if tp_txt == "entrada":
            inc_d.update({
                "quantidade_transacoes": firestore.Increment(1),
                "quantidade_transacoes_validas": firestore.Increment(1),
                "total_entrada": firestore.Increment(val),
                "saldo_dia": firestore.Increment(val)
            })
            inc_m.update({
                "quantidade_transacoes": firestore.Increment(1),
                "quantidade_transacoes_validas": firestore.Increment(1),
                "total_entrada": firestore.Increment(val),
                "saldo_mes": firestore.Increment(val)
            })
            inc_m.update({f"categorias_entrada.{cat}": firestore.Increment(val)})
            inc_d.update({f"categorias_entrada.{cat}": firestore.Increment(val)})
        elif tp_txt == "saida":
            inc_d.update({
                "quantidade_transacoes": firestore.Increment(1),
                "quantidade_transacoes_validas": firestore.Increment(1),
                "total_saida": firestore.Increment(val),
                "saldo_dia": firestore.Increment(-val)
            })
            inc_m.update({
                "quantidade_transacoes": firestore.Increment(1),
                "quantidade_transacoes_validas": firestore.Increment(1),
                "total_saida": firestore.Increment(val),
                "saldo_mes": firestore.Increment(-val)
            })
            inc_m.update({f"categorias_saida.{cat}": firestore.Increment(val)})
            inc_d.update({f"categorias_saida.{cat}": firestore.Increment(val)})
        elif tp_txt == "ajuste":
            inc_d.update({
                "quantidade_transacoes": firestore.Increment(1),
                "quantidade_transacoes_validas": firestore.Increment(1),
                "total_ajuste": firestore.Increment(val),
                "saldo_dia": firestore.Increment(val)
            })
            inc_m.update({
                "quantidade_transacoes": firestore.Increment(1),
                "quantidade_transacoes_validas": firestore.Increment(1),
                "total_ajuste": firestore.Increment(val),
                "saldo_mes": firestore.Increment(val)
            })
            inc_m.update({f"categorias_ajuste.{cat}": firestore.Increment(val)})
            inc_d.update({f"categorias_ajuste.{cat}": firestore.Increment(val)})
        elif tp_txt == "estorno":
            inc_d.update({"total_estorno": firestore.Increment(abs(val))})
            inc_m.update({"total_estorno": firestore.Increment(abs(val))})
            inc_m.update({f"categorias_estorno.{cat}": firestore.Increment(abs(val))})
            inc_d.update({f"categorias_estorno.{cat}": firestore.Increment(abs(val))})
        else:
            inc_d.update({
                "quantidade_transacoes": firestore.Increment(1),
                "quantidade_transacoes_validas": firestore.Increment(1),
                "total_ajuste": firestore.Increment(val),
                "saldo_dia": firestore.Increment(val)
            })
            inc_m.update({
                "quantidade_transacoes": firestore.Increment(1),
                "quantidade_transacoes_validas": firestore.Increment(1),
                "total_ajuste": firestore.Increment(val),
                "saldo_mes": firestore.Increment(val)
            })
            inc_m.update({f"categorias_ajuste.{cat}": firestore.Increment(val)})
            inc_d.update({f"categorias_ajuste.{cat}": firestore.Increment(val)})
        batch.set(dref, inc_d, merge=True)
        batch.set(mref, inc_m, merge=True)
        out.append(doc)
    batch.commit()
    return out
def estornar_transacao(cliente_id, referencia_id, motivo=None, origem="api"):
    db = get_db()
    root = _cliente_root(cliente_id)
    dr, did = parse_ref_id(referencia_id)
    if dr:
        tdoc = root.collection("transacoes").document(dr).collection("items").document(did).get()
    else:
        tdoc = root.collection("transacoes").document(str(referencia_id)).get()
    if not tdoc.exists:
        return None
    o = tdoc.to_dict() or {}
    if o.get("estornado"):
        return {
            "valor": float(o.get("valor", 0) or 0),
            "tipo": "estorno",
            "categoria": str(o.get("categoria", "outros")),
            "descricao": "estorno",
            "data_referencia": str(o.get("data_referencia") or datetime.now().strftime("%Y-%m-%d")),
            "moeda": str(o.get("moeda", "BRL")),
            "origem": origem,
            "referencia_id": str(referencia_id),
            "motivo_ajuste": motivo,
            "imutavel": True,
        }
    val = float(o.get("valor", 0) or 0)
    tp_raw = str(o.get("tipo", "entrada")).strip().lower()
    if tp_raw in ("0", "despesa"):
        tp = "saida"
    elif tp_raw in ("1", "receita"):
        tp = "entrada"
    else:
        tp = tp_raw
    dr = str(o.get("data_referencia") or datetime.now().strftime("%Y-%m-%d"))
    if tp not in ("entrada", "saida"):
        return None
    payload = {
        "valor": abs(val),
        "tipo": "estorno",
        "categoria": str(o.get("categoria", "outros")),
        "descricao": "estorno",
        "data_referencia": dr,
        "timestamp_criacao": firestore.SERVER_TIMESTAMP,
        "moeda": str(o.get("moeda", "BRL")),
        "origem": origem,
        "referencia_id": str(referencia_id),
        "motivo_ajuste": motivo,
        "imutavel": True,
    }
    batch = db.batch()
    estorno_day = root.collection("transacoes").document(dr)
    estorno_ref = estorno_day.collection("items").document()
    payload["ref_id"] = build_ref_id(dr, estorno_ref.id)
    batch.set(estorno_ref, payload)
    if parse_ref_id(referencia_id)[0]:
        _, orig_id = parse_ref_id(referencia_id)
        orig_ref = root.collection("transacoes").document(dr).collection("items").document(orig_id)
    else:
        orig_ref = root.collection("transacoes").document(str(referencia_id))
    try:
        batch.update(orig_ref, {"estornado": True, "atualizado_em": firestore.SERVER_TIMESTAMP})
    except:
        pass
    dref = root.collection("dias").document(dr)
    mref = root.collection("meses").document(dr[:7])
    cat = str(o.get("categoria", "outros"))
    inc_d = {"atualizado_em": firestore.SERVER_TIMESTAMP}
    inc_m = {"atualizado_em": firestore.SERVER_TIMESTAMP}
    abs_val = abs(val)
    inc_d.update({"total_estorno": firestore.Increment(abs_val)})
    inc_m.update({
        "total_estorno": firestore.Increment(abs_val),
        f"categorias_estorno.{cat}": firestore.Increment(abs_val),
    })
    inc_d.update({
        f"categorias_estorno.{cat}": firestore.Increment(abs_val),
    })
    if tp == "entrada":
        inc_d.update({
            "total_entrada": firestore.Increment(-val),
            "saldo_dia": firestore.Increment(-val),
            f"categorias_entrada.{cat}": firestore.Increment(-val),
        })
        inc_m.update({
            "total_entrada": firestore.Increment(-val),
            "saldo_mes": firestore.Increment(-val),
            f"categorias_entrada.{cat}": firestore.Increment(-val),
        })
    else:
        inc_d.update({
            "total_saida": firestore.Increment(-val),
            "saldo_dia": firestore.Increment(val),
            f"categorias_saida.{cat}": firestore.Increment(-val),
        })
        inc_m.update({
            "total_saida": firestore.Increment(-val),
            "saldo_mes": firestore.Increment(val),
            f"categorias_saida.{cat}": firestore.Increment(-val),
        })
    batch.set(dref, inc_d, merge=True)
    batch.set(mref, inc_m, merge=True)
    batch.commit()
    return payload
def backup_firestore_data(output_path=None):
    db = get_db()
    ts = datetime.now().strftime("%Y%m%dT%H%M%S")
    path = output_path or os.path.join(os.getcwd(), f"backup_firestore_{ts}.json")
    data = {"clientes": {}, "transacoes_root": []}
    try:
        for d in db.collection("transacoes").stream():
            data["transacoes_root"].append(d.to_dict() or {})
    except:
        pass
    try:
        for c in db.collection("clientes").stream():
            cid = c.id
            data["clientes"][cid] = {"transacoes": [], "dias": {}, "meses": {}}
            for t in db.collection("clientes").document(cid).collection("transacoes").stream():
                data["clientes"][cid]["transacoes"].append(t.to_dict() or {})
            for d in db.collection("clientes").document(cid).collection("dias").stream():
                data["clientes"][cid]["dias"][d.id] = d.to_dict() or {}
            for m in db.collection("clientes").document(cid).collection("meses").stream():
                data["clientes"][cid]["meses"][m.id] = m.to_dict() or {}
    except:
        pass
    try:
        with open(path, "w", encoding="utf-8") as f:
            import json as _json
            _json.dump(data, f, ensure_ascii=False, indent=2)
    except:
        return None
    return path
def migrate_cliente_transacoes_to_nested(cliente_id: str, delete_original: bool = False):
    db = get_db()
    root = _cliente_root(cliente_id)
    moved = 0
    skipped = 0
    errors = 0
    try:
        docs = list(root.collection("transacoes").stream())
    except Exception:
        docs = []
    for d in docs:
        try:
            o = d.to_dict() or {}
            dr = str(o.get("data_referencia") or "").strip()
            if not dr or len(dr) != 10:
                skipped += 1
                continue
            items = root.collection("transacoes").document(dr).collection("items")
            # Avoid duplicates: reuse original id
            tgt = items.document(d.id)
            tdoc = tgt.get()
            if tdoc.exists:
                skipped += 1
                # Optionally delete original even if exists
                if delete_original:
                    try:
                        root.collection("transacoes").document(d.id).delete()
                    except:
                        pass
                continue
            payload = dict(o)
            payload["ref_id"] = build_ref_id(dr, d.id)
            tgt.set(payload)
            moved += 1
            if delete_original:
                try:
                    root.collection("transacoes").document(d.id).delete()
                except:
                    pass
        except Exception:
            errors += 1
    return {"cliente_id": str(cliente_id), "moved": moved, "skipped": skipped, "errors": errors}
def recompute_cliente_aggregates(cliente_id: str):
    db = get_db()
    root = _cliente_root(cliente_id)
    month_agg = {}
    days_processed = 0
    try:
        docs = list(root.collection("transacoes").stream())
    except Exception:
        docs = []
    for d in docs:
        dr = str(d.id or "").strip()
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", dr or ""):
            continue
        total_entrada = 0.0
        total_saida = 0.0
        total_ajuste = 0.0
        total_estorno = 0.0
        qtd = 0
        qtd_validas = 0
        day_cat_inc = {}
        day_cat_exp = {}
        day_cat_est = {}
        day_cat_adj = {}
        try:
            items = list(root.collection("transacoes").document(dr).collection("items").stream())
        except Exception:
            items = []
        for it in items:
            o = it.to_dict() or {}
            if bool(o.get("estornado", False)):
                continue
            tp_raw = str(o.get("tipo", "")).strip().lower()
            cat = str(o.get("categoria", "outros") or "outros").strip().lower()
            val = float(o.get("valor", 0) or 0)
            if tp_raw in ("1", "receita", "entrada"):
                total_entrada += val
                qtd += 1
                qtd_validas += 1
                mk = dr[:7]
                m = month_agg.setdefault(mk, {
                    "total_entrada": 0.0,
                    "total_saida": 0.0,
                    "total_ajuste": 0.0,
                    "total_estorno": 0.0,
                    "quantidade_transacoes": 0,
                    "quantidade_transacoes_validas": 0,
                    "categorias_entrada": {},
                    "categorias_saida": {},
                    "categorias_estorno": {},
                    "categorias_ajuste": {},
                })
                m["total_entrada"] += val
                m["quantidade_transacoes"] += 1
                m["quantidade_transacoes_validas"] += 1
                m["categorias_entrada"][cat] = float(m["categorias_entrada"].get(cat, 0.0) or 0.0) + val
                day_cat_inc[cat] = float(day_cat_inc.get(cat, 0.0) or 0.0) + val
            elif tp_raw in ("0", "despesa", "saida"):
                total_saida += val
                qtd += 1
                qtd_validas += 1
                mk = dr[:7]
                m = month_agg.setdefault(mk, {
                    "total_entrada": 0.0,
                    "total_saida": 0.0,
                    "total_ajuste": 0.0,
                    "total_estorno": 0.0,
                    "quantidade_transacoes": 0,
                    "quantidade_transacoes_validas": 0,
                    "categorias_entrada": {},
                    "categorias_saida": {},
                    "categorias_estorno": {},
                    "categorias_ajuste": {},
                })
                m["total_saida"] += val
                m["quantidade_transacoes"] += 1
                m["quantidade_transacoes_validas"] += 1
                m["categorias_saida"][cat] = float(m["categorias_saida"].get(cat, 0.0) or 0.0) + val
                day_cat_exp[cat] = float(day_cat_exp.get(cat, 0.0) or 0.0) + val
            elif tp_raw in ("ajuste",):
                total_ajuste += val
                qtd += 1
                qtd_validas += 1
                mk = dr[:7]
                m = month_agg.setdefault(mk, {
                    "total_entrada": 0.0,
                    "total_saida": 0.0,
                    "total_ajuste": 0.0,
                    "total_estorno": 0.0,
                    "quantidade_transacoes": 0,
                    "quantidade_transacoes_validas": 0,
                    "categorias_entrada": {},
                    "categorias_saida": {},
                    "categorias_estorno": {},
                    "categorias_ajuste": {},
                })
                m["total_ajuste"] += val
                m["quantidade_transacoes"] += 1
                m["quantidade_transacoes_validas"] += 1
                m["categorias_ajuste"][cat] = float(m["categorias_ajuste"].get(cat, 0.0) or 0.0) + val
                day_cat_adj[cat] = float(day_cat_adj.get(cat, 0.0) or 0.0) + val
            elif tp_raw in ("estorno",):
                total_estorno += abs(val)
                mk = dr[:7]
                m = month_agg.setdefault(mk, {
                    "total_entrada": 0.0,
                    "total_saida": 0.0,
                    "total_ajuste": 0.0,
                    "total_estorno": 0.0,
                    "quantidade_transacoes": 0,
                    "quantidade_transacoes_validas": 0,
                    "categorias_entrada": {},
                    "categorias_saida": {},
                    "categorias_estorno": {},
                    "categorias_ajuste": {},
                })
                m["total_estorno"] += abs(val)
                m["categorias_estorno"][cat] = float(m["categorias_estorno"].get(cat, 0.0) or 0.0) + abs(val
                )
                day_cat_est[cat] = float(day_cat_est.get(cat, 0.0) or 0.0) + abs(val)
        saldo_dia = total_entrada - total_saida + total_ajuste
        try:
            root.collection("dias").document(dr).set({
                "total_entrada": total_entrada,
                "total_saida": total_saida,
                "total_ajuste": total_ajuste,
                "total_estorno": total_estorno,
                "saldo_dia": saldo_dia,
                "quantidade_transacoes": qtd,
                "quantidade_transacoes_validas": qtd_validas,
                "categorias_entrada": dict({k: float(v or 0.0) for k, v in day_cat_inc.items()}),
                "categorias_saida": dict({k: float(v or 0.0) for k, v in day_cat_exp.items()}),
                "categorias_estorno": dict({k: float(v or 0.0) for k, v in day_cat_est.items()}),
                "categorias_ajuste": dict({k: float(v or 0.0) for k, v in day_cat_adj.items()}),
                "atualizado_em": firestore.SERVER_TIMESTAMP,
            }, merge=True)
        except Exception:
            pass
        days_processed += 1
    months_processed = 0
    for mk, m in month_agg.items():
        saldo_mes = m["total_entrada"] - m["total_saida"] + m["total_ajuste"]
        try:
            root.collection("meses").document(mk).set({
                "total_entrada": float(m["total_entrada"] or 0.0),
                "total_saida": float(m["total_saida"] or 0.0),
                "total_ajuste": float(m["total_ajuste"] or 0.0),
                "total_estorno": float(m["total_estorno"] or 0.0),
                "saldo_mes": float(saldo_mes or 0.0),
                "quantidade_transacoes": int(m["quantidade_transacoes"] or 0),
                "quantidade_transacoes_validas": int(m["quantidade_transacoes_validas"] or 0),
                "categorias_entrada": dict(m["categorias_entrada"] or {}),
                "categorias_saida": dict(m["categorias_saida"] or {}),
                "categorias_estorno": dict(m["categorias_estorno"] or {}),
                "categorias_ajuste": dict(m["categorias_ajuste"] or {}),
                "atualizado_em": firestore.SERVER_TIMESTAMP,
            }, merge=True)
        except Exception:
            pass
        months_processed += 1
    return {
        "cliente_id": str(cliente_id),
        "dias_processados": days_processed,
        "meses_processados": months_processed,
    }
def migrate_all_clientes(delete_original: bool = False, recompute: bool = True):
    db = get_db()
    results = []
    try:
        clientes = list(db.collection("clientes").stream())
    except Exception:
        clientes = []
    for c in clientes:
        cid = c.id
        res_mig = migrate_cliente_transacoes_to_nested(cid, delete_original=delete_original)
        res_rec = None
        if recompute:
            try:
                res_rec = recompute_cliente_aggregates(cid)
            except Exception:
                res_rec = None
        results.append({"migracao": res_mig, "recompute": res_rec})
    return {
        "sucesso": True,
        "clientes_processados": len(results),
        "detalhes": results,
    }
