import os
import re
import unicodedata
from datetime import datetime, timezone, timedelta
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

def _canon_category(cat):
    k = str(cat or 'outros').strip().lower()
    try:
        k = ''.join(c for c in unicodedata.normalize('NFKD', k) if not unicodedata.combining(c))
    except:
        pass
    k = re.sub(r'[^a-z0-9]+', ' ', k).strip()
    if k in {
        'alimentacao','transporte','moradia','saude','lazer','vestuario','servicos','salario','vendas','outros','duvida'
    }:
        return k
    try:
        mapped = detect_category(k)
        return mapped if mapped else (k or 'outros')
    except:
        return (k or 'outros')

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
    try:
        tm = str(os.getenv("TEST_MODE") or "").strip().lower()
    except Exception:
        tm = ""
    if tm in ("1", "true", "yes", "on"):
        class _FakeDocSnap:
            def __init__(self, _id, _data, _exists):
                self.id = _id
                self._data = _data
                self.exists = bool(_exists)
            def to_dict(self):
                try:
                    return dict(self._data or {})
                except Exception:
                    return {}
        class _FakeDocRef:
            def __init__(self, store, path):
                self._store = store
                self._path = path
                try:
                    self.id = path.rsplit("/", 1)[-1]
                except Exception:
                    self.id = path
            def get(self):
                data = self._store.get(self._path)
                return _FakeDocSnap(self.id, (data or {}), (data is not None))
            def set(self, payload, merge=True):
                cur = dict(self._store.get(self._path) or {})
                nxt = dict(payload or {})
                if merge:
                    try:
                        for k, v in nxt.items():
                            if isinstance(v, dict) and isinstance(cur.get(k), dict):
                                d = dict(cur.get(k) or {})
                                for kk, vv in v.items():
                                    d[kk] = vv
                                cur[k] = d
                            else:
                                cur[k] = v
                        self._store[self._path] = cur
                        return
                    except Exception:
                        pass
                self._store[self._path] = nxt
            def delete(self):
                try:
                    if self._path in self._store:
                        del self._store[self._path]
                except Exception:
                    pass
            def collection(self, name):
                return _FakeCollectionRef(self._store, f"{self._path}/{str(name)}")
        class _FakeQueryRef:
            def __init__(self, store, base_path, cond):
                self._store = store
                self._path = base_path
                self._cond = cond
            def stream(self):
                return []
        class _FakeCollectionRef:
            def __init__(self, store, path):
                self._store = store
                self._path = path
            def document(self, doc_id):
                return _FakeDocRef(self._store, f"{self._path}/{str(doc_id)}")
            def stream(self):
                out = []
                prefix = f"{self._path}/"
                plen = len(self._path.split("/"))
                for k, v in list(self._store.items()):
                    if k.startswith(prefix):
                        try:
                            segs = k.split("/")
                            if len(segs) == plen + 1:
                                out.append(_FakeDocSnap(segs[-1], (v or {}), True))
                        except Exception:
                            pass
                return out
            def where(self, field, op, value):
                return _FakeQueryRef(self._store, self._path, (field, op, value))
        class _FakeFirestoreClient:
            def __init__(self):
                self._store = {}
            def collection(self, name):
                return _FakeCollectionRef(self._store, str(name))
        class _FakeIncr:
            def __init__(self, val):
                self.value = val
        class _FakeFirestore:
            SERVER_TIMESTAMP = 0
            @staticmethod
            def Increment(val):
                return _FakeIncr(val)
        _db = _FakeFirestoreClient()
        globals()["firestore"] = _FakeFirestore
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
    now = _now_sp().isoformat()
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
    categoria_raw = str(it.get('categoria', 'outros')).strip()
    categoria = _canon_category(categoria_raw)
    desc_raw = str(it.get('descricao', '')).strip()
    desc_clean = clean_desc(desc_raw)
    desc_fixed = _apply_fixes(desc_clean)
    desc_final = _canon_desc(desc_fixed, tipo, categoria)
    it['descricao'] = desc_final
    it['valor'] = _norm_valor(it.get('valor', 0))
    if not categoria or categoria == 'outros':
        categoria = detect_category(desc_final or desc_fixed)
    else:
        if categoria not in {
            'alimentacao','transporte','moradia','saude','lazer','vestuario','servicos','salario','vendas','outros','duvida'
        }:
            try:
                categoria2 = detect_category(categoria)
                if categoria2:
                    categoria = categoria2
            except:
                pass
    it['categoria'] = categoria
    return it

def salvar_no_firestore(dados):
    db = get_db()
    ts = _now_sp().isoformat()
    dr = _day_key_sp()
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
    dr = _day_key_sp()
    mr = _month_key_sp()
    arr = dados if isinstance(dados, list) else [dados]
    out = []
    fallback_out = None
    batch = db.batch()
    for item in arr or []:
        base = normalize_item_for_store(item or {})
        tp_raw = str(base.get("tipo", "0")).strip().lower()
        if tp_raw in ("1", "receita", "entrada"):
            tp_txt = "entrada"
        elif tp_raw in ("0", "despesa", "saida"):
            tp_txt = "saida"
        elif tp_raw in ("ajuste",):
            tp_txt = "ajuste"
        elif tp_raw in ("estorno",):
            tp_txt = "estorno"
        else:
            tp_txt = "saida"
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
            "confidence_score": float(base.get("confidence_score", item.get("confidence_score", 0.0) or 0.0) or 0.0),
            "pendente_confirmacao": bool(base.get("pendente_confirmacao", item.get("pendente_confirmacao", False))),
        }
        day_ref = root.collection("transacoes").document(doc["data_referencia"])
        item_ref = day_ref.collection("items").document()
        doc["ref_id"] = build_ref_id(doc["data_referencia"], item_ref.id)
        batch.set(item_ref, doc)
        dref = root.collection("dias").document(doc["data_referencia"])
        mkey = doc["data_referencia"][:7]
        mref = root.collection("meses").document(mkey)
        try:
            ano, m = mkey.split("-")
            mref.set({"ano": int(ano), "mes": int(m)}, merge=True)
        except:
            mref.set({}, merge=True)
        try:
            ano_d, mes_d, dia_d = doc["data_referencia"].split("-")
            batch.set(dref, {"ano": int(ano_d), "mes": int(mes_d), "dia": int(dia_d), "data": doc["data_referencia"]}, merge=True)
        except:
            batch.set(dref, {"data": doc["data_referencia"]}, merge=True)
        inc_d = {"atualizado_em": firestore.SERVER_TIMESTAMP}
        inc_m = {"atualizado_em": firestore.SERVER_TIMESTAMP}
        cat = str(base.get("categoria", "outros"))
        if tp_txt == "entrada":
            inc_d.update({
                "quantidade_transacoes": firestore.Increment(1),
                "quantidade_transacoes_validas": firestore.Increment(1),
                "totais_dia.total_entrada": firestore.Increment(val),
                "totais_por_tipo.entrada": firestore.Increment(val),
                "totais_dia.saldo_dia": firestore.Increment(val),
            })
            inc_m.update({
                "quantidade_transacoes": firestore.Increment(1),
                "quantidade_transacoes_validas": firestore.Increment(1),
                "totais_mes.total_entrada": firestore.Increment(val),
                "totais_por_tipo.entrada": firestore.Increment(val),
                "totais_mes.saldo_mes": firestore.Increment(val),
            })
            inc_m.update({f"categorias.entrada.{cat}": firestore.Increment(val)})
            inc_d.update({f"categorias.entrada.{cat}": firestore.Increment(val)})
        elif tp_txt == "saida":
            inc_d.update({
                "quantidade_transacoes": firestore.Increment(1),
                "quantidade_transacoes_validas": firestore.Increment(1),
                "totais_dia.total_saida": firestore.Increment(val),
                "totais_por_tipo.saida": firestore.Increment(val),
                "totais_dia.saldo_dia": firestore.Increment(-val),
            })
            inc_m.update({
                "quantidade_transacoes": firestore.Increment(1),
                "quantidade_transacoes_validas": firestore.Increment(1),
                "totais_mes.total_saida": firestore.Increment(val),
                "totais_por_tipo.saida": firestore.Increment(val),
                "totais_mes.saldo_mes": firestore.Increment(-val),
            })
            inc_m.update({f"categorias.saida.{cat}": firestore.Increment(val)})
            inc_d.update({f"categorias.saida.{cat}": firestore.Increment(val)})
        elif tp_txt == "ajuste":
            inc_d.update({
                "quantidade_transacoes": firestore.Increment(1),
                "quantidade_transacoes_validas": firestore.Increment(1),
                "totais_dia.total_ajuste": firestore.Increment(val),
                "totais_por_tipo.ajuste": firestore.Increment(val),
                "totais_dia.saldo_dia": firestore.Increment(val),
            })
            inc_m.update({
                "quantidade_transacoes": firestore.Increment(1),
                "quantidade_transacoes_validas": firestore.Increment(1),
                "totais_mes.total_ajuste": firestore.Increment(val),
                "totais_por_tipo.ajuste": firestore.Increment(val),
                "totais_mes.saldo_mes": firestore.Increment(val),
            })
            inc_m.update({f"categorias.ajuste.{cat}": firestore.Increment(val)})
            inc_d.update({f"categorias.ajuste.{cat}": firestore.Increment(val)})
        elif tp_txt == "estorno":
            inc_d.update({
                "quantidade_transacoes": firestore.Increment(1),
                "quantidade_transacoes_validas": firestore.Increment(1),
                "totais_dia.total_estorno": firestore.Increment(abs(val)),
                "totais_por_tipo.estorno": firestore.Increment(abs(val)),
                "totais_dia.saldo_dia": firestore.Increment(abs(val)),
            })
            inc_m.update({
                "quantidade_transacoes": firestore.Increment(1),
                "quantidade_transacoes_validas": firestore.Increment(1),
                "totais_mes.total_estorno": firestore.Increment(abs(val)),
                "totais_por_tipo.estorno": firestore.Increment(abs(val)),
                "totais_mes.saldo_mes": firestore.Increment(abs(val)),
            })
            inc_m.update({f"categorias.estorno.{cat}": firestore.Increment(abs(val))})
            inc_d.update({f"categorias.estorno.{cat}": firestore.Increment(abs(val))})
        else:
            inc_d.update({
                "quantidade_transacoes": firestore.Increment(1),
                "quantidade_transacoes_validas": firestore.Increment(1),
                "totais_dia.total_ajuste": firestore.Increment(val),
                "totais_por_tipo.ajuste": firestore.Increment(val),
                "totais_dia.saldo_dia": firestore.Increment(val),
            })
            inc_m.update({
                "quantidade_transacoes": firestore.Increment(1),
                "quantidade_transacoes_validas": firestore.Increment(1),
                "totais_mes.total_ajuste": firestore.Increment(val),
                "totais_por_tipo.ajuste": firestore.Increment(val),
                "totais_mes.saldo_mes": firestore.Increment(val),
            })
            inc_m.update({f"categorias.ajuste.{cat}": firestore.Increment(val)})
            inc_d.update({f"categorias.ajuste.{cat}": firestore.Increment(val)})
        batch.set(dref, inc_d, merge=True)
        batch.set(mref, inc_m, merge=True)
        try:
            delta = 0.0
            if tp_txt == "entrada":
                delta = float(val or 0)
            elif tp_txt == "saida":
                delta = -float(val or 0)
            elif tp_txt == "ajuste":
                delta = float(val or 0)
            if abs(delta) > 0:
                batch.set(root, {"saldo_real": firestore.Increment(delta), "atualizado_em": firestore.SERVER_TIMESTAMP}, merge=True)
        except:
            pass
        out.append(doc)
    try:
        batch.commit()
    except Exception:
        fallback_out = []
        for item in arr or []:
            try:
                base = normalize_item_for_store(item or {})
                tp_raw = str(base.get("tipo", "0")).strip().lower()
                if tp_raw in ("1", "receita", "entrada"):
                    tp_txt = "entrada"
                elif tp_raw in ("0", "despesa", "saida"):
                    tp_txt = "saida"
                elif tp_raw in ("ajuste",):
                    tp_txt = "ajuste"
                elif tp_raw in ("estorno",):
                    tp_txt = "estorno"
                else:
                    tp_txt = "saida"
                if tipo_operacao in ("ajuste", "estorno"):
                    tp_txt = tipo_operacao
                val = float(base.get("valor", 0) or 0)
                ref_day = str(base.get("data_referencia") or dr)
                day_ref = root.collection("transacoes").document(ref_day)
                item_ref = day_ref.collection("items").document()
                doc = {
                    "valor": val,
                    "tipo": tp_txt,
                    "categoria": str(base.get("categoria", "outros")),
                    "descricao": str(base.get("descricao", "")),
                    "data_referencia": ref_day,
                    "timestamp_criacao": firestore.SERVER_TIMESTAMP,
                    "moeda": str(item.get("moeda", "BRL")),
                    "origem": origem,
                    "referencia_id": referencia_id,
                    "motivo_ajuste": motivo_ajuste,
                    "imutavel": True,
                    "confidence_score": float(base.get("confidence_score", item.get("confidence_score", 0.0) or 0.0) or 0.0),
                    "pendente_confirmacao": bool(base.get("pendente_confirmacao", item.get("pendente_confirmacao", False))),
                }
                doc["ref_id"] = build_ref_id(ref_day, item_ref.id)
                dref = root.collection("dias").document(ref_day)
                mref = root.collection("meses").document(ref_day[:7])
                item_ref.set(doc)
                inc_d = {"atualizado_em": firestore.SERVER_TIMESTAMP}
                inc_m = {"atualizado_em": firestore.SERVER_TIMESTAMP}
                cat = str(base.get("categoria", "outros"))
                if tp_txt == "entrada":
                    inc_d.update({
                        "quantidade_transacoes": firestore.Increment(1),
                        "quantidade_transacoes_validas": firestore.Increment(1),
                        "totais_dia.total_entrada": firestore.Increment(val),
                        "totais_por_tipo.entrada": firestore.Increment(val),
                        "totais_dia.saldo_dia": firestore.Increment(val)
                    })
                    inc_m.update({
                        "quantidade_transacoes": firestore.Increment(1),
                        "quantidade_transacoes_validas": firestore.Increment(1),
                        "totais_mes.total_entrada": firestore.Increment(val),
                        "totais_por_tipo.entrada": firestore.Increment(val),
                        "totais_mes.saldo_mes": firestore.Increment(val)
                    })
                    inc_m.update({f"categorias.entrada.{cat}": firestore.Increment(val)})
                    inc_d.update({f"categorias.entrada.{cat}": firestore.Increment(val)})
                elif tp_txt == "saida":
                    inc_d.update({
                        "quantidade_transacoes": firestore.Increment(1),
                        "quantidade_transacoes_validas": firestore.Increment(1),
                        "totais_dia.total_saida": firestore.Increment(val),
                        "totais_por_tipo.saida": firestore.Increment(val),
                        "totais_dia.saldo_dia": firestore.Increment(-val)
                    })
                    inc_m.update({
                        "quantidade_transacoes": firestore.Increment(1),
                        "quantidade_transacoes_validas": firestore.Increment(1),
                        "totais_mes.total_saida": firestore.Increment(val),
                        "totais_por_tipo.saida": firestore.Increment(val),
                        "totais_mes.saldo_mes": firestore.Increment(-val)
                    })
                    inc_m.update({f"categorias.saida.{cat}": firestore.Increment(val)})
                    inc_d.update({f"categorias.saida.{cat}": firestore.Increment(val)})
                elif tp_txt == "ajuste":
                    inc_d.update({
                        "quantidade_transacoes": firestore.Increment(1),
                        "quantidade_transacoes_validas": firestore.Increment(1),
                        "totais_dia.total_ajuste": firestore.Increment(val),
                        "totais_por_tipo.ajuste": firestore.Increment(val),
                        "totais_dia.saldo_dia": firestore.Increment(val)
                    })
                    inc_m.update({
                        "quantidade_transacoes": firestore.Increment(1),
                        "quantidade_transacoes_validas": firestore.Increment(1),
                        "totais_mes.total_ajuste": firestore.Increment(val),
                        "totais_por_tipo.ajuste": firestore.Increment(val),
                        "totais_mes.saldo_mes": firestore.Increment(val)
                    })
                    inc_m.update({f"categorias.ajuste.{cat}": firestore.Increment(val)})
                    inc_d.update({f"categorias.ajuste.{cat}": firestore.Increment(val)})
                elif tp_txt == "estorno":
                    inc_d.update({
                        "quantidade_transacoes": firestore.Increment(1),
                        "quantidade_transacoes_validas": firestore.Increment(1),
                        "totais_dia.total_estorno": firestore.Increment(abs(val)),
                        "totais_por_tipo.estorno": firestore.Increment(abs(val)),
                        "totais_dia.saldo_dia": firestore.Increment(abs(val)),
                    })
                    inc_m.update({
                        "quantidade_transacoes": firestore.Increment(1),
                        "quantidade_transacoes_validas": firestore.Increment(1),
                        "totais_mes.total_estorno": firestore.Increment(abs(val)),
                        "totais_por_tipo.estorno": firestore.Increment(abs(val)),
                        "totais_mes.saldo_mes": firestore.Increment(abs(val)),
                    })
                    inc_m.update({f"categorias.estorno.{cat}": firestore.Increment(abs(val))})
                    inc_d.update({f"categorias.estorno.{cat}": firestore.Increment(abs(val))})
                else:
                    inc_d.update({
                        "quantidade_transacoes": firestore.Increment(1),
                        "quantidade_transacoes_validas": firestore.Increment(1),
                        "totais_dia.total_ajuste": firestore.Increment(val),
                        "totais_por_tipo.ajuste": firestore.Increment(val),
                        "totais_dia.saldo_dia": firestore.Increment(val)
                    })
                    inc_m.update({
                        "quantidade_transacoes": firestore.Increment(1),
                        "quantidade_transacoes_validas": firestore.Increment(1),
                        "totais_mes.total_ajuste": firestore.Increment(val),
                        "totais_por_tipo.ajuste": firestore.Increment(val),
                        "totais_mes.saldo_mes": firestore.Increment(val)
                    })
                    inc_m.update({f"categorias.ajuste.{cat}": firestore.Increment(val)})
                    inc_d.update({f"categorias.ajuste.{cat}": firestore.Increment(val)})
                try:
                    ano_d, mes_d, dia_d = ref_day.split("-")
                    root.collection("dias").document(ref_day).set({"ano": int(ano_d), "mes": int(mes_d), "dia": int(dia_d), "data": ref_day}, merge=True)
                except:
                    root.collection("dias").document(ref_day).set({"data": ref_day}, merge=True)
                root.collection("dias").document(ref_day).set(inc_d, merge=True)
                mref.set(inc_m, merge=True)
                try:
                    delta = 0.0
                    if tp_txt == "entrada":
                        delta = float(val or 0)
                    elif tp_txt == "saida":
                        delta = -float(val or 0)
                    elif tp_txt == "ajuste":
                        delta = float(val or 0)
                    if abs(delta) > 0:
                        root.set({"saldo_real": firestore.Increment(delta), "atualizado_em": firestore.SERVER_TIMESTAMP}, merge=True)
                except:
                    pass
                fallback_out.append(doc)
            except Exception:
                pass
    if fallback_out is not None:
        return fallback_out
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
            "data_referencia": str(o.get("data_referencia") or _day_key_sp()),
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
    dr = str(o.get("data_referencia") or _day_key_sp())
    if tp not in ("entrada", "saida"):
        return None
    cat = str(o.get("categoria", "outros") or "outros").strip().lower()
    if not dr:
        try:
            dr = _day_key_sp()
        except Exception:
            dr = _day_key_sp()
    if not did:
        try:
            dr2 = str(o.get("data_referencia") or "")
        except Exception:
            dr2 = ""
        if dr2 and len(dr2) == 10:
            try:
                cdoc = root.collection("transacoes").document(dr2).collection("items").document(str(referencia_id)).get()
                if cdoc.exists:
                    co = cdoc.to_dict() or {}
                    ccat = str(co.get("categoria", "outros") or "outros").strip().lower()
                    if cat in ("", "outros") and ccat and ccat not in ("outros",):
                        cat = ccat
            except Exception:
                pass
    payload_cat = cat
    payload = {
        "valor": abs(val),
        "tipo": "estorno",
        "categoria": payload_cat,
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
    cat = payload_cat
    inc_d = {"atualizado_em": firestore.SERVER_TIMESTAMP}
    inc_m = {"atualizado_em": firestore.SERVER_TIMESTAMP}
    abs_val = abs(val)
    try:
        ano_d, mes_d, dia_d = dr.split("-")
        batch.set(dref, {"ano": int(ano_d), "mes": int(mes_d), "dia": int(dia_d), "data": dr}, merge=True)
    except:
        batch.set(dref, {"data": dr}, merge=True)
    inc_d.update({
        "quantidade_transacoes": firestore.Increment(1),
        "quantidade_transacoes_validas": firestore.Increment(1),
        "totais_dia.total_estorno": firestore.Increment(abs_val),
        "totais_por_tipo.estorno": firestore.Increment(abs_val),
        "totais_dia.saldo_dia": firestore.Increment(abs_val),
        f"categorias.estorno.{cat}": firestore.Increment(abs_val),
    })
    inc_m.update({
        "quantidade_transacoes": firestore.Increment(1),
        "quantidade_transacoes_validas": firestore.Increment(1),
        "totais_mes.total_estorno": firestore.Increment(abs_val),
        "totais_por_tipo.estorno": firestore.Increment(abs_val),
        "totais_mes.saldo_mes": firestore.Increment(abs_val),
        f"categorias.estorno.{cat}": firestore.Increment(abs_val),
    })
    batch.set(dref, inc_d, merge=True)
    batch.set(mref, inc_m, merge=True)
    try:
        delta = 0.0
        if tp == "entrada":
            delta = -float(val or 0)
        else:
            delta = float(val or 0)
        if abs(delta) > 0:
            batch.set(root, {"saldo_real": firestore.Increment(delta), "atualizado_em": firestore.SERVER_TIMESTAMP}, merge=True)
    except:
        pass
    batch.commit()
    return payload
def atualizar_categoria_transacao(cliente_id: str, referencia_id: str, nova_categoria: str, nova_descricao: str = None):
    db = get_db()
    root = _cliente_root(cliente_id)
    dr, did = parse_ref_id(referencia_id)
    if dr:
        tdoc_ref = root.collection("transacoes").document(dr).collection("items").document(did)
        tdoc = tdoc_ref.get()
    else:
        tdoc_ref = root.collection("transacoes").document(str(referencia_id))
        tdoc = tdoc_ref.get()
    if not tdoc.exists:
        return None
    o = tdoc.to_dict() or {}
    if bool(o.get("estornado", False)):
        return None
    val = float(o.get("valor", 0) or 0)
    old_cat = str(o.get("categoria", "outros") or "outros")
    tp_raw = str(o.get("tipo", "saida") or "saida").strip().lower()
    dr = str(o.get("data_referencia") or _day_key_sp())
    mk = dr[:7]
    novo_cat = str(nova_categoria or old_cat).strip().lower()
    if novo_cat == old_cat:
        try:
            tdoc_ref.set({
                "pendente_confirmacao": False,
                "confidence_score": 0.95,
                "atualizado_em": firestore.SERVER_TIMESTAMP,
            }, merge=True)
        except:
            pass
        return {
            "ref_id": str(o.get("ref_id") or referencia_id),
            "data_referencia": dr,
            "valor": val,
            "tipo": tp_raw,
            "categoria_anterior": old_cat,
            "categoria_nova": novo_cat,
        }
    batch = db.batch()
    upd = {
        "categoria": novo_cat,
        "pendente_confirmacao": False,
        "confidence_score": 0.95,
        "atualizado_em": firestore.SERVER_TIMESTAMP,
    }
    if isinstance(nova_descricao, str) and nova_descricao.strip():
        try:
            from app.services.rule_based import naturalize_description, clean_desc
            desc_raw = str(nova_descricao or "")
            desc_nat = naturalize_description(str(o.get("tipo") or ""), novo_cat, clean_desc(desc_raw))
            upd["descricao"] = desc_nat
        except:
            upd["descricao"] = str(nova_descricao)
    batch.update(tdoc_ref, upd)
    try:
        if dr and did:
            try:
                for it in root.collection("transacoes").document(dr).collection("items").where("ref_id", "==", str(o.get("ref_id") or referencia_id)).stream():
                    batch.update(it.reference, upd)
            except:
                pass
            try:
                for t in root.collection("transacoes").where("ref_id", "==", str(o.get("ref_id") or referencia_id)).stream():
                    batch.update(t.reference, upd)
            except:
                pass
        else:
            dr2 = str(o.get("data_referencia") or "")
            if dr2 and len(dr2) == 10:
                it_ref = root.collection("transacoes").document(dr2).collection("items").document(str(referencia_id))
                if it_ref.get().exists:
                    batch.update(it_ref, upd)
            try:
                for t in root.collection("transacoes").where("ref_id", "==", str(referencia_id)).stream():
                    batch.update(t.reference, upd)
            except:
                pass
    except Exception:
        pass
    dref = root.collection("dias").document(dr)
    mref = root.collection("meses").document(mk)
    inc_d = {"atualizado_em": firestore.SERVER_TIMESTAMP}
    inc_m = {"atualizado_em": firestore.SERVER_TIMESTAMP}
    if tp_raw in ("entrada", "1", "receita"):
        inc_d.update({
            f"categorias.entrada.{old_cat}": firestore.Increment(-val),
            f"categorias.entrada.{novo_cat}": firestore.Increment(val),
        })
        inc_m.update({
            f"categorias.entrada.{old_cat}": firestore.Increment(-val),
            f"categorias.entrada.{novo_cat}": firestore.Increment(val),
        })
    elif tp_raw in ("saida", "0", "despesa"):
        inc_d.update({
            f"categorias.saida.{old_cat}": firestore.Increment(-val),
            f"categorias.saida.{novo_cat}": firestore.Increment(val),
        })
        inc_m.update({
            f"categorias.saida.{old_cat}": firestore.Increment(-val),
            f"categorias.saida.{novo_cat}": firestore.Increment(val),
        })
    batch.set(dref, inc_d, merge=True)
    batch.set(mref, inc_m, merge=True)
    try:
        batch.set(root, {"atualizado_em": firestore.SERVER_TIMESTAMP}, merge=True)
    except:
        pass
    batch.commit()
    return {
        "ref_id": str(o.get("ref_id") or referencia_id),
        "data_referencia": dr,
        "valor": val,
        "tipo": tp_raw,
        "categoria_anterior": old_cat,
        "categoria_nova": novo_cat,
    }
def get_categoria_memoria(cliente_id: str):
    try:
        db = get_db()
        root = _cliente_root(cliente_id)
        doc = root.collection("memoria").document("categorias").get()
        o = doc.to_dict() or {}
        return dict(o.get("desc_map", {}) or {})
    except:
        return {}
def atualizar_memoria_categoria(cliente_id: str, key: str, categoria: str):
    try:
        db = get_db()
        root = _cliente_root(cliente_id)
        payload = {
            "desc_map": {str(key or ""): str(categoria or "outros")},
            "atualizado_em": firestore.SERVER_TIMESTAMP,
        }
        root.collection("memoria").document("categorias").set(payload, merge=True)
        return True
    except:
        return False
def backup_firestore_data(output_path=None):
    db = get_db()
    ts = _now_sp().strftime("%Y%m%dT%H%M%S")
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
    day_keys = []
    try:
        for d in list(root.collection("transacoes").stream()):
            di = str(d.id or "").strip()
            if re.fullmatch(r"\d{4}-\d{2}-\d{2}", di or ""):
                day_keys.append(di)
    except Exception:
        pass
    try:
        for dd in list(root.collection("dias").stream()):
            di = str(dd.id or "").strip()
            if re.fullmatch(r"\d{4}-\d{2}-\d{2}", di or ""):
                day_keys.append(di)
    except Exception:
        pass
    try:
        day_keys = sorted(list(set(day_keys)))
    except Exception:
        day_keys = list(set(day_keys))
    for dr in day_keys:
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
        idx = {}
        items = []
        tops = []
        try:
            items = list(root.collection("transacoes").document(dr).collection("items").stream())
        except Exception:
            items = []
        try:
            tops = list(root.collection("transacoes").where("data_referencia", "==", dr).stream())
        except Exception:
            tops = []
        def _sig(o):
            try:
                return str(o.get("ref_id") or "") or (str(o.get("tipo", "")) + "|" + str(float(o.get("valor", 0) or 0)) + "|" + str(o.get("categoria", "")) + "|" + str(o.get("descricao", "")) + "|" + str(o.get("timestamp_criacao", "")))
            except Exception:
                return str(o.get("ref_id") or "")
        for it in items:
            o = it.to_dict() or {}
            k = _sig(o)
            if idx.get(k):
                continue
            idx[k] = 1
            tp_raw = str(o.get("tipo", "")).strip().lower()
            cat = str(o.get("categoria", "outros") or "outros").strip().lower()
            val = float(o.get("valor", 0) or 0)
            if tp_raw in ("estorno",):
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
                m["categorias_estorno"][cat] = float(m["categorias_estorno"].get(cat, 0.0) or 0.0) + abs(val)
                day_cat_est[cat] = float(day_cat_est.get(cat, 0.0) or 0.0) + abs(val)
            elif bool(o.get("estornado", False)):
                pass
            elif tp_raw in ("1", "receita", "entrada"):
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
        for it in tops:
            o = it.to_dict() or {}
            k = _sig(o)
            if idx.get(k):
                continue
            idx[k] = 1
            tp_raw = str(o.get("tipo", "")).strip().lower()
            cat = str(o.get("categoria", "outros") or "outros").strip().lower()
            val = float(o.get("valor", 0) or 0)
            if tp_raw in ("estorno",):
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
                m["categorias_estorno"][cat] = float(m["categorias_estorno"].get(cat, 0.0) or 0.0) + abs(val)
                day_cat_est[cat] = float(day_cat_est.get(cat, 0.0) or 0.0) + abs(val)
            elif bool(o.get("estornado", False)):
                pass
            elif tp_raw in ("1", "receita", "entrada"):
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
        saldo_dia = total_entrada - total_saida + total_ajuste
        try:
            try:
                ano_d, mes_d, dia_d = dr.split("-")
            except:
                ano_d = None
                mes_d = None
                dia_d = None
            root.collection("dias").document(dr).set({
                "ano": (int(ano_d) if ano_d is not None else None),
                "mes": (int(mes_d) if mes_d is not None else None),
                "dia": (int(dia_d) if dia_d is not None else None),
                "data": dr,
                "quantidade_transacoes": qtd,
                "quantidade_transacoes_validas": qtd_validas,
                "categorias": {
                    "entrada": dict({k: float(v or 0.0) for k, v in day_cat_inc.items()}),
                    "saida": dict({k: float(v or 0.0) for k, v in day_cat_exp.items()}),
                    "estorno": dict({k: float(v or 0.0) for k, v in day_cat_est.items()}),
                    "ajuste": dict({k: float(v or 0.0) for k, v in day_cat_adj.items()}),
                },
                "totais_por_tipo": {
                    "entrada": float(sum(day_cat_inc.values()) or total_entrada),
                    "saida": float(sum(day_cat_exp.values()) or total_saida),
                    "estorno": float(sum(day_cat_est.values()) or total_estorno),
                    "ajuste": float(sum(day_cat_adj.values()) or total_ajuste),
                },
                "totais_dia": {
                    "total_entrada": total_entrada,
                    "total_saida": total_saida,
                    "total_estorno": total_estorno,
                    "total_ajuste": total_ajuste,
                    "saldo_dia": float(total_entrada - total_saida + total_estorno + total_ajuste),
                },
                "atualizado_em": firestore.SERVER_TIMESTAMP,
            }, merge=True)
        except Exception:
            pass
        days_processed += 1
    months_processed = 0
    for mk, m in month_agg.items():
        tot_entrada = float(m.get("total_entrada", 0.0) or 0.0)
        tot_saida = float(m.get("total_saida", 0.0) or 0.0)
        tot_ajuste = float(m.get("total_ajuste", 0.0) or 0.0)
        tot_estorno = float(m.get("total_estorno", 0.0) or 0.0)
        saldo_mes = tot_entrada - tot_saida + tot_estorno + tot_ajuste
        try:
            ano_i = int(mk.split("-")[0])
            mes_i = int(mk.split("-")[1])
        except:
            ano_i = None
            mes_i = None
        try:
            doc = root.collection("meses").document(mk)
            payload = {
                "ano": ano_i,
                "mes": mes_i,
                "quantidade_transacoes": int(m.get("quantidade_transacoes", 0) or 0),
                "quantidade_transacoes_validas": int(m.get("quantidade_transacoes_validas", 0) or 0),
                "categorias": {
                    "entrada": dict(m.get("categorias_entrada", {}) or {}),
                    "saida": dict(m.get("categorias_saida", {}) or {}),
                    "estorno": dict(m.get("categorias_estorno", {}) or {}),
                    "ajuste": dict(m.get("categorias_ajuste", {}) or {}),
                },
                "totais_por_tipo": {
                    "entrada": float(sum((m.get("categorias_entrada", {}) or {}).values()) or tot_entrada),
                    "saida": float(sum((m.get("categorias_saida", {}) or {}).values()) or tot_saida),
                    "estorno": float(sum((m.get("categorias_estorno", {}) or {}).values()) or tot_estorno),
                    "ajuste": float(sum((m.get("categorias_ajuste", {}) or {}).values()) or tot_ajuste),
                },
                "totais_mes": {
                    "total_entrada": tot_entrada,
                    "total_saida": tot_saida,
                    "total_estorno": tot_estorno,
                    "total_ajuste": tot_ajuste,
                    "saldo_mes": float(saldo_mes or 0.0),
                },
                "atualizado_em": firestore.SERVER_TIMESTAMP,
            }
            doc.set(payload, merge=True)
        except Exception:
            pass
        months_processed += 1
    return {
        "cliente_id": str(cliente_id),
        "dias_processados": days_processed,
        "meses_processados": months_processed,
    }
def purge_cliente_aggregates(cliente_id: str, purge_days: bool = True, purge_months: bool = True):
    db = get_db()
    root = _cliente_root(cliente_id)
    deleted_days = 0
    deleted_months = 0
    try:
        if purge_days:
            try:
                days = list(root.collection("dias").stream())
            except Exception:
                days = []
            for d in days:
                try:
                    root.collection("dias").document(d.id).delete()
                    deleted_days += 1
                except Exception:
                    pass
    except Exception:
        pass
    try:
        if purge_months:
            try:
                months = list(root.collection("meses").stream())
            except Exception:
                months = []
            for m in months:
                try:
                    root.collection("meses").document(m.id).delete()
                    deleted_months += 1
                except Exception:
                    pass
    except Exception:
        pass
    return {
        "cliente_id": str(cliente_id),
        "dias_deletados": deleted_days,
        "meses_deletados": deleted_months,
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
