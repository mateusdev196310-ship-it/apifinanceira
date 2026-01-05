# arquivo: api_financeira.py
from flask import Flask, request, jsonify
import os
from datetime import datetime, timedelta, timezone
from flask_cors import CORS
from app.services.extractor import extrair_informacoes_financeiras
from app.services.rule_based import parse_text_to_transactions, clean_desc, detect_category, naturalize_description, natural_score
from audio_processor import audio_processor
from app.services.database import get_db, salvar_transacao_cliente, firestore, ensure_cliente, build_ref_id, parse_ref_id, recompute_cliente_aggregates
from app.services.pdf_extractor import extrair_transacoes_de_pdf, extrair_totais_a_pagar_de_pdf

app = Flask(__name__)
CORS(app)  # Permitir requisições do bot

from app.config import API_HOST, API_PORT
import re as _re

SOURCE_STATS = {
    "local-regra": 0,
    "gemini": 0,
    "json-transacoes": 0,
    "nenhum": 0,
    "audio-local": 0,
}
 

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

def _coerce_val(o):
    try:
        for k in ("valor_total", "valor", "pagamento", "valor_previsto"):
            v = o.get(k, None)
            if isinstance(v, (int, float)):
                return float(v or 0)
            if v is not None:
                try:
                    return float(v)
                except:
                    pass
    except:
        pass
    return 0.0
def _coerce_date_str_from_val(v):
    try:
        if hasattr(v, "strftime"):
            return v.strftime("%Y-%m-%d")
        s = str(v or "").strip()
        if not s:
            return ""
        if len(s) >= 10 and s[4] == "-" and s[7] == "-":
            return s[:10]
        m = _re.match(r'^(\d{2})[./-](\d{2})[./-](\d{4})$', s)
        if m:
            d, m_, y = m.groups()
            return f"{y}-{m_}-{d}"
    except:
        pass
    return ""
def _coerce_date_str(o):
    if not isinstance(o, dict):
        return ""
    for k in ("vencimento_iso", "vencimento", "vencimento_ts"):
        if k in o:
            return _coerce_date_str_from_val(o.get(k))
    return ""

def _normalize_date_br(s: str):
    t = str(s or "").strip()
    m = _re.match(r'^(\d{2})[./-](\d{2})[./-](\d{4})$', t)
    if m:
        d, m_, y = m.groups()
        return f"{y}-{m_}-{d}"
    m = _re.match(r'^(\d{4})-(\d{2})-(\d{2})$', t)
    if m:
        return t
    return None
 
 

# ===== ENDPOINTS DA API =====
@app.route('/processar', methods=['POST'])
def processar():
    """Processa uma mensagem do usuário."""
    data = request.json
    if not data:
        return jsonify({"sucesso": False, "erro": "Payload não fornecido"}), 400
    if isinstance(data.get('transacoes'), list) and data.get('transacoes'):
        texto_original = data.get('texto_original', 'imagem')
        transacoes_in = data.get('transacoes', [])
        def _norm_dedup(items, texto_ctx=None):
            d = {}
            for item in items:
                tipo_n = str(item.get('tipo')).strip()
                valor_n = float(item.get('valor', 0))
                desc_raw = str(item.get('descricao', ''))
                desc_n = clean_desc(desc_raw)
                cat_n = str(item.get('categoria', '')).strip().lower()
                if not cat_n or cat_n == 'outros':
                    try:
                        cat_n = detect_category(desc_n or (texto_ctx or ''))
                    except:
                        pass
                desc_final = naturalize_description(tipo_n, cat_n, desc_n)
                k = (tipo_n, valor_n, cat_n)
                cur = d.get(k)
                if cur is None or natural_score(desc_final) > natural_score(str(cur.get('descricao', ''))) or (natural_score(desc_final) == natural_score(str(cur.get('descricao', ''))) and len(desc_final) <= len(str(cur.get('descricao', '')))):
                    novo = dict(item)
                    novo['descricao'] = desc_final
                    novo['categoria'] = cat_n
                    d[k] = novo
            return list(d.values())
        transacoes = _norm_dedup(transacoes_in, texto_original)
        if not transacoes:
            return jsonify({"sucesso": False, "erro": "Nenhuma transação válida"}), 200
        try:
            cliente_id = str(data.get("cliente_id") or "default")
            origem = str(data.get("origem") or "api")
            try:
                ensure_cliente(cliente_id, nome=str(data.get("cliente_nome") or None), username=str(data.get("username") or None))
            except:
                pass
            erro_salvar = None
            salvas = []
            try:
                salvas = salvar_transacao_cliente(transacoes, cliente_id=cliente_id, origem=origem)
            except Exception:
                erro_salvar = "Falha ao salvar no Firestore"
        except Exception as e:
            erro_salvar = "Falha ao salvar no Firestore"
        try:
            SOURCE_STATS["json-transacoes"] = SOURCE_STATS.get("json-transacoes", 0) + 1
            total_stats = sum(SOURCE_STATS.values())
            print(f"[processar] fonte=json-transacoes qtd={len(transacoes)} stats={SOURCE_STATS} total={total_stats}")
        except:
            pass
        return jsonify({
            "sucesso": True,
            "transacoes": transacoes,
            "total": len(transacoes),
            "arquivo": None,
            "processado_em": _now_sp().isoformat(),
            "debug": {
                "source": "json-transacoes",
                "version": "image-doc-v1"
            },
            "erro_salvar": erro_salvar,
            "salvas": [
                {
                    "ref_id": x.get("ref_id"),
                    "valor": float(x.get("valor", 0) or 0),
                    "tipo": str(x.get("tipo", "")),
                    "categoria": str(x.get("categoria", "")),
                    "descricao": str(x.get("descricao", "")),
                    "data_referencia": str(x.get("data_referencia", "")),
                }
                for x in (salvas or [])
            ]
        })
    if 'mensagem' not in data:
        return jsonify({"sucesso": False, "erro": "Mensagem não fornecida"}), 400
    
    texto = data['mensagem']
    rb = parse_text_to_transactions(texto) or []
    base = []
    orig_count = 0
    if not rb:
        base = extrair_informacoes_financeiras(texto) or []
    def _norm_dedup(items):
        d = {}
        for item in items:
            tipo_n = str(item.get('tipo')).strip()
            valor_n = float(item.get('valor', 0))
            desc_raw = str(item.get('descricao', ''))
            desc_n = clean_desc(desc_raw)
            cat_n = str(item.get('categoria', '')).strip().lower()
            if not cat_n or cat_n == 'outros':
                try:
                    cat_n = detect_category(desc_n or texto)
                except:
                    pass
            desc_final = naturalize_description(tipo_n, cat_n, desc_n)
            k = (tipo_n, valor_n, cat_n)
            cur = d.get(k)
            if cur is None or natural_score(desc_final) > natural_score(str(cur.get('descricao', ''))) or (natural_score(desc_final) == natural_score(str(cur.get('descricao', ''))) and len(desc_final) <= len(str(cur.get('descricao', '')))):
                novo = dict(item)
                novo['descricao'] = desc_final
                novo['categoria'] = cat_n
                d[k] = novo
        return list(d.values())
    ai_dedup = _norm_dedup(base) if base else []
    rb_dedup = _norm_dedup(rb) if rb else []
    transacoes = ai_dedup if ai_dedup else rb_dedup
    try:
        src = "gemini" if ai_dedup else ("local-regra" if rb_dedup else "nenhum")
        SOURCE_STATS[src] = SOURCE_STATS.get(src, 0) + 1
        total_stats = sum(SOURCE_STATS.values())
        print(f"[processar] fonte={src} qtd={len(transacoes)} stats={SOURCE_STATS} total={total_stats}")
    except:
        pass
    try:
        print(f"[processar] ai_dedup={ai_dedup} rb_dedup={rb_dedup} escolhidas={transacoes}")
    except:
        pass
    try:
        print(f"[processar] texto='{texto}' -> transacoes={transacoes}")
    except:
        pass
    
    if not transacoes:
        return jsonify({"sucesso": False, "erro": "Nenhuma transação encontrada"}), 200
    
    erro_salvar = None
    try:
        cliente_id = str(data.get("cliente_id") or "default")
        origem = str(data.get("origem") or "api")
        try:
            ensure_cliente(cliente_id, nome=str(data.get("cliente_nome") or None), username=str(data.get("username") or None))
        except:
            pass
        try:
            salvas = salvar_transacao_cliente(transacoes, cliente_id=cliente_id, origem=origem)
        except Exception:
            erro_salvar = "Falha ao salvar no Firestore"
    except Exception:
        erro_salvar = "Falha ao salvar no Firestore"
    normalized = []
    for item in transacoes:
        tipo_n = str(item.get('tipo')).strip()
        valor_n = float(item.get('valor', 0))
        desc_raw = str(item.get('descricao', ''))
        desc_n = clean_desc(desc_raw)
        cat_n = str(item.get('categoria', '')).strip().lower()
        if not cat_n or cat_n == 'outros':
            try:
                cat_n = detect_category(desc_n or texto)
            except:
                pass
        novo = dict(item)
        novo['descricao'] = naturalize_description(tipo_n, cat_n, desc_n)
        novo['categoria'] = cat_n
        normalized.append(novo)
    transacoes = _norm_dedup(normalized)
    try:
        print(f"[processar] normalized={normalized} final={transacoes}")
    except:
        pass
    
    return jsonify({
        "sucesso": True,
        "transacoes": transacoes,
        "total": len(transacoes),
        "arquivo": None,
        "processado_em": _now_sp().isoformat(),
        "debug": {
            "ai_count": orig_count,
            "rb_count": len(rb or []),
            "version": "ai+rule-v1"
        },
        "erro_salvar": erro_salvar,
        "salvas": [
            {
                "ref_id": x.get("ref_id"),
                "valor": float(x.get("valor", 0) or 0),
                "tipo": str(x.get("tipo", "")),
                "categoria": str(x.get("categoria", "")),
                "descricao": str(x.get("descricao", "")),
                "data_referencia": str(x.get("data_referencia", "")),
            }
            for x in (salvas or [])
        ]
    })

@app.route('/processar_audio', methods=['POST'])
def processar_audio():
    data = request.json
    if not data:
        return jsonify({"sucesso": False, "erro": "Payload não fornecido"}), 400
    arquivo = data.get('arquivo')
    if not arquivo or not os.path.exists(arquivo):
        return jsonify({"sucesso": False, "erro": "Arquivo não encontrado"}), 400
    try:
        with open(arquivo, 'rb') as f:
            b = f.read()
        fmt = 'ogg' if arquivo.lower().endswith('.ogg') else ('mp3' if arquivo.lower().endswith('.mp3') or arquivo.lower().endswith('.mpeg') else 'wav')
        texto = audio_processor.transcribe_audio_file(b, format=fmt)
    except Exception as e:
        return jsonify({"sucesso": False, "erro": str(e)}), 500
    if not texto:
        return jsonify({"sucesso": False, "erro": "Transcrição vazia"}), 200
    rb = parse_text_to_transactions(texto) or []
    if not rb:
        return jsonify({"sucesso": False, "erro": "Nenhuma transação encontrada"}), 200
    def _norm_dedup(items):
        d = {}
        for item in items:
            tipo_n = str(item.get('tipo')).strip()
            valor_n = float(item.get('valor', 0))
            desc_raw = str(item.get('descricao', ''))
            desc_n = clean_desc(desc_raw)
            cat_n = str(item.get('categoria', '')).strip().lower()
            if not cat_n or cat_n == 'outros':
                try:
                    cat_n = detect_category(desc_n or texto)
                except:
                    pass
            k = (tipo_n, valor_n, cat_n)
            cur = d.get(k)
            desc_final = naturalize_description(tipo_n, cat_n, desc_n)
            if cur is None or natural_score(desc_final) > natural_score(str(cur.get('descricao', ''))) or (natural_score(desc_final) == natural_score(str(cur.get('descricao', ''))) and len(desc_final) <= len(str(cur.get('descricao', '')))):
                novo = dict(item)
                novo['descricao'] = desc_final
                novo['categoria'] = cat_n
                d[k] = novo
        return list(d.values())
    transacoes = _norm_dedup(rb)
    try:
        SOURCE_STATS["audio-local"] = SOURCE_STATS.get("audio-local", 0) + 1
        total_stats = sum(SOURCE_STATS.values())
        print(f"[processar_audio] fonte=local-regra qtd={len(transacoes)} stats={SOURCE_STATS} total={total_stats}")
    except:
        pass
    erro_salvar = None
    try:
        cliente_id = str(data.get("cliente_id") or "default")
        origem = f"audio-file:{os.path.basename(arquivo)}"
        try:
            ensure_cliente(cliente_id, nome=str(data.get("cliente_nome") or None), username=str(data.get("username") or None))
        except:
            pass
        try:
            salvar_transacao_cliente(transacoes, cliente_id=cliente_id, origem=origem)
        except Exception:
            erro_salvar = "Falha ao salvar no Firestore"
    except Exception:
        erro_salvar = "Falha ao salvar no Firestore"
    return jsonify({"sucesso": True,
        "transacoes": transacoes,
        "total": len(transacoes),
        "arquivo": None,
        "processado_em": _now_sp().isoformat(),
        "debug": {
            "source": "audio-file",
            "texto": texto[:120]
        },
        "erro_salvar": erro_salvar
    })
@app.route('/processar_pdf', methods=['POST'])
def processar_pdf():
    data = request.json
    if not data:
        return jsonify({"sucesso": False, "erro": "Payload não fornecido"}), 400
    arquivo = data.get('arquivo')
    if not arquivo or not os.path.exists(arquivo):
        return jsonify({"sucesso": False, "erro": "Arquivo não encontrado"}), 400
    try:
        transacoes = extrair_transacoes_de_pdf(path=arquivo) or []
    except Exception as e:
        return jsonify({"sucesso": False, "erro": str(e)}), 500
    if not transacoes:
        return jsonify({"sucesso": False, "erro": "Nenhuma transação encontrada"}), 200
    erro_salvar = None
    try:
        cliente_id = str(data.get("cliente_id") or "default")
        origem = f"pdf-file:{os.path.basename(arquivo)}"
        try:
            ensure_cliente(cliente_id, nome=str(data.get("cliente_nome") or None), username=str(data.get("username") or None))
        except:
            pass
        salvar_transacao_cliente(transacoes, cliente_id=cliente_id, origem=origem)
    except Exception as e:
        erro_salvar = "Falha ao salvar no Firestore"
    return jsonify({
        "sucesso": True,
        "transacoes": transacoes,
        "total": len(transacoes),
        "arquivo": arquivo,
        "processado_em": _now_sp().isoformat(),
        "debug": {
            "source": "pdf-file"
        },
        "erro_salvar": erro_salvar
    })
@app.route('/processar_pdf_totais', methods=['POST'])
def processar_pdf_totais():
    data = request.json
    if not data:
        return jsonify({"sucesso": False, "erro": "Payload não fornecido"}), 400
    arquivo = data.get('arquivo')
    if not arquivo or not os.path.exists(arquivo):
        return jsonify({"sucesso": False, "erro": "Arquivo não encontrado"}), 400
    try:
        tot = extrair_totais_a_pagar_de_pdf(path=arquivo) or {}
    except Exception as e:
        return jsonify({"sucesso": False, "erro": str(e)}), 500
    total_val = tot.get("total_a_pagar")
    pagamento_val = tot.get("pagamento")
    if total_val is None and pagamento_val is None:
        return jsonify({"sucesso": False, "erro": "Nenhum total válido encontrado"}), 200
    transacoes = []
    inst = tot.get("instituicao")
    brand = tot.get("bandeira")
    doc_tipo = tot.get("doc_tipo")
    def _desc_total():
        base = "Fatura"
        if doc_tipo == "cartao":
            base += " de cartão"
        if inst:
            base += f" {inst}"
        elif brand:
            base += f" {brand}"
        return base
    def _desc_pagamento():
        base = "Pagamento da fatura"
        if doc_tipo == "cartao":
            base += " de cartão"
        if inst:
            base += f" {inst}"
        elif brand:
            base += f" {brand}"
        return base
    if isinstance(total_val, (int, float)):
        transacoes.append({
            "tipo": "0",
            "valor": float(total_val),
            "categoria": "fatura",
            "descricao": _desc_total(),
            "moeda": "BRL",
        })
    if isinstance(pagamento_val, (int, float)):
        transacoes.append({
            "tipo": "0",
            "valor": float(pagamento_val),
            "categoria": "pagamento-fatura",
            "descricao": _desc_pagamento(),
            "moeda": "BRL",
        })
    erro_salvar = None
    try:
        cliente_id = str(data.get("cliente_id") or "default")
        origem = f"pdf-totais:{os.path.basename(arquivo)}"
        try:
            ensure_cliente(cliente_id, nome=str(data.get("cliente_nome") or None), username=str(data.get("username") or None))
        except:
            pass
        salvas = salvar_transacao_cliente(transacoes, cliente_id=cliente_id, origem=origem)
        try:
            venc_raw = tot.get("vencimento")
            venc_iso = _normalize_date_br(venc_raw) if venc_raw else None
            if venc_iso:
                db = get_db()
                root = db.collection('clientes').document(cliente_id)
                st = "vencido"
                try:
                    hoje = _day_key_sp()
                    st = "a_vencer" if venc_iso >= hoje else "vencido"
                except:
                    st = "a_vencer"
                doc = {
                    "valor_total": float(total_val) if isinstance(total_val, (int, float)) else None,
                    "pagamento": float(pagamento_val) if isinstance(pagamento_val, (int, float)) else None,
                    "vencimento_iso": venc_iso,
                    "vencimento_raw": venc_raw,
                    "doc_tipo": doc_tipo,
                    "instituicao": inst or brand,
                    "descricao": _desc_total(),
                    "categoria": "fatura",
                    "status": st,
                    "mes": venc_iso[:7],
                    "timestamp_criacao": firestore.SERVER_TIMESTAMP,
                    "origem": origem,
                }
                try:
                    root.collection('compromissos').add(doc)
                except:
                    pass
        except:
            pass
    except Exception as e:
        erro_salvar = "Falha ao salvar no Firestore"
    return jsonify({
        "sucesso": True,
        "totais": tot,
        "transacoes": transacoes,
        "total": len(transacoes),
        "arquivo": arquivo,
        "processado_em": _now_sp().isoformat(),
        "debug": {
            "source": "pdf-totais"
        },
        "erro_salvar": erro_salvar,
        "salvas": [
            {
                "ref_id": x.get("ref_id"),
                "valor": float(x.get("valor", 0) or 0),
                "tipo": str(x.get("tipo", "")),
                "categoria": str(x.get("categoria", "")),
                "descricao": str(x.get("descricao", "")),
                "data_referencia": str(x.get("data_referencia", "")),
            }
            for x in (salvas or [])
        ]
    })
@app.route('/compromissos/mes', methods=['GET'])
def compromissos_mes():
    mes_qs = request.args.get("mes")
    mes_atual = mes_qs or _month_key_sp()
    tipo = str(request.args.get("tipo") or "").strip().lower() or None
    try:
        db = get_db()
        cliente_id = str(request.args.get("cliente_id") or "default")
        cliente_nome = request.args.get("cliente_nome")
        cliente_username = request.args.get("username")
        try:
            ensure_cliente(cliente_id, nome=cliente_nome, username=cliente_username)
        except:
            pass
        root = db.collection('clientes').document(cliente_id)
        ano, m = mes_atual.split("-")
        dt_ini = f"{ano}-{m}-01"
        if m == "12":
            dt_fim = f"{int(ano)+1}-01-01"
        else:
            dt_fim = f"{ano}-{int(m)+1:02d}-01"
        vencidos = []
        a_vencer = []
        total_vencidos = 0.0
        total_a_vencer = 0.0
        try:
            itens = list(root.collection('compromissos').where('mes', '==', mes_atual).stream())
        except:
            itens = []
        if not itens:
            try:
                q = root.collection('compromissos').where('vencimento_iso', '>=', dt_ini).where('vencimento_iso', '<', dt_fim)
                itens = list(q.stream())
            except:
                itens = []
        hoje = _day_key_sp()
        for it in itens:
            o = it.to_dict() or {}
            v = _coerce_val(o)
            ven = _coerce_date_str(o)
            if ven and ven < hoje:
                total_vencidos += v
                vencidos.append({
                    "codigo": it.id,
                    "descricao": str(o.get("descricao", "")),
                    "valor": v,
                    "vencimento": ven,
                    "instituicao": str(o.get("instituicao", "") or ""),
                    "status": "st"
                })
            else:
                total_a_vencer += v
                a_vencer.append({
                    "codigo": it.id,
                    "descricao": str(o.get("descricao", "")),
                    "valor": v,
                    "vencimento": ven,
                    "instituicao": str(o.get("instituicao", "") or ""),
                    "status": "v"
                })
        if not vencidos and not a_vencer:
            try:
                docs_all = list(root.collection('compromissos').stream())
            except:
                docs_all = []
            for d in docs_all:
                o = d.to_dict() or {}
                mk = str(o.get("mes") or _coerce_date_str(o)[:7])
                if mk != mes_atual:
                    continue
                v = _coerce_val(o)
                ven = _coerce_date_str(o)
                st = "v"
                try:
                    if ven and ven < hoje:
                        st = "st"
                except:
                    pass
                if st == "st":
                    total_vencidos += v
                    vencidos.append({
                        "codigo": d.id,
                        "descricao": str(o.get("descricao", "")),
                        "valor": v,
                        "vencimento": ven,
                        "instituicao": str(o.get("instituicao", "") or ""),
                        "status": st
                    })
                else:
                    total_a_vencer += v
                    a_vencer.append({
                        "codigo": d.id,
                        "descricao": str(o.get("descricao", "")),
                        "valor": v,
                        "vencimento": ven,
                        "instituicao": str(o.get("instituicao", "") or ""),
                        "status": st
                    })
        if not vencidos and not a_vencer and cliente_id != "default":
            try:
                root_def = db.collection('clientes').document("default")
                itens_def = list(root_def.collection('compromissos').where('mes', '==', mes_atual).stream())
            except:
                itens_def = []
            if not itens_def:
                try:
                    qd = root_def.collection('compromissos').where('vencimento_iso', '>=', dt_ini).where('vencimento_iso', '<', dt_fim)
                    itens_def = list(qd.stream())
                except:
                    itens_def = []
            for it in itens_def:
                o = it.to_dict() or {}
                v = _coerce_val(o)
                ven = _coerce_date_str(o)
                if ven and ven < hoje:
                    total_vencidos += v
                    vencidos.append({
                        "codigo": it.id,
                        "descricao": str(o.get("descricao", "")),
                        "valor": v,
                        "vencimento": ven,
                        "instituicao": str(o.get("instituicao", "") or ""),
                        "status": "st"
                    })
                else:
                    total_a_vencer += v
                    a_vencer.append({
                        "codigo": it.id,
                        "descricao": str(o.get("descricao", "")),
                        "valor": v,
                        "vencimento": ven,
                        "instituicao": str(o.get("instituicao", "") or ""),
                        "status": "v"
                    })
        if tipo in ("vencidos", "a_vencer"):
            lst = vencidos if tipo == "vencidos" else a_vencer
            return jsonify({
                "sucesso": True,
                "mes": mes_atual,
                "tipo": tipo,
                "compromissos": lst,
                "total": sum(float(x.get("valor", 0) or 0) for x in lst)
            })
        if not vencidos and not a_vencer:
            try:
                mm = root.collection('meses').document(mes_atual).get().to_dict() or {}
                cats = dict(mm.get("categorias_saida", {}) or {})
                known = {"internet","energia","água","agua","aluguel","condomínio","condominio","telefonia","telefone","assinatura","fatura"}
                for k, v in cats.items():
                    if str(k).strip().lower() in known and float(v or 0) > 0:
                        total_a_vencer += float(v or 0)
                        a_vencer.append({
                            "codigo": None,
                            "descricao": str(k),
                            "valor": float(v or 0),
                            "vencimento": None,
                            "instituicao": "",
                            "status": "v"
                        })
            except:
                pass
        return jsonify({
            "sucesso": True,
            "mes": mes_atual,
            "totais": {
                "vencidos": total_vencidos,
                "a_vencer": total_a_vencer,
                "total": total_vencidos + total_a_vencer,
            },
            "vencidos": vencidos,
            "a_vencer": a_vencer,
        })
    except Exception as e:
        return jsonify({"sucesso": False, "erro": str(e)}), 500
@app.route('/compromissos/meses', methods=['GET'])
def compromissos_meses():
    try:
        db = get_db()
        cliente_id = str(request.args.get("cliente_id") or "default")
        cliente_nome = request.args.get("cliente_nome")
        cliente_username = request.args.get("username")
        try:
            ensure_cliente(cliente_id, nome=cliente_nome, username=cliente_username)
        except:
            pass
        root = db.collection('clientes').document(cliente_id)
        meses = {}
        hoje = _day_key_sp()
        try:
            docs = list(root.collection('compromissos').stream())
        except:
            docs = []
        for d in docs:
            o = d.to_dict() or {}
            ven = _coerce_date_str(o)
            if ven and ven >= hoje:
                mk = str(o.get("mes") or ven[:7])
                meses[mk] = float(meses.get(mk, 0) or 0) + float(_coerce_val(o) or 0)
        if not meses and cliente_id != "default":
            try:
                root_def = db.collection('clientes').document("default")
                docs2 = list(root_def.collection('compromissos').stream())
            except:
                docs2 = []
            for d in docs2:
                o = d.to_dict() or {}
                ven = _coerce_date_str(o)
                if ven and ven >= hoje:
                    mk = str(o.get("mes") or ven[:7])
                    meses[mk] = float(meses.get(mk, 0) or 0) + float(_coerce_val(o) or 0)
        lst = sorted([m for m, tot in meses.items() if float(tot or 0) > 0])
        return jsonify({"sucesso": True, "meses": lst})
    except Exception as e:
        return jsonify({"sucesso": False, "erro": str(e)}), 500
@app.route('/metas/mes', methods=['GET'])
def metas_mes():
    mes_qs = request.args.get("mes")
    mes_atual = mes_qs or _month_key_sp()
    try:
        db = get_db()
        cliente_id = str(request.args.get("cliente_id") or "default")
        cliente_nome = request.args.get("cliente_nome")
        cliente_username = request.args.get("username")
        try:
            ensure_cliente(cliente_id, nome=cliente_nome, username=cliente_username)
        except:
            pass
        root = db.collection('clientes').document(cliente_id)
        metas = []
        total = 0.0
        try:
            docs = list(root.collection('metas').where('mes', '==', mes_atual).stream())
        except:
            docs = []
        for d in docs:
            o = d.to_dict() or {}
            v = _coerce_val(o)
            metas.append({
                "descricao": str(o.get("descricao", "")),
                "valor": v,
                "mes": mes_atual
            })
            total += float(v or 0)
        return jsonify({"sucesso": True, "mes": mes_atual, "metas": metas, "total": total})
    except Exception as e:
        return jsonify({"sucesso": False, "erro": str(e)}), 500
@app.route('/metas/meses', methods=['GET'])
def metas_meses():
    try:
        db = get_db()
        cliente_id = str(request.args.get("cliente_id") or "default")
        cliente_nome = request.args.get("cliente_nome")
        cliente_username = request.args.get("username")
        try:
            ensure_cliente(cliente_id, nome=cliente_nome, username=cliente_username)
        except:
            pass
        root = db.collection('clientes').document(cliente_id)
        meses = {}
        try:
            docs = list(root.collection('metas').stream())
        except:
            docs = []
        for d in docs:
            o = d.to_dict() or {}
            mk = str(o.get("mes") or "")
            if mk:
                meses[mk] = float(meses.get(mk, 0) or 0) + float(_coerce_val(o) or 0)
        if not meses and cliente_id != "default":
            try:
                root_def = db.collection('clientes').document("default")
                docs2 = list(root_def.collection('metas').stream())
            except:
                docs2 = []
            for d in docs2:
                o = d.to_dict() or {}
                mk = str(o.get("mes") or "")
                if mk:
                    meses[mk] = float(meses.get(mk, 0) or 0) + float(_coerce_val(o) or 0)
        lst = sorted([m for m, tot in meses.items() if float(tot or 0) > 0])
        return jsonify({"sucesso": True, "meses": lst})
    except Exception as e:
        return jsonify({"sucesso": False, "erro": str(e)}), 500
@app.route('/metas/adicionar', methods=['POST'])
def metas_adicionar():
    data = request.json
    if not data:
        return jsonify({"sucesso": False, "erro": "Payload não fornecido"}), 400
    try:
        cliente_id = str(data.get("cliente_id") or "default")
        cliente_nome = data.get("cliente_nome")
        cliente_username = data.get("username")
        try:
            ensure_cliente(cliente_id, nome=cliente_nome, username=cliente_username)
        except:
            pass
        descricao = str(data.get("descricao", "") or "").strip()
        valor = float(data.get("valor"))
        mes_atual = str(data.get("mes") or _month_key_sp())
    except Exception:
        return jsonify({"sucesso": False, "erro": "Campos inválidos"}), 400
    try:
        db = get_db()
        root = db.collection('clientes').document(cliente_id)
        ref = root.collection('metas').document()
        doc = {
            "descricao": descricao,
            "valor": float(valor),
            "mes": mes_atual,
            "criado_em": firestore.SERVER_TIMESTAMP,
            "origem": "api-metas",
        }
        ref.set(doc)
        try:
            docs = list(root.collection('metas').where('mes', '==', mes_atual).stream())
        except:
            docs = []
        total = 0.0
        metas = []
        for d in docs:
            o = d.to_dict() or {}
            v = _coerce_val(o)
            metas.append({
                "descricao": str(o.get("descricao", "")),
                "valor": v,
                "mes": mes_atual
            })
            total += float(v or 0)
        return jsonify({"sucesso": True, "meta_id": ref.id, "meta": doc, "mes": mes_atual, "metas": metas, "total": total})
    except Exception as e:
        return jsonify({"sucesso": False, "erro": str(e)}), 500
@app.route('/ajustes/adicionar', methods=['POST'])
def ajustes_adicionar():
    data = request.json
    if not data:
        return jsonify({"sucesso": False, "erro": "Payload não fornecido"}), 400
    try:
        cliente_id = str(data.get("cliente_id") or "default")
        valor = float(data.get("valor"))
        operacao = str(data.get("operacao", "somar")).strip().lower()
        alvo = str(data.get("alvo", "saldo")).strip().lower()
        categoria = str(data.get("categoria", "ajuste"))
        descricao = str(data.get("descricao", "")) or ("Ajuste " + ("+" if operacao == "somar" else "-") + f"{valor}")
        motivo = str(data.get("motivo", "")) or None
    except:
        return jsonify({"sucesso": False, "erro": "Campos inválidos"}), 400
    sign = 1.0 if operacao == "somar" else -1.0
    dv = valor * sign
    try:
        db = get_db()
        root = db.collection('clientes').document(cliente_id)
        dr = data.get("data_referencia") or _day_key_sp()
        mref = root.collection('meses').document(dr[:7])
        dref = root.collection('dias').document(dr)
        batch = db.batch()
        tdoc = {
            "valor": abs(valor),
            "tipo": "ajuste",
            "categoria": categoria,
            "descricao": descricao,
            "data_referencia": dr,
            "timestamp_criacao": firestore.SERVER_TIMESTAMP,
            "moeda": "BRL",
            "origem": "api-ajuste",
            "referencia_id": None,
            "motivo_ajuste": motivo,
            "imutavel": True,
        }
        # Nested write under day
        day_ref = root.collection('transacoes').document(dr)
        item_ref = day_ref.collection('items').document()
        tdoc["ref_id"] = build_ref_id(dr, item_ref.id)
        batch.set(item_ref, tdoc)
        inc_d = {"quantidade_transacoes": firestore.Increment(1), "quantidade_transacoes_validas": firestore.Increment(1), "atualizado_em": firestore.SERVER_TIMESTAMP}
        inc_m = {"quantidade_transacoes": firestore.Increment(1), "quantidade_transacoes_validas": firestore.Increment(1), "atualizado_em": firestore.SERVER_TIMESTAMP}
        if alvo == "saldo":
            inc_d.update({"total_ajuste": firestore.Increment(dv), "saldo_dia": firestore.Increment(dv)})
            inc_m.update({"total_ajuste": firestore.Increment(dv), "saldo_mes": firestore.Increment(dv)})
        elif alvo == "entrada":
            inc_d.update({"total_entrada": firestore.Increment(dv), "saldo_dia": firestore.Increment(dv)})
            inc_m.update({"total_entrada": firestore.Increment(dv), "saldo_mes": firestore.Increment(dv)})
        elif alvo == "saida":
            inc_d.update({"total_saida": firestore.Increment(dv), "saldo_dia": firestore.Increment(-dv)})
            inc_m.update({"total_saida": firestore.Increment(dv), "saldo_mes": firestore.Increment(-dv)})
        else:
            inc_d.update({"total_ajuste": firestore.Increment(dv), "saldo_dia": firestore.Increment(dv)})
            inc_m.update({"total_ajuste": firestore.Increment(dv), "saldo_mes": firestore.Increment(dv)})
        batch.set(dref, inc_d, merge=True)
        batch.set(mref, inc_m, merge=True)
        batch.commit()
        ddoc = dref.get().to_dict() or {}
        mdoc = mref.get().to_dict() or {}
        return jsonify({
            "sucesso": True,
            "ajuste": tdoc,
            "totais_dia": {
                "despesas": float(ddoc.get("total_saida", 0) or 0),
                "receitas": float(ddoc.get("total_entrada", 0) or 0),
                "ajustes": float(ddoc.get("total_ajuste", 0) or 0),
                "saldo": float(ddoc.get("saldo_dia", 0) or 0),
            },
            "totais_mes": {
                "despesas": float(mdoc.get("total_saida", 0) or 0),
                "receitas": float(mdoc.get("total_entrada", 0) or 0),
                "ajustes": float(mdoc.get("total_ajuste", 0) or 0),
                "saldo": float(mdoc.get("saldo_mes", 0) or 0),
            }
        })
    except Exception as e:
        return jsonify({"sucesso": False, "erro": str(e)}), 500
@app.route('/ajustes/estornar', methods=['POST'])
def ajustes_estornar():
    data = request.json
    if not data:
        return jsonify({"sucesso": False, "erro": "Payload não fornecido"}), 400
    try:
        cliente_id = str(data.get("cliente_id") or "default")
        referencia_id = str(data.get("referencia_id"))
        motivo = str(data.get("motivo", "")) or None
    except:
        return jsonify({"sucesso": False, "erro": "Campos inválidos"}), 400
    try:
        from app.services.database import estornar_transacao
        payload = estornar_transacao(cliente_id, referencia_id, motivo=motivo, origem="api-estorno")
        if not payload:
            return jsonify({"sucesso": False, "erro": "Falha ao estornar transação"}), 500
        root = get_db().collection('clientes').document(cliente_id)
        dr = payload.get("data_referencia")
        ddoc = root.collection('dias').document(dr).get().to_dict() or {}
        mdoc = root.collection('meses').document(dr[:7]).get().to_dict() or {}
        return jsonify({
            "sucesso": True,
            "estorno": {
                k: (_now_sp().isoformat() if k == "timestamp_criacao" else v)
                for k, v in payload.items()
                if k != "imutavel"
            },
            "totais_dia": {
                "despesas": float(ddoc.get("total_saida", 0) or 0),
                "receitas": float(ddoc.get("total_entrada", 0) or 0),
                "ajustes": float(ddoc.get("total_ajuste", 0) or 0),
                "estornos": float(ddoc.get("total_estorno", 0) or 0),
                "saldo": float(ddoc.get("saldo_dia", 0) or 0),
            },
            "totais_mes": {
                "despesas": float(mdoc.get("total_saida", 0) or 0),
                "receitas": float(mdoc.get("total_entrada", 0) or 0),
                "ajustes": float(mdoc.get("total_ajuste", 0) or 0),
                "estornos": float(mdoc.get("total_estorno", 0) or 0),
                "saldo": float(mdoc.get("saldo_mes", 0) or 0),
            }
        })
    except Exception as e:
        return jsonify({"sucesso": False, "erro": str(e)}), 500
@app.route('/transacoes/atualizar_categoria', methods=['POST'])
def transacoes_atualizar_categoria():
    data = request.json
    if not data:
        return jsonify({"sucesso": False, "erro": "Payload não fornecido"}), 400
    try:
        cliente_id = str(data.get("cliente_id") or "default")
        referencia_id = str(data.get("referencia_id"))
        nova_categoria = str(data.get("nova_categoria", "") or "")
        nova_descricao = str(data.get("nova_descricao", "") or "")
    except:
        return jsonify({"sucesso": False, "erro": "Campos inválidos"}), 400
    if not nova_categoria:
        return jsonify({"sucesso": False, "erro": "Nova categoria não fornecida"}), 400
    try:
        from app.services.database import atualizar_categoria_transacao, get_db
        res = atualizar_categoria_transacao(cliente_id, referencia_id, nova_categoria, nova_descricao if nova_descricao else None)
        if not res:
            return jsonify({"sucesso": False, "erro": "Falha ao atualizar categoria"}), 500
        root = get_db().collection('clientes').document(cliente_id)
        dr = res.get("data_referencia")
        ddoc = root.collection('dias').document(dr).get().to_dict() or {}
        mdoc = root.collection('meses').document(dr[:7]).get().to_dict() or {}
        return jsonify({
            "sucesso": True,
            "atualizacao": res,
            "totais_dia": {
                "despesas": float(ddoc.get("total_saida", 0) or 0),
                "receitas": float(ddoc.get("total_entrada", 0) or 0),
                "ajustes": float(ddoc.get("total_ajuste", 0) or 0),
                "estornos": float(ddoc.get("total_estorno", 0) or 0),
                "saldo": float(ddoc.get("saldo_dia", 0) or 0),
            },
            "totais_mes": {
                "despesas": float(mdoc.get("total_saida", 0) or 0),
                "receitas": float(mdoc.get("total_entrada", 0) or 0),
                "ajustes": float(mdoc.get("total_ajuste", 0) or 0),
                "estornos": float(mdoc.get("total_estorno", 0) or 0),
                "saldo": float(mdoc.get("saldo_mes", 0) or 0),
            }
        })
    except Exception as e:
        return jsonify({"sucesso": False, "erro": str(e)}), 500
@app.route('/ajustes/buscar_por_valor', methods=['POST'])
def ajustes_buscar_por_valor():
    data = request.json
    if not data:
        return jsonify({"sucesso": False, "erro": "Payload não fornecido"}), 400
    try:
        cliente_id = str(data.get("cliente_id") or "default")
        dr = str(data.get("data_referencia"))
        v = float(data.get("valor"))
    except:
        return jsonify({"sucesso": False, "erro": "Campos inválidos: cliente_id, data_referencia, valor"}), 400
    try:
        db = get_db()
        tipo_raw = str(data.get("tipo") or "").strip().lower() or None
        tipo = None
        if tipo_raw in ("entrada", "receita", "1"):
            tipo = "entrada"
        elif tipo_raw in ("saida", "despesa", "0"):
            tipo = "saida"
        tolerancia = float(data.get("tolerancia", 0.005) or 0.005)
        desc_contains = str(data.get("descricao_contains") or "").strip().lower() or None
        matches = []
        omitidos_estornados = []
        # Prefer nested items: clientes/{id}/transacoes/{dr}/items/*
        try:
            items = db.collection('clientes').document(cliente_id).collection('transacoes').document(dr).collection('items').stream()
        except:
            items = []
        for d in items:
            o = d.to_dict() or {}
            tp = str(o.get('tipo', '')).strip().lower()
            if tp in ('ajuste', 'estorno'):
                continue
            if bool(o.get('estornado', False)):
                omitidos_estornados.append(build_ref_id(dr, d.id))
                continue
            if tipo and tp != tipo:
                continue
            val = float(o.get('valor', 0) or 0)
            if not (abs(val - v) < tolerancia or val == v):
                continue
            if desc_contains:
                dl = str(o.get('descricao', '')).strip().lower()
                if desc_contains not in dl:
                    continue
            ts = o.get('timestamp_criacao')
            ts_str = None
            try:
                ts_str = ts.isoformat() if hasattr(ts, 'isoformat') else (str(ts) if ts else None)
            except:
                ts_str = None
            safe = {
                "id": build_ref_id(dr, d.id),
                "valor": val,
                "tipo": tp,
                "categoria": str(o.get('categoria', 'outros')),
                "descricao": str(o.get('descricao', '')),
                "data_referencia": str(o.get('data_referencia') or dr),
                "origem": str(o.get('origem', '')),
            }
            if ts_str:
                safe["timestamp_criacao"] = ts_str
            matches.append(safe)
        if not matches:
            # Fallback to flat collection if nested empty
            tcoll = db.collection('clientes').document(cliente_id).collection('transacoes')
            q = tcoll.where('data_referencia', '==', dr)
            for d in q.stream():
                o = d.to_dict() or {}
                tp = str(o.get('tipo', '')).strip().lower()
                if tp in ('ajuste', 'estorno'):
                    continue
                if bool(o.get('estornado', False)):
                    omitidos_estornados.append(d.id)
                    continue
                if tipo and tp != tipo:
                    continue
                val = float(o.get('valor', 0) or 0)
                if not (abs(val - v) < tolerancia or val == v):
                    continue
                if desc_contains:
                    dl = str(o.get('descricao', '')).strip().lower()
                    if desc_contains not in dl:
                        continue
                ts = o.get('timestamp_criacao')
                ts_str = None
                try:
                    ts_str = ts.isoformat() if hasattr(ts, 'isoformat') else (str(ts) if ts else None)
                except:
                    ts_str = None
                safe = {
                    "id": d.id,
                    "valor": val,
                    "tipo": tp,
                    "categoria": str(o.get('categoria', 'outros')),
                    "descricao": str(o.get('descricao', '')),
                    "data_referencia": str(o.get('data_referencia') or dr),
                    "origem": str(o.get('origem', '')),
                }
                if ts_str:
                    safe["timestamp_criacao"] = ts_str
                matches.append(safe)
        return jsonify({
            "sucesso": True,
            "quantidade": len(matches),
            "matches": matches,
            "omitidos_estornados": len(omitidos_estornados),
            "ids_omitidos_estornados": omitidos_estornados,
            "avisos": (["Algumas transações já estavam estornadas e foram omitidas."] if omitidos_estornados else [])
        })
    except Exception as e:
        return jsonify({"sucesso": False, "erro": str(e)}), 500
@app.route('/extrato/hoje', methods=['GET'])
def extrato_hoje():
    """Retorna extrato do dia atual."""
    data_atual = _day_key_sp()
    try:
        db = get_db()
        cliente_id = str(request.args.get("cliente_id") or "default")
        cliente_nome = request.args.get("cliente_nome")
        cliente_username = request.args.get("username")
        include_trans = str(request.args.get("include_transacoes", "true")).strip().lower() != "false"
        try:
            ensure_cliente(cliente_id, nome=cliente_nome, username=cliente_username)
        except:
            pass
        root = db.collection('clientes').document(cliente_id)
        transacoes = []
        if include_trans:
            docs = []
            tops = []
            try:
                docs = root.collection('transacoes').document(data_atual).collection('items').stream()
            except:
                docs = []
            try:
                tops = root.collection('transacoes').where('data_referencia', '==', data_atual).stream()
            except:
                tops = []
            idx = {}
            tl = []
            for d in docs:
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
            transacoes = tl
        dref = root.collection('dias').document(data_atual).get()
        dd = dref.to_dict() or {}
        despesas = float(dd.get("total_saida", 0) or 0)
        receitas = float(dd.get("total_entrada", 0) or 0)
        ajustes = float(dd.get("total_ajuste", 0) or 0)
        estornos = float(dd.get("total_estorno", 0) or 0)
        try:
            saldo = float(dd.get("saldo_dia")) if dd.get("saldo_dia") is not None else (receitas - despesas + ajustes)
        except:
            saldo = receitas - despesas + ajustes
        need_fallback = False
        try:
            need_fallback = (despesas == 0.0 and receitas == 0.0 and ajustes == 0.0 and estornos == 0.0)
        except:
            need_fallback = True
        if need_fallback and transacoes:
            td = tr = taj = tes = 0.0
            cnt_tmp = 0
            for t in transacoes:
                if bool(t.get('estornado', False)):
                    continue
                tp = str(t.get('tipo', '')).strip().lower()
                val = float(t.get('valor', 0) or 0)
                if tp in ('entrada', '1', 'receita'):
                    tr += val
                    cnt_tmp += 1
                elif tp in ('saida', '0', 'despesa'):
                    td += val
                    cnt_tmp += 1
                elif tp in ('ajuste',):
                    taj += val
                    cnt_tmp += 1
                elif tp in ('estorno',):
                    tes += abs(val)
            despesas = float(td or 0)
            receitas = float(tr or 0)
            ajustes = float(taj or 0)
            estornos = float(tes or 0)
            saldo = receitas - despesas + ajustes
            try:
                root.collection('dias').document(data_atual).set({
                    "total_entrada": receitas,
                    "total_saida": despesas,
                    "total_ajuste": ajustes,
                    "total_estorno": estornos,
                    "saldo_dia": saldo,
                    "quantidade_transacoes_validas": int(cnt_tmp),
                    "atualizado_em": firestore.SERVER_TIMESTAMP,
                }, merge=True)
            except:
                pass
        # Contagem válida: entradas/saídas (não considera 'estorno' como transação)
        try:
            cnt = 0
            for t in transacoes:
                if bool(t.get('estornado', False)):
                    continue
                tp = str(t.get('tipo', '')).strip().lower()
                if tp in ('entrada', '1', 'receita') or tp in ('saida', '0', 'despesa') or tp in ('ajuste',):
                    cnt += 1
            qtd_validas = int(cnt)
        except:
            qtd_validas = int(dd.get("quantidade_transacoes_validas", dd.get("quantidade_transacoes", 0)) or 0)
        
        return jsonify({
            "sucesso": True,
            "data": data_atual,
            "transacoes": transacoes,
            "total": {
                "despesas": despesas,
                "receitas": receitas,
                "saldo": saldo,
                "estornos": estornos
            },
            "quantidade_transacoes_validas": int(qtd_validas)
        })
        
    except Exception as e:
        return jsonify({"sucesso": False, "erro": str(e)}), 500

@app.route('/total/mes', methods=['GET'])
def total_mes():
    """Retorna totais do mês atual ou do mês fornecido."""
    mes_qs = request.args.get("mes")
    mes_atual = mes_qs or _month_key_sp()
    total_despesas = 0
    total_receitas = 0
    quantidade_transacoes_validas = 0
    total_ajustes = 0
    total_estornos = 0
    
    try:
        db = get_db()
        cliente_id = str(request.args.get("cliente_id") or "default")
        cliente_nome = request.args.get("cliente_nome")
        cliente_username = request.args.get("username")
        try:
            ensure_cliente(cliente_id, nome=cliente_nome, username=cliente_username)
        except:
            pass
        mref = db.collection('clientes').document(cliente_id).collection('meses').document(mes_atual).get()
        mm = mref.to_dict() or {}
        total_despesas = float(mm.get("total_saida", 0) or 0)
        total_receitas = float(mm.get("total_entrada", 0) or 0)
        total_ajustes = float(mm.get("total_ajuste", 0) or 0)
        total_estornos = float(mm.get("total_estorno", 0) or 0)
        saldo = total_receitas - total_despesas + total_ajustes
        quantidade_transacoes_validas = int(mm.get("quantidade_transacoes_validas", mm.get("quantidade_transacoes", 0)) or 0)
        if (total_despesas == 0 and total_receitas == 0 and total_ajustes == 0 and total_estornos == 0):
            try:
                ano, mes = mes_atual.split("-")
                dt_ini = f"{ano}-{mes}-01"
                if mes == "12":
                    dt_fim = f"{int(ano)+1}-01-01"
                else:
                    dt_fim = f"{ano}-{int(mes)+1:02d}-01"
                root = db.collection('clientes').document(cliente_id)
                cur = datetime.strptime(dt_ini, "%Y-%m-%d")
                end = datetime.strptime(dt_fim, "%Y-%m-%d")
                td = tr = taj = tes = 0.0
                while cur < end:
                    dkey = cur.strftime("%Y-%m-%d")
                    try:
                        dd = root.collection('dias').document(dkey).get().to_dict() or {}
                        td += float(dd.get("total_saida", 0) or 0)
                        tr += float(dd.get("total_entrada", 0) or 0)
                        taj += float(dd.get("total_ajuste", 0) or 0)
                        tes += float(dd.get("total_estorno", 0) or 0)
                    except:
                        pass
                    cur = cur + timedelta(days=1)
                total_despesas = td
                total_receitas = tr
                total_ajustes = taj
                total_estornos = tes
                saldo = total_receitas - total_despesas + total_ajustes
            except:
                pass
        try:
            ano, mes = mes_atual.split("-")
            dt_ini = f"{ano}-{mes}-01"
            if mes == "12":
                dt_fim = f"{int(ano)+1}-01-01"
            else:
                dt_fim = f"{ano}-{int(mes)+1:02d}-01"
            root = db.collection('clientes').document(cliente_id)
            cur = datetime.strptime(dt_ini, "%Y-%m-%d")
            end = datetime.strptime(dt_fim, "%Y-%m-%d")
            qtd_sum = 0
            while cur < end:
                dkey = cur.strftime("%Y-%m-%d")
                try:
                    # Reconta SEMPRE deduplicando itens + topo por dia
                    items = []
                    tops = []
                    try:
                        items = root.collection('transacoes').document(dkey).collection('items').stream()
                    except:
                        items = []
                    try:
                        tops = root.collection('transacoes').where('data_referencia', '==', dkey).stream()
                    except:
                        tops = []
                    idx = {}
                    cnt = 0
                    for it in items:
                        o = it.to_dict() or {}
                        k = str(o.get('ref_id') or '') or (str(o.get('tipo', '')) + '|' + str(float(o.get('valor', 0) or 0)) + '|' + str(o.get('categoria', '')) + '|' + str(o.get('descricao', '')) + '|' + str(o.get('timestamp_criacao', '')))
                        if idx.get(k):
                            continue
                        idx[k] = 1
                        tp = str(o.get('tipo', '')).strip().lower()
                        if tp in ('entrada', '1', 'receita') or tp in ('saida', '0', 'despesa') or tp in ('ajuste',):
                            if not bool(o.get('estornado', False)):
                                cnt += 1
                    for it in tops:
                        o = it.to_dict() or {}
                        k = str(o.get('ref_id') or '') or (str(o.get('tipo', '')) + '|' + str(float(o.get('valor', 0) or 0)) + '|' + str(o.get('categoria', '')) + '|' + str(o.get('descricao', '')) + '|' + str(o.get('timestamp_criacao', '')))
                        if idx.get(k):
                            continue
                        idx[k] = 1
                        tp = str(o.get('tipo', '')).strip().lower()
                        if tp in ('entrada', '1', 'receita') or tp in ('saida', '0', 'despesa') or tp in ('ajuste',):
                            if not bool(o.get('estornado', False)):
                                cnt += 1
                    qtd_sum += cnt
                except:
                    pass
                cur = cur + timedelta(days=1)
            quantidade_transacoes_validas = int(qtd_sum)
        except:
            pass
        try:
            mdoc = db.collection('clientes').document(cliente_id).collection('meses').document(mes_atual)
            mm2 = {
                "total_entrada": float(total_receitas or 0),
                "total_saida": float(total_despesas or 0),
                "total_ajuste": float(total_ajustes or 0),
                "total_estorno": float(total_estornos or 0),
                "saldo_mes": float((total_receitas - total_despesas + total_ajustes) or 0),
                "quantidade_transacoes_validas": int(quantidade_transacoes_validas or 0),
            }
            mdoc.set(mm2, merge=True)
        except:
            pass
        
        return jsonify({
            "sucesso": True,
            "mes": mes_atual,
            "total": {
                "despesas": total_despesas,
                "receitas": total_receitas,
                "saldo": saldo,
                "estornos": total_estornos,
                "ajustes": total_ajustes
            },
            "quantidade_transacoes": int(mm.get("quantidade_transacoes", 0) or 0),
            "quantidade_transacoes_validas": int(quantidade_transacoes_validas)
        })
        
    except Exception as e:
        return jsonify({"sucesso": False, "erro": str(e)}), 500

@app.route('/categorias/mes', methods=['GET'])
def categorias_mes():
    mes_qs = request.args.get("mes")
    mes_atual = mes_qs or _month_key_sp()
    categorias = {}
    total_despesas = 0
    categorias_estorno = {}
    total_estornos = 0
    
    try:
        db = get_db()
        cliente_id = str(request.args.get("cliente_id") or "default")
        cliente_nome = request.args.get("cliente_nome")
        cliente_username = request.args.get("username")
        try:
            ensure_cliente(cliente_id, nome=cliente_nome, username=cliente_username)
        except:
            pass
        mdoc = db.collection('clientes').document(cliente_id).collection('meses').document(mes_atual).get()
        mm = mdoc.to_dict() or {}
        categorias_map = dict(mm.get("categorias_saida", {}) or {})
        categorias_est_map = dict(mm.get("categorias_estorno", {}) or {})
        categorias = {k: float(v or 0) for k, v in categorias_map.items() if float(v or 0) > 0}
        categorias_estorno = {k: float(v or 0) for k, v in categorias_est_map.items() if float(v or 0) > 0}
        mm_total_saida = float(mm.get("total_saida", 0) or 0)
        sum_cat_exp = sum(float(v or 0) for v in categorias.values())
        sum_cat_est = sum(float(v or 0) for v in categorias_estorno.values())
        recalc = False
        try:
            if (not categorias and not categorias_estorno) or (abs(sum_cat_exp - mm_total_saida) > 1e-6):
                recalc = True
        except:
            recalc = True
        if recalc:
            ano, mes = mes_atual.split("-")
            dt_ini = f"{ano}-{mes}-01"
            if mes == "12":
                dt_fim = f"{int(ano)+1}-01-01"
            else:
                dt_fim = f"{ano}-{int(mes)+1:02d}-01"
            root = db.collection('clientes').document(cliente_id)
            cur = datetime.strptime(dt_ini, "%Y-%m-%d")
            end = datetime.strptime(dt_fim, "%Y-%m-%d")
            categorias = {}
            categorias_estorno = {}
            while cur < end:
                dkey = cur.strftime("%Y-%m-%d")
                try:
                    dd = root.collection('dias').document(dkey).get().to_dict() or {}
                except:
                    dd = {}
                try:
                    for k, v in dict(dd.get("categorias_saida", {}) or {}).items():
                        categorias[k] = float(categorias.get(k, 0) or 0) + float(v or 0)
                    for k, v in dict(dd.get("categorias_estorno", {}) or {}).items():
                        categorias_estorno[k] = float(categorias_estorno.get(k, 0) or 0) + float(v or 0)
                except:
                    pass
                cur = cur + timedelta(days=1)
            if not categorias and not categorias_estorno:
                cur = datetime.strptime(dt_ini, "%Y-%m-%d")
                end = datetime.strptime(dt_fim, "%Y-%m-%d")
                while cur < end:
                    dkey = cur.strftime("%Y-%m-%d")
                    try:
                        items = root.collection('transacoes').document(dkey).collection('items').stream()
                    except:
                        items = []
                    for it in items:
                        t = it.to_dict() or {}
                        tp_raw = str(t.get('tipo', '')).strip().lower()
                        val = float(t.get('valor', 0) or 0)
                        cat = str(t.get('categoria', 'outros') or 'outros').strip().lower()
                        if tp_raw in ('0', 'despesa', 'saida'):
                            try:
                                categorias[cat] = float(categorias.get(cat, 0) or 0) + float(val or 0)
                            except:
                                pass
                        elif tp_raw in ('estorno',):
                            try:
                                categorias_estorno[cat] = float(categorias_estorno.get(cat, 0) or 0) + float(abs(val) or 0)
                            except:
                                pass
                            try:
                                ref_id = str(t.get('referencia_id') or '')
                                dr_ref, did_ref = parse_ref_id(ref_id)
                                if dr_ref and did_ref:
                                    odoc = root.collection('transacoes').document(dr_ref).collection('items').document(did_ref).get()
                                    o = odoc.to_dict() or {}
                                    otp_raw = str(o.get('tipo', '')).strip().lower()
                                    if otp_raw in ('0', 'despesa', 'saida'):
                                        oc = str(o.get('categoria', 'outros') or 'outros').strip().lower()
                                        categorias[oc] = float(categorias.get(oc, 0) or 0) - float(abs(val) or 0)
                            except:
                                pass
                    cur = cur + timedelta(days=1)
            if not categorias and not categorias_estorno:
                cur = datetime.strptime(dt_ini, "%Y-%m-%d")
                end = datetime.strptime(dt_fim, "%Y-%m-%d")
                while cur < end:
                    dkey = cur.strftime("%Y-%m-%d")
                    try:
                        items = list(root.collection('transacoes').document(dkey).collection('items').stream())
                    except:
                        items = []
                    try:
                        tops = list(root.collection('transacoes').where('data_referencia', '==', dkey).stream())
                    except:
                        tops = []
                    idx = {}
                    for it in items:
                        o = it.to_dict() or {}
                        k = str(o.get('ref_id') or '') or (str(o.get('tipo', '')) + '|' + str(float(o.get('valor', 0) or 0)) + '|' + str(o.get('categoria', '')) + '|' + str(o.get('descricao', '')) + '|' + str(o.get('timestamp_criacao', '')))
                        if idx.get(k):
                            continue
                        idx[k] = 1
                        tp_raw = str(o.get('tipo', '')).strip().lower()
                        cat = str(o.get('categoria', 'outros') or 'outros').strip().lower()
                        val = float(o.get('valor', 0) or 0)
                        if tp_raw in ('0', 'despesa', 'saida'):
                            try:
                                categorias[cat] = float(categorias.get(cat, 0) or 0) + float(val or 0)
                            except:
                                pass
                        elif tp_raw in ('estorno',):
                            try:
                                categorias_estorno[cat] = float(categorias_estorno.get(cat, 0) or 0) + float(abs(val) or 0)
                            except:
                                pass
                    for it in tops:
                        o = it.to_dict() or {}
                        k = str(o.get('ref_id') or '') or (str(o.get('tipo', '')) + '|' + str(float(o.get('valor', 0) or 0)) + '|' + str(o.get('categoria', '')) + '|' + str(o.get('descricao', '')) + '|' + str(o.get('timestamp_criacao', '')))
                        if idx.get(k):
                            continue
                        idx[k] = 1
                        tp_raw = str(o.get('tipo', '')).strip().lower()
                        cat = str(o.get('categoria', 'outros') or 'outros').strip().lower()
                        val = float(o.get('valor', 0) or 0)
                        if tp_raw in ('0', 'despesa', 'saida'):
                            try:
                                categorias[cat] = float(categorias.get(cat, 0) or 0) + float(val or 0)
                            except:
                                pass
                        elif tp_raw in ('estorno',):
                            try:
                                categorias_estorno[cat] = float(categorias_estorno.get(cat, 0) or 0) + float(abs(val) or 0)
                            except:
                                pass
                        cur = cur + timedelta(days=1)
                try:
                    mdoc_set = root.collection('meses').document(mes_atual)
                    mdoc_set.set({
                        "categorias_saida": {k: float(v or 0) for k, v in categorias.items()},
                        "categorias_estorno": {k: float(v or 0) for k, v in categorias_estorno.items()},
                        "atualizado_em": firestore.SERVER_TIMESTAMP,
                    }, merge=True)
                except:
                    pass
            if not categorias and not categorias_estorno:
                try:
                    todos = list(root.collection('transacoes').stream())
                except:
                    todos = []
                for it in todos:
                    o = it.to_dict() or {}
                    dr = str(o.get('data_referencia', '') or '')
                    if not dr.startswith(mes_atual + "-"):
                        continue
                    if bool(o.get('estornado', False)):
                        continue
                    tp_raw = str(o.get('tipo', '')).strip().lower()
                    cat = str(o.get('categoria', 'outros') or 'outros').strip().lower()
                    val = float(o.get('valor', 0) or 0)
                    if tp_raw in ('0', 'despesa', 'saida'):
                        try:
                            categorias[cat] = float(categorias.get(cat, 0) or 0) + float(val or 0)
                        except:
                            pass
                    elif tp_raw in ('estorno',):
                        try:
                            categorias_estorno[cat] = float(categorias_estorno.get(cat, 0) or 0) + float(abs(val) or 0)
                        except:
                            pass
                try:
                    mdoc_set2 = root.collection('meses').document(mes_atual)
                    mdoc_set2.set({
                        "categorias_saida": {k: float(v or 0) for k, v in categorias.items()},
                        "categorias_estorno": {k: float(v or 0) for k, v in categorias_estorno.items()},
                        "atualizado_em": firestore.SERVER_TIMESTAMP,
                    }, merge=True)
                except:
                    pass
        try:
            categorias = {k: float(v or 0) - float(categorias_estorno.get(k, 0) or 0) for k, v in categorias.items()}
            categorias = {k: float(v or 0) for k, v in categorias.items() if float(v or 0) > 0}
        except:
            pass
        total_despesas = sum(float(v or 0) for v in categorias.values())
        total_estornos = sum(float(v or 0) for v in categorias_estorno.values())
        
        return jsonify({
            "sucesso": True,
            "mes": mes_atual,
            "categorias": {k: v for k, v in categorias.items() if v > 0},
            "categorias_estorno": {k: v for k, v in categorias_estorno.items() if v > 0},
            "total_despesas": total_despesas,
            "total_estornos": total_estornos
        })
        
    except Exception as e:
        return jsonify({"sucesso": False, "erro": str(e)}), 500

@app.route('/total/semana', methods=['GET'])
def total_semana():
    hoje = _now_sp()
    inicio_semana = hoje - timedelta(days=hoje.weekday())
    total_despesas = 0
    total_receitas = 0
    total_estornos = 0
    total_ajustes = 0
    try:
        db = get_db()
        cliente_id = str(request.args.get("cliente_id") or "default")
        cliente_nome = request.args.get("cliente_nome")
        cliente_username = request.args.get("username")
        try:
            ensure_cliente(cliente_id, nome=cliente_nome, username=cliente_username)
        except:
            pass
        # Itera pelos dias da semana somando agregados
        dt_cur = inicio_semana
        while dt_cur.date() <= hoje.date():
            dkey = dt_cur.strftime("%Y-%m-%d")
            ddoc = db.collection('clientes').document(cliente_id).collection('dias').document(dkey).get()
            o = ddoc.to_dict() or {}
            total_despesas += float(o.get("total_saida", 0) or 0)
            total_receitas += float(o.get("total_entrada", 0) or 0)
            total_estornos += float(o.get("total_estorno", 0) or 0)
            total_ajustes += float(o.get("total_ajuste", 0) or 0)
            dt_cur += timedelta(days=1)
        saldo = total_receitas - total_despesas + total_ajustes
        return jsonify({
            "sucesso": True,
            "inicio": inicio_semana.strftime("%Y-%m-%d"),
            "fim": hoje.strftime("%Y-%m-%d"),
            "total": {
                "despesas": total_despesas,
                "receitas": total_receitas,
                "saldo": saldo,
                "estornos": total_estornos,
                "ajustes": total_ajustes
            }
        })
    except Exception as e:
        return jsonify({"sucesso": False, "erro": str(e)}), 500
@app.route('/total/geral', methods=['GET'])
def total_geral():
    total_despesas = 0
    total_receitas = 0
    total_estornos = 0
    total_ajustes = 0
    try:
        db = get_db()
        cliente_id = str(request.args.get("cliente_id") or "default")
        cliente_nome = request.args.get("cliente_nome")
        cliente_username = request.args.get("username")
        try:
            ensure_cliente(cliente_id, nome=cliente_nome, username=cliente_username)
        except:
            pass
        for m in db.collection('clientes').document(cliente_id).collection('meses').stream():
            o = m.to_dict() or {}
            total_despesas += float(o.get("total_saida", 0) or 0)
            total_receitas += float(o.get("total_entrada", 0) or 0)
            total_estornos += float(o.get("total_estorno", 0) or 0)
            total_ajustes += float(o.get("total_ajuste", 0) or 0)
        saldo = total_receitas - total_despesas + total_ajustes
        return jsonify({
            "sucesso": True,
            "total": {
                "despesas": total_despesas,
                "receitas": total_receitas,
                "saldo": saldo,
                "estornos": total_estornos,
                "ajustes": total_ajustes
            }
        })
    except Exception as e:
        return jsonify({"sucesso": False, "erro": str(e)}), 500
@app.route('/migrar/transacoes', methods=['POST'])
def migrar_transacoes():
    data = request.json
    if not data:
        return jsonify({"sucesso": False, "erro": "Payload não fornecido"}), 400
    try:
        cliente_id = str(data.get("cliente_id") or "default")
        delete_original = bool(data.get("delete_original", False))
    except:
        return jsonify({"sucesso": False, "erro": "Campos inválidos"}), 400
    try:
        from app.services.database import migrate_cliente_transacoes_to_nested
        try:
            ensure_cliente(cliente_id, nome=str(data.get("cliente_nome") or None), username=str(data.get("username") or None))
        except:
            pass
        res = migrate_cliente_transacoes_to_nested(cliente_id, delete_original=delete_original)
        return jsonify({"sucesso": True, "resultado": res})
    except Exception as e:
        return jsonify({"sucesso": False, "erro": str(e)}), 500
@app.route('/saldo/geral', methods=['GET'])
def total_geral_alias():
    return total_geral()
@app.route('/migrar/todos', methods=['POST'])
def migrar_todos():
    data = request.json
    if not data:
        data = {}
    try:
        delete_original = bool(data.get("delete_original", False))
        recompute = bool(data.get("recompute", True))
    except:
        return jsonify({"sucesso": False, "erro": "Campos inválidos"}), 400
    try:
        from app.services.database import migrate_all_clientes
        res = migrate_all_clientes(delete_original=delete_original, recompute=recompute)
        return jsonify({"sucesso": True, "resultado": res})
    except Exception as e:
        return jsonify({"sucesso": False, "erro": str(e)}), 500
@app.route('/recompute/cliente', methods=['POST'])
def recompute_cliente():
    data = request.json
    if not data:
        data = {}
    try:
        cliente_id = str(data.get("cliente_id") or request.args.get("cliente_id") or "default")
        cliente_nome = str(data.get("cliente_nome") or request.args.get("cliente_nome") or "")
        cliente_username = str(data.get("username") or request.args.get("username") or "")
    except:
        return jsonify({"sucesso": False, "erro": "Campos inválidos"}), 400
    try:
        try:
            ensure_cliente(cliente_id, nome=cliente_nome, username=cliente_username)
        except:
            pass
        res = recompute_cliente_aggregates(cliente_id)
        db = get_db()
        root = db.collection('clientes').document(cliente_id)
        mes_atual = _month_key_sp()
        mm = root.collection('meses').document(mes_atual).get().to_dict() or {}
        total = {
            "despesas": float(mm.get("total_saida", 0) or 0),
            "receitas": float(mm.get("total_entrada", 0) or 0),
            "ajustes": float(mm.get("total_ajuste", 0) or 0),
            "estornos": float(mm.get("total_estorno", 0) or 0),
        }
        total["saldo"] = total["receitas"] - total["despesas"] + total["ajustes"]
        qtd_validas = int(mm.get("quantidade_transacoes_validas", mm.get("quantidade_transacoes", 0)) or 0)
        return jsonify({"sucesso": True, "resultado": res, "mes_atual": mes_atual, "total": total, "quantidade_transacoes_validas": qtd_validas})
    except Exception as e:
        return jsonify({"sucesso": False, "erro": str(e)}), 500
@app.route('/saldo/atual', methods=['GET'])
def saldo_atual():
    inicio = request.args.get('inicio')
    fim = request.args.get('fim')
    mes = request.args.get('mes')
    cliente_id = str(request.args.get('cliente_id') or "default")
    categorias_qs = request.args.get('categorias')
    tipo_qs = request.args.get('tipo')
    group_by = request.args.get('group_by')
    cats = None
    if categorias_qs:
        cats = [c.strip().lower() for c in categorias_qs.split(',') if c.strip()]
    tipo_filter = None
    if tipo_qs:
        tl = tipo_qs.strip().lower()
        if tl in ('despesa', 'despesas', '0', 'saida'):
            tipo_filter = 'saida'
        elif tl in ('receita', 'receitas', '1', 'entrada'):
            tipo_filter = 'entrada'
    dt_ini = None
    dt_fim = None
    if mes:
        try:
            ano, m = mes.split("-")
            dt_ini = f"{ano}-{m}-01"
            if m == "12":
                dt_fim = f"{int(ano)+1}-01-01"
            else:
                dt_fim = f"{ano}-{int(m)+1:02d}-01"
        except:
            mes = None
    if not mes and inicio and fim:
        dt_ini = inicio
        dt_fim = fim
    total_despesas = 0.0
    total_receitas = 0.0
    total_ajustes = 0.0
    total_estornos = 0.0
    try:
        db = get_db()
        root = db.collection('clientes').document(cliente_id)
        # Caminho otimizado: sem agrupamento por categoria e sem filtros → usar agregados
        if not group_by and not cats and not tipo_filter:
            if mes:
                mdoc = root.collection('meses').document(mes).get()
                mm = mdoc.to_dict() or {}
                total_despesas = float(mm.get("total_saida", 0) or 0)
                total_receitas = float(mm.get("total_entrada", 0) or 0)
                total_ajustes = float(mm.get("total_ajuste", 0) or 0)
                total_estornos = float(mm.get("total_estorno", 0) or 0)
                try:
                    saldo = float(mm.get("saldo_mes")) if mm.get("saldo_mes") is not None else (total_receitas - total_despesas + total_ajustes)
                except:
                    saldo = total_receitas - total_despesas + total_ajustes
                saldo_real = saldo
                try:
                    sr = 0.0
                    for mdoc2 in root.collection('meses').stream():
                        mid = str(mdoc2.id or "")
                        if mid and mid <= mes:
                            mo = mdoc2.to_dict() or {}
                            try:
                                v = float(mo.get("saldo_mes")) if mo.get("saldo_mes") is not None else (
                                    float(mo.get("total_entrada", 0) or 0) - float(mo.get("total_saida", 0) or 0) + float(mo.get("total_ajuste", 0) or 0)
                                )
                            except:
                                v = (float(mo.get("total_entrada", 0) or 0) - float(mo.get("total_saida", 0) or 0) + float(mo.get("total_ajuste", 0) or 0))
                            sr += float(v or 0)
                    saldo_real = float(sr or saldo)
                except:
                    saldo_real = saldo
                need_fallback = False
                try:
                    need_fallback = (total_despesas == 0.0 and total_receitas == 0.0 and total_ajustes == 0.0 and total_estornos == 0.0)
                except:
                    need_fallback = True
                if need_fallback:
                    try:
                        ano, m = mes.split("-")
                        dt_ini = f"{ano}-{m}-01"
                        if m == "12":
                            dt_fim = f"{int(ano)+1}-01-01"
                        else:
                            dt_fim = f"{ano}-{int(m)+1:02d}-01"
                        dt_cur = datetime.strptime(dt_ini, "%Y-%m-%d")
                        dt_end = datetime.strptime(dt_fim, "%Y-%m-%d")
                        td = tr = taj = tes = 0.0
                        while dt_cur < dt_end:
                            dkey = dt_cur.strftime("%Y-%m-%d")
                            try:
                                dd = root.collection('dias').document(dkey).get().to_dict() or {}
                                td += float(dd.get("total_saida", 0) or 0)
                                tr += float(dd.get("total_entrada", 0) or 0)
                                taj += float(dd.get("total_ajuste", 0) or 0)
                                tes += float(dd.get("total_estorno", 0) or 0)
                            except:
                                pass
                            dt_cur = dt_cur + timedelta(days=1)
                        total_despesas = float(td or 0)
                        total_receitas = float(tr or 0)
                        total_ajustes = float(taj or 0)
                        total_estornos = float(tes or 0)
                        saldo = total_receitas - total_despesas + total_ajustes
                        try:
                            root.collection('meses').document(mes).set({
                                "total_entrada": total_receitas,
                                "total_saida": total_despesas,
                                "total_ajuste": total_ajustes,
                                "total_estorno": total_estornos,
                                "saldo_mes": saldo,
                                "atualizado_em": firestore.SERVER_TIMESTAMP,
                            }, merge=True)
                        except:
                            pass
                    except:
                        pass
            elif dt_ini and dt_fim:
                # Somar agregados de 'dias' iterando pelo intervalo
                dt_cur = datetime.strptime(dt_ini, "%Y-%m-%d")
                dt_end = datetime.strptime(dt_fim, "%Y-%m-%d")
                # Inclusivo em dt_end
                while dt_cur <= dt_end:
                    dkey = dt_cur.strftime("%Y-%m-%d")
                    ddoc = root.collection('dias').document(dkey).get()
                    dd = ddoc.to_dict() or {}
                    total_despesas += float(dd.get("total_saida", 0) or 0)
                    total_receitas += float(dd.get("total_entrada", 0) or 0)
                    total_ajustes += float(dd.get("total_ajuste", 0) or 0)
                    total_estornos += float(dd.get("total_estorno", 0) or 0)
                    dt_cur += timedelta(days=1)
                saldo_real = None
                try:
                    dtf = datetime.strptime(dt_fim, "%Y-%m-%d")
                    mes_f = dtf.strftime("%Y-%m")
                    sr = 0.0
                    for mdoc3 in root.collection('meses').stream():
                        mid = str(mdoc3.id or "")
                        if mid and mid < mes_f:
                            mo = mdoc3.to_dict() or {}
                            try:
                                v = float(mo.get("saldo_mes")) if mo.get("saldo_mes") is not None else (
                                    float(mo.get("total_entrada", 0) or 0) - float(mo.get("total_saida", 0) or 0) + float(mo.get("total_ajuste", 0) or 0)
                                )
                            except:
                                v = (float(mo.get("total_entrada", 0) or 0) - float(mo.get("total_saida", 0) or 0) + float(mo.get("total_ajuste", 0) or 0))
                            sr += float(v or 0)
                    base = datetime.strptime(f"{mes_f}-01", "%Y-%m-%d")
                    cur2 = base
                    while cur2 <= dtf:
                        k = cur2.strftime("%Y-%m-%d")
                        try:
                            dd2 = root.collection('dias').document(k).get().to_dict() or {}
                            sr += float(dd2.get("total_entrada", 0) or 0) - float(dd2.get("total_saida", 0) or 0) + float(dd2.get("total_ajuste", 0) or 0)
                        except:
                            pass
                        cur2 = cur2 + timedelta(days=1)
                    saldo_real = float(sr or 0)
                except:
                    saldo_real = None
            else:
                # Sem intervalo: somar todos os meses do cliente
                sr = 0.0
                sr_count = 0
                for m in root.collection('meses').stream():
                    mm = m.to_dict() or {}
                    total_despesas += float(mm.get("total_saida", 0) or 0)
                    total_receitas += float(mm.get("total_entrada", 0) or 0)
                    total_ajustes += float(mm.get("total_ajuste", 0) or 0)
                    total_estornos += float(mm.get("total_estorno", 0) or 0)
                    try:
                        v = float(mm.get("saldo_mes")) if mm.get("saldo_mes") is not None else (
                            float(mm.get("total_entrada", 0) or 0) - float(mm.get("total_saida", 0) or 0) + float(mm.get("total_ajuste", 0) or 0)
                        )
                    except:
                        v = (float(mm.get("total_entrada", 0) or 0) - float(mm.get("total_saida", 0) or 0) + float(mm.get("total_ajuste", 0) or 0))
                    sr += float(v or 0)
                    sr_count += 1
                try:
                    base_root = root.get().to_dict() or {}
                    saldo_real_root = float(base_root.get("saldo_real")) if base_root.get("saldo_real") is not None else None
                except:
                    saldo_real_root = None
                try:
                    saldo_real = float(sr) if sr_count > 0 else (saldo_real_root if saldo_real_root is not None else None)
                except:
                    saldo_real = saldo_real_root
            saldo = total_receitas - total_despesas + total_ajustes
            if mes:
                try:
                    pass
                except:
                    pass
            if dt_ini and dt_fim and saldo_real is None:
                saldo_real = saldo
            if not mes and not dt_ini and not dt_fim:
                if saldo_real is None:
                    saldo_real = saldo
            return jsonify({
                "sucesso": True,
                "filtros": {
                    "inicio": dt_ini,
                    "fim": dt_fim,
                    "mes": mes,
                    "categorias": cats or [],
                    "tipo": tipo_filter,
                    "cliente_id": cliente_id,
                },
                "total": {
                    "despesas": total_despesas,
                    "receitas": total_receitas,
                    "saldo": saldo,
                    "saldo_real": float(saldo_real if saldo_real is not None else saldo),
                    "estornos": total_estornos,
                    "ajustes": total_ajustes
                }
            })
        if group_by == 'categoria' and mes:
            mdoc = root.collection('meses').document(mes).get()
            mm = mdoc.to_dict() or {}
            cat_exp = dict(mm.get("categorias_saida", {}) or {})
            cat_inc = dict(mm.get("categorias_entrada", {}) or {})
            cat_est = dict(mm.get("categorias_estorno", {}) or {})
            recalc = False
            try:
                mm_total_saida = float(mm.get("total_saida", 0) or 0)
                mm_total_entrada = float(mm.get("total_entrada", 0) or 0)
                sum_cat_exp = sum(float(v or 0) for v in dict(cat_exp or {}).values())
                sum_cat_inc = sum(float(v or 0) for v in dict(cat_inc or {}).values())
                if abs(sum_cat_exp - mm_total_saida) > 1e-6 or abs(sum_cat_inc - mm_total_entrada) > 1e-6:
                    recalc = True
            except:
                recalc = False
            if (not cat_exp and not cat_inc and not cat_est) or recalc:
                try:
                    ano, m = mes.split("-")
                    dt_ini = f"{ano}-{m}-01"
                    if m == "12":
                        dt_fim = f"{int(ano)+1}-01-01"
                    else:
                        dt_fim = f"{ano}-{int(m)+1:02d}-01"
                    dt_cur = datetime.strptime(dt_ini, "%Y-%m-%d")
                    dt_end = datetime.strptime(dt_fim, "%Y-%m-%d")
                    while dt_cur < dt_end:
                        dkey = dt_cur.strftime("%Y-%m-%d")
                        try:
                            ddoc = root.collection('dias').document(dkey).get()
                            dd = ddoc.to_dict() or {}
                        except:
                            dd = {}
                        try:
                            for k, v in dict(dd.get("categorias_saida", {}) or {}).items():
                                cat_exp[k] = float(cat_exp.get(k, 0) or 0) + float(v or 0)
                            for k, v in dict(dd.get("categorias_entrada", {}) or {}).items():
                                cat_inc[k] = float(cat_inc.get(k, 0) or 0) + float(v or 0)
                            for k, v in dict(dd.get("categorias_estorno", {}) or {}).items():
                                cat_est[k] = float(cat_est.get(k, 0) or 0) + float(v or 0)
                        except:
                            pass
                        dt_cur = dt_cur + timedelta(days=1)
                    try:
                        mdoc_set = root.collection('meses').document(mes)
                        mdoc_set.set({
                            "categorias_saida": {k: float(v or 0) for k, v in cat_exp.items()},
                            "categorias_entrada": {k: float(v or 0) for k, v in cat_inc.items()},
                            "categorias_estorno": {k: float(v or 0) for k, v in cat_est.items()},
                            "atualizado_em": firestore.SERVER_TIMESTAMP,
                        }, merge=True)
                    except:
                        pass
                except:
                    pass
            need_stream = False
            try:
                mm_total_saida = float(mm.get("total_saida", 0) or 0)
                mm_total_entrada = float(mm.get("total_entrada", 0) or 0)
                sum_cat_exp = sum(float(v or 0) for v in dict(cat_exp or {}).values())
                sum_cat_inc = sum(float(v or 0) for v in dict(cat_inc or {}).values())
                if abs(sum_cat_exp - mm_total_saida) > 1e-6 or abs(sum_cat_inc - mm_total_entrada) > 1e-6:
                    need_stream = True
            except:
                need_stream = False
            if need_stream:
                try:
                    ano, m = mes.split("-")
                    dt_ini = f"{ano}-{m}-01"
                    if m == "12":
                        dt_fim = f"{int(ano)+1}-01-01"
                    else:
                        dt_fim = f"{ano}-{int(m)+1:02d}-01"
                    dt_cur = datetime.strptime(dt_ini, "%Y-%m-%d")
                    dt_end = datetime.strptime(dt_fim, "%Y-%m-%d")
                    cat_exp = {}
                    cat_inc = {}
                    cat_est = {}
                    while dt_cur < dt_end:
                        dkey = dt_cur.strftime("%Y-%m-%d")
                        try:
                            dd = root.collection('dias').document(dkey).get().to_dict() or {}
                        except:
                            dd = {}
                        try:
                            for k, v in dict(dd.get("categorias_saida", {}) or {}).items():
                                cat_exp[k] = float(cat_exp.get(k, 0) or 0) + float(v or 0)
                            for k, v in dict(dd.get("categorias_entrada", {}) or {}).items():
                                cat_inc[k] = float(cat_inc.get(k, 0) or 0) + float(v or 0)
                            for k, v in dict(dd.get("categorias_estorno", {}) or {}).items():
                                cat_est[k] = float(cat_est.get(k, 0) or 0) + float(v or 0)
                        except:
                            pass
                        dt_cur = dt_cur + timedelta(days=1)
                    try:
                        mdoc_set2 = root.collection('meses').document(mes)
                        mdoc_set2.set({
                            "categorias_saida": {k: float(v or 0) for k, v in cat_exp.items()},
                            "categorias_entrada": {k: float(v or 0) for k, v in cat_inc.items()},
                            "categorias_estorno": {k: float(v or 0) for k, v in cat_est.items()},
                            "atualizado_em": firestore.SERVER_TIMESTAMP,
                        }, merge=True)
                    except:
                        pass
                except:
                    pass
            if cats:
                cat_exp = {k: v for k, v in cat_exp.items() if k in cats}
                cat_inc = {k: v for k, v in cat_inc.items() if k in cats}
                cat_est = {k: v for k, v in cat_est.items() if k in cats}
            try:
                cat_exp = {k: float(v or 0) - float(cat_est.get(k, 0) or 0) for k, v in cat_exp.items()}
                cat_exp = {k: float(v or 0) for k, v in cat_exp.items() if float(v or 0) > 0}
            except:
                pass
            total_despesas = 0.0
            total_receitas = 0.0
            if not tipo_filter or tipo_filter == 'saida':
                total_despesas = sum(float(v or 0) for v in cat_exp.values())
            if not tipo_filter or tipo_filter == 'entrada':
                total_receitas = sum(float(v or 0) for v in cat_inc.values())
            total_ajustes = 0.0 if (cats or tipo_filter) else float(mm.get("total_ajuste", 0) or 0)
            total_estornos = sum(float(v or 0) for v in cat_est.values()) if (cats or tipo_filter) else float(mm.get("total_estorno", 0) or 0)
            saldo = total_receitas - total_despesas + total_ajustes
            resp = {
                "sucesso": True,
                "filtros": {
                    "inicio": dt_ini,
                    "fim": dt_fim,
                    "mes": mes,
                    "categorias": cats or [],
                    "tipo": tipo_filter,
                    "cliente_id": cliente_id,
                },
                "total": {
                    "despesas": total_despesas,
                    "receitas": total_receitas,
                    "saldo": saldo,
                    "estornos": total_estornos
                },
                "categorias": {
                    "despesas": cat_exp,
                    "receitas": cat_inc,
                    "estornos": cat_est
                }
            }
            return jsonify(resp)
        # Caminho com agrupamento por categoria ou filtros → stream de transações do cliente
        # Preparar janela de consulta
        tcoll = root.collection('transacoes')
        q = None
        if mes:
            ano, m = mes.split("-")
            q_ini = f"{ano}-{m}-01"
            if m == "12":
                q_fim = f"{int(ano)+1}-01-01"
            else:
                q_fim = f"{ano}-{int(m)+1:02d}-01"
            q = tcoll.where('data_referencia', '>=', q_ini).where('data_referencia', '<', q_fim)
        elif dt_ini and dt_fim:
            q = tcoll.where('data_referencia', '>=', dt_ini).where('data_referencia', '<=', dt_fim)
        else:
            q = tcoll
        cat_agg_exp = {}
        cat_agg_inc = {}
        for d in q.stream():
            t = d.to_dict() or {}
            if t.get('estornado'):
                continue
            tp_raw = str(t.get('tipo', '')).strip().lower()
            # normalizar tipo para 'saida'/'entrada'
            if tp_raw in ('0', 'despesa'):
                tp = 'saida'
            elif tp_raw in ('1', 'receita'):
                tp = 'entrada'
            else:
                tp = tp_raw
            val = float(t.get('valor', 0) or 0)
            cat = str(t.get('categoria', 'outros') or 'outros').strip().lower()
            if cats and cat not in cats:
                continue
            if tipo_filter and tp != tipo_filter:
                continue
            if tp == 'saida':
                total_despesas += val
                if group_by == 'categoria':
                    cat_agg_exp[cat] = cat_agg_exp.get(cat, 0.0) + val
            elif tp == 'entrada':
                total_receitas += val
                if group_by == 'categoria':
                    cat_agg_inc[cat] = cat_agg_inc.get(cat, 0.0) + val
            elif tp == 'ajuste':
                total_ajustes += val
            elif tp == 'estorno':
                total_estornos += val
        saldo = total_receitas - total_despesas + total_ajustes
        resp = {
            "sucesso": True,
            "filtros": {
                "inicio": dt_ini,
                "fim": dt_fim,
                "mes": mes,
                "categorias": cats or [],
                "tipo": tipo_filter,
                "cliente_id": cliente_id,
            },
            "total": {
                "despesas": total_despesas,
                "receitas": total_receitas,
                "saldo": saldo,
                "estornos": total_estornos,
                "ajustes": total_ajustes
            }
        }
        if group_by == 'categoria':
            resp["categorias"] = {
                "despesas": cat_agg_exp,
                "receitas": cat_agg_inc
            }
        return jsonify(resp)
    except Exception as e:
        return jsonify({"sucesso": False, "erro": str(e)}), 500
@app.route('/health', methods=['GET'])
def health():
    """Endpoint de saúde da API."""
    return jsonify({
        "status": "online",
        "timestamp": _now_sp().isoformat(),
        "servico": "API Financeira"
    })
@app.route('/health/consistency', methods=['GET'])
def health_consistency():
    hoje = _day_key_sp()
    mes_atual = _month_key_sp()
    try:
        db = get_db()
        cliente_id = str(request.args.get("cliente_id") or "default")
        cliente_nome = request.args.get("cliente_nome")
        cliente_username = request.args.get("username")
        try:
            ensure_cliente(cliente_id, nome=cliente_nome, username=cliente_username)
        except:
            pass
        root = db.collection('clientes').document(cliente_id)
        ddoc = root.collection('dias').document(hoje).get()
        dd = ddoc.to_dict() or {}
        dia_ag = {
            "despesas": float(dd.get("total_saida", 0) or 0),
            "receitas": float(dd.get("total_entrada", 0) or 0),
            "ajustes": float(dd.get("total_ajuste", 0) or 0),
            "estornos": float(dd.get("total_estorno", 0) or 0),
        }
        dia_ag["saldo"] = dia_ag["receitas"] - dia_ag["despesas"] + dia_ag["ajustes"]
        qtd_validas_ag = int(dd.get("quantidade_transacoes_validas", dd.get("quantidade_transacoes", 0)) or 0)
        cnt_stream = 0
        try:
            items = []
            tops = []
            try:
                items = root.collection('transacoes').document(hoje).collection('items').stream()
            except:
                items = []
            try:
                tops = root.collection('transacoes').where('data_referencia', '==', hoje).stream()
            except:
                tops = []
            idx = {}
            for it in items:
                o = it.to_dict() or {}
                k = str(o.get('ref_id') or '') or (str(o.get('tipo', '')) + '|' + str(float(o.get('valor', 0) or 0)) + '|' + str(o.get('categoria', '')) + '|' + str(o.get('descricao', '')) + '|' + str(o.get('timestamp_criacao', '')))
                if idx.get(k):
                    continue
                idx[k] = 1
                if o.get('estornado'):
                    continue
                tp = str(o.get('tipo', '')).strip().lower()
                if tp in ('entrada', '1', 'receita') or tp in ('saida', '0', 'despesa') or tp in ('ajuste',):
                    cnt_stream += 1
            for it in tops:
                o = it.to_dict() or {}
                k = str(o.get('ref_id') or '') or (str(o.get('tipo', '')) + '|' + str(float(o.get('valor', 0) or 0)) + '|' + str(o.get('categoria', '')) + '|' + str(o.get('descricao', '')) + '|' + str(o.get('timestamp_criacao', '')))
                if idx.get(k):
                    continue
                idx[k] = 1
                if o.get('estornado'):
                    continue
                tp = str(o.get('tipo', '')).strip().lower()
                if tp in ('entrada', '1', 'receita') or tp in ('saida', '0', 'despesa') or tp in ('ajuste',):
                    cnt_stream += 1
        except:
            cnt_stream = 0
        mdoc = root.collection('meses').document(mes_atual).get()
        mm = mdoc.to_dict() or {}
        mes_ag = {
            "despesas": float(mm.get("total_saida", 0) or 0),
            "receitas": float(mm.get("total_entrada", 0) or 0),
            "ajustes": float(mm.get("total_ajuste", 0) or 0),
            "estornos": float(mm.get("total_estorno", 0) or 0),
        }
        mes_ag["saldo"] = mes_ag["receitas"] - mes_ag["despesas"] + mes_ag["ajustes"]
        qtd_validas_mesdoc = int(mm.get("quantidade_transacoes_validas", mm.get("quantidade_transacoes", 0)) or 0)
        td = tr = taj = tes = 0.0
        qtd_validas_dias = 0
        try:
            ano, mes = mes_atual.split("-")
            dt_ini = f"{ano}-{mes}-01"
            if mes == "12":
                dt_fim = f"{int(ano)+1}-01-01"
            else:
                dt_fim = f"{ano}-{int(mes)+1:02d}-01"
            cur = datetime.strptime(dt_ini, "%Y-%m-%d")
            end = datetime.strptime(dt_fim, "%Y-%m-%d")
            while cur < end:
                dkey = cur.strftime("%Y-%m-%d")
                try:
                    dd2 = root.collection('dias').document(dkey).get().to_dict() or {}
                    td += float(dd2.get("total_saida", 0) or 0)
                    tr += float(dd2.get("total_entrada", 0) or 0)
                    taj += float(dd2.get("total_ajuste", 0) or 0)
                    tes += float(dd2.get("total_estorno", 0) or 0)
                    if "quantidade_transacoes_validas" in dd2:
                        qtd_validas_dias += int(dd2.get("quantidade_transacoes_validas", 0) or 0)
                    else:
                        try:
                            items = root.collection('transacoes').document(dkey).collection('items').stream()
                        except:
                            items = []
                        cnt = 0
                        for it in items:
                            o = it.to_dict() or {}
                            tp = str(o.get('tipo', '')).strip().lower()
                            if tp in ('entrada', '1', 'receita') or tp in ('saida', '0', 'despesa') or tp in ('ajuste',):
                                if not bool(o.get('estornado', False)):
                                    cnt += 1
                        qtd_validas_dias += cnt
                except:
                    pass
                cur = cur + timedelta(days=1)
        except:
            pass
        mes_dias = {
            "despesas": td,
            "receitas": tr,
            "ajustes": taj,
            "estornos": tes,
        }
        mes_dias["saldo"] = mes_dias["receitas"] - mes_dias["despesas"] + mes_dias["ajustes"]
        resp = {
            "sucesso": True,
            "cliente_id": cliente_id,
            "dia": {
                "agregado": {**dia_ag, "quantidade_transacoes_validas": int(qtd_validas_ag)},
                "stream_count_validas": int(cnt_stream),
                "consistente": (int(qtd_validas_ag) == int(cnt_stream)),
            },
            "mes": {
                "agregado": {**mes_ag, "quantidade_transacoes_validas": int(qtd_validas_mesdoc)},
                "soma_dias": {**mes_dias, "quantidade_transacoes_validas": int(qtd_validas_dias)},
                "consistente_totais": (
                    abs(mes_ag["despesas"] - mes_dias["despesas"]) < 1e-6 and
                    abs(mes_ag["receitas"] - mes_dias["receitas"]) < 1e-6 and
                    abs(mes_ag["ajustes"] - mes_dias["ajustes"]) < 1e-6 and
                    abs(mes_ag["estornos"] - mes_dias["estornos"]) < 1e-6 and
                    abs(mes_ag["saldo"] - mes_dias["saldo"]) < 1e-6
                ),
                "consistente_qtd": (int(qtd_validas_mesdoc) == int(qtd_validas_dias))
            }
        }
        return jsonify(resp)
    except Exception as e:
        return jsonify({"sucesso": False, "erro": str(e)}), 500

if __name__ == '__main__':
    print("🚀 API Financeira iniciada!")
    print("📡 Endpoints disponíveis:")
    print("   POST /processar - Processa transações")
    print("   GET  /extrato/hoje - Extrato do dia")
    print("   GET  /total/mes - Totais do mês")
    print("   GET  /total/semana - Totais da semana")
    print("   GET  /total/geral - Totais gerais (todas transações)")
    print("   GET  /categorias/mes - Gastos por categoria")
    print("   GET  /health - Status da API")
    print(f"\n🔗 URL: http://{API_HOST}:{API_PORT}")
    app.run(debug=True, host=API_HOST, port=API_PORT, use_reloader=False)
