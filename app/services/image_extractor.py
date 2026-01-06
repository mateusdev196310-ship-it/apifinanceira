import json
import re
import time
import io
import base64
import requests
from PIL import Image
import numpy as np
from datetime import datetime
from app.services.gemini import get_client
from google.genai import types
from app.services.extractor import extrair_informacoes_financeiras


def _infer_fields(tl: str, tp: str):
    s = (tl or "").lower()
    metodo = ""
    if "pix" in s:
        metodo = "pix"
    elif re.search(r"transfer", s, re.IGNORECASE):
        metodo = "transferencia"
    elif re.search(r"cart[ãa]o|cr[eé]dito|d[ée]bito", s, re.IGNORECASE):
        metodo = "cartao"
    elif "boleto" in s:
        metodo = "boleto"
    elif re.search(r"dinheiro|esp[eé]cie", s, re.IGNORECASE):
        metodo = "dinheiro"
    est = ""
    m = re.search(r"\b(mercado|supermercado|farmacia|farmácia|restaurante|padaria|loja|posto)\b\s+([a-z0-9\u00c0-\u017f][a-z0-9\u00c0-\u017f\s]{0,30})", tl or "", re.IGNORECASE)
    if m:
        est = f"{m.group(1)} {m.group(2)}".strip()
    rec = ""
    if str(tp) == "0":
        rec = est or ""
        if not rec:
            mm = re.search(r"(favorecido|benefici[áa]rio|recebedor|destinat[áa]rio)[:\s]+([A-Za-z\u00c0-\u017f][A-Za-z0-9\u00c0-\u017f\s]{2,30})", tl or "", re.IGNORECASE)
            if mm:
                rec = mm.group(2).strip()
    else:
        mm = re.search(r"(?:de|do|da)\s+([A-Za-z\u00c0-\u017f][A-Za-z0-9\u00c0-\u017f\s]{2,30})", tl or "", re.IGNORECASE)
        if mm and not re.search(r"sal[áa]ri|salario|servi[çc]o|mercado|supermercado", mm.group(1), re.IGNORECASE):
            rec = mm.group(1).strip()
        elif re.search(r"\b(cliente|pagador|remetente)\b", tl or "", re.IGNORECASE):
            rec = "cliente"
        elif re.search(r"\bsal[áa]ri\w*\b|\bfolha\b", tl or "", re.IGNORECASE):
            rec = "empregador"
    dt = ""
    iso = re.search(r"\b(\d{4})[-/\.](\d{2})[-/\.](\d{2})\b", tl or "")
    if iso:
        try:
            a = int(iso.group(1))
            mth = int(iso.group(2))
            d = int(iso.group(3))
            dt = f"{a:04d}-{mth:02d}-{d:02d}"
        except:
            dt = ""
    if not dt:
        dmy = re.search(r"\b(\d{2})[./-](\d{2})[./-](\d{4})\b", tl or "")
        if dmy:
            try:
                d = int(dmy.group(1))
                mth = int(dmy.group(2))
                a = int(dmy.group(3))
                dt = f"{a:04d}-{mth:02d}-{d:02d}"
            except:
                dt = ""
    if not dt:
        dmy2 = re.search(r"\b(\d{2})[./-](\d{2})[./-](\d{2})\b", tl or "")
        if dmy2:
            try:
                d = int(dmy2.group(1))
                mth = int(dmy2.group(2))
                yy = int(dmy2.group(3))
                a = 2000 + yy if yy < 70 else 1900 + yy
                dt = f"{a:04d}-{mth:02d}-{d:02d}"
            except:
                dt = ""
    return {
        "metodo_pagamento": metodo,
        "estabelecimento": est,
        "recebedor": rec,
        "data_transacao": dt,
    }

def _detect_mime(data: bytes) -> str:
    if not data:
        return "image/jpeg"
    try:
        if data.startswith(b"\xff\xd8"):
            return "image/jpeg"
        if data.startswith(b"\x89PNG\r\n\x1a\n"):
            return "image/png"
        if data.startswith(b"GIF87a") or data.startswith(b"GIF89a"):
            return "image/gif"
    except:
        pass
    return "image/jpeg"

def extrair_informacoes_da_imagem(image_bytes: bytes, transcrito_override: str = ""):
    client = get_client()
    if client is None:
        client = None
    try:
        try:
            print("[image_extractor] start")
        except:
            pass
        def _contents(prompt_text: str, blob_obj=None):
            parts = [types.Part(text=prompt_text)]
            if blob_obj is not None:
                parts.append(types.Part(inline_data=blob_obj))
            return [types.Content(role='user', parts=parts)]
        dados = image_bytes or b""
        mime = _detect_mime(dados)
        blob = types.Blob(mime_type=mime, data=dados)
        contents_ocr = _contents("Transcreva apenas o texto do comprovante/recibo. Retorne somente o texto puro.", blob)
        transcrito = (transcrito_override or "").strip()
        if not transcrito:
            try:
                if client is not None:
                    try:
                        print("[image_extractor] before_ocr")
                    except:
                        pass
                    try:
                        ocr = client.models.generate_content(
                            model='gemini-2.5-flash',
                            contents=contents_ocr,
                            config=types.GenerateContentConfig(
                                temperature=0.0,
                                max_output_tokens=800,
                            ),
                        )
                        transcrito = (ocr.text or "").strip()
                    except Exception as e:
                        msg = str(e) if e else ""
                        if "RESOURCE_EXHAUSTED" in msg or "429" in msg:
                            try:
                                time.sleep(5)
                                ocr = client.models.generate_content(
                                    model='gemini-2.5-flash',
                                    contents=contents_ocr,
                                    config=types.GenerateContentConfig(
                                        temperature=0.0,
                                        max_output_tokens=800,
                                    ),
                                )
                                transcrito = (ocr.text or "").strip()
                            except Exception:
                                transcrito = ""
                        else:
                            transcrito = ""
            except:
                transcrito = ""
        if not transcrito:
            try:
                img = Image.open(io.BytesIO(dados)).convert("RGB")
                arr = np.array(img)
                import easyocr
                reader = easyocr.Reader(['pt', 'en'], gpu=False)
                res = reader.readtext(arr, detail=0)
                transcrito = "\n".join([str(x) for x in res if str(x).strip()])
            except:
                transcrito = ""
        if not transcrito:
            try:
                b64 = base64.b64encode(dados).decode('ascii')
                mime_hdr = f"data:{mime};base64,{b64}"
                r = requests.post(
                    "https://api.ocr.space/parse/image",
                    data={
                        "apikey": "helloworld",
                        "base64Image": mime_hdr,
                        "language": "por",
                        "isOverlayRequired": "false",
                        "scale": "true",
                        "OCREngine": "2",
                    },
                    timeout=20
                )
                if r.ok:
                    j = r.json()
                    if j.get("IsErroredOnProcessing"):
                        try:
                            img = Image.open(io.BytesIO(dados)).convert("RGB")
                            buf = io.BytesIO()
                            img.thumbnail((1280, 1280))
                            img.save(buf, format="JPEG", quality=60, optimize=True)
                            small = buf.getvalue()
                            b64s = base64.b64encode(small).decode('ascii')
                            mime_hdr = f"data:image/jpeg;base64,{b64s}"
                            r2 = requests.post(
                                "https://api.ocr.space/parse/image",
                                data={
                                    "apikey": "helloworld",
                                    "base64Image": mime_hdr,
                                    "language": "por",
                                    "isOverlayRequired": "false",
                                    "scale": "true",
                                    "OCREngine": "2",
                                },
                                timeout=20
                            )
                            if r2.ok:
                                j2 = r2.json()
                                pr2 = j2.get("ParsedResults") or []
                                if pr2:
                                    transcrito = (pr2[0].get("ParsedText") or "").strip()
                            try:
                                print("[image_extractor] ocr_space_resized", len(transcrito))
                            except:
                                pass
                        except:
                            pass
                    else:
                        pr = j.get("ParsedResults") or []
                        if pr:
                            transcrito = (pr[0].get("ParsedText") or "").strip()
                        if not transcrito:
                            try:
                                img = Image.open(io.BytesIO(dados)).convert("RGB")
                                buf = io.BytesIO()
                                img.thumbnail((1280, 1280))
                                img.save(buf, format="JPEG", quality=60, optimize=True)
                                small = buf.getvalue()
                                b64s = base64.b64encode(small).decode('ascii')
                                mime_hdr = f"data:image/jpeg;base64,{b64s}"
                                r2 = requests.post(
                                    "https://api.ocr.space/parse/image",
                                    data={
                                        "apikey": "helloworld",
                                        "base64Image": mime_hdr,
                                        "language": "por",
                                        "isOverlayRequired": "false",
                                        "scale": "true",
                                        "OCREngine": "2",
                                    },
                                    timeout=20
                                )
                                if r2.ok:
                                    j2 = r2.json()
                                    pr2 = j2.get("ParsedResults") or []
                                    if pr2:
                                        transcrito = (pr2[0].get("ParsedText") or "").strip()
                                try:
                                    print("[image_extractor] ocr_space_resized", len(transcrito))
                                except:
                                    pass
                            except:
                                pass
                    try:
                        print("[image_extractor] ocr_space", len(transcrito))
                    except:
                        pass
            except:
                transcrito = transcrito or ""
        try:
            print(f"[image_extractor] ocr_len={len(transcrito)}")
        except:
            pass
        if not transcrito:
            if client is not None:
                try:
                    from app.constants.categories import CATEGORY_LIST
                    cat_list = ", ".join(CATEGORY_LIST)
                    prompt_img = (
                        "Analise a imagem (comprovante/recibo/extrato) e identifique TODAS as transações financeiras.\n"
                        "Retorne SOMENTE um ARRAY JSON onde cada item tenha:\n"
                        "[\n"
                        "  {\n"
                        '    "tipo": "0" para despesa ou "1" para receita,\n'
                        '    "valor": número decimal,\n'
                        f'    "categoria": uma de [{cat_list}],\n'
                        '    "descricao": frase profissional breve (3–8 palavras),\n'
                        '    "moeda": "BRL"\n'
                        "  }\n"
                        "]\n"
                        "Regras: use contexto real para categoria e descrição; evite 'outros' ao máximo (use 'duvida' só sem pistas); padronize nomes; reformule termos informais.\n"
                        "Exemplos: 'feira' → categoria 'alimentacao', descrição 'Feira'; 'supermercado' → categoria 'alimentacao', descrição 'Supermercado'; 'mandei pro cara do uber' → categoria 'transporte', descrição 'Transporte por Aplicativo'.\n"
                    )
                    contents_img = _contents(prompt_img, blob)
                    try:
                        map_img = client.models.generate_content(
                            model='gemini-1.5-flash',
                            contents=contents_img,
                            config=types.GenerateContentConfig(
                                response_mime_type="application/json",
                                temperature=0.0,
                                max_output_tokens=800,
                            ),
                        )
                    except Exception:
                        map_img = client.models.generate_content(
                            model='gemini-2.5-flash',
                            contents=contents_img,
                            config=types.GenerateContentConfig(
                                response_mime_type="application/json",
                                temperature=0.0,
                                max_output_tokens=800,
                            ),
                        )
                    texto_img = (map_img.text or "").strip()
                    try:
                        dados_img = json.loads(texto_img)
                    except:
                        try:
                            txti = texto_img.replace('```json', '').replace('```', '').strip()
                            mi = re.search(r'\[\s*\{[\s\S]*?\}\s*\]', txti)
                            dados_img = json.loads(mi.group(0)) if mi else []
                        except:
                            dados_img = []
                    if isinstance(dados_img, dict):
                        dados_img = [dados_img]
                    if isinstance(dados_img, list) and dados_img:
                        out_img = []
                        for item in dados_img:
                            try:
                                tipo = '0' if str(item.get('tipo')).strip() == '0' else ('1' if str(item.get('tipo')).strip() == '1' else '0')
                                valor = float(item.get('valor', 0) or 0)
                                cat = str(item.get('categoria', 'outros')).strip().lower() or 'outros'
                                desc_raw = str(item.get('descricao', '')).strip()
                                fctx = _infer_fields(transcrito, tipo)
                                out_img.append({
                                    "tipo": tipo,
                                    "valor": valor,
                                    "categoria": cat,
                                    "descricao": desc_raw,
                                    "moeda": "BRL",
                                    "metodo_pagamento": fctx.get("metodo_pagamento", ""),
                                    "estabelecimento": fctx.get("estabelecimento", ""),
                                    "recebedor": fctx.get("recebedor", ""),
                                    "data_transacao": fctx.get("data_transacao", ""),
                                    "pendente_confirmacao": (cat in ("outros", "duvida")),
                                    "confidence_score": float(0.6 if cat in ("outros", "duvida") else 0.9),
                                })
                            except:
                                continue
                        if out_img:
                            return out_img
                except:
                    pass
        prompt_json = (
            "Com base no TEXTO transcrito do comprovante abaixo, extraia transações financeiras e retorne SOMENTE um ARRAY JSON:\n"
            "[\n"
            "  {\n"
            '    "tipo": "0" para despesa ou "1" para receita,\n'
            '    "valor": número decimal,\n'
            f'    "categoria": uma de [{", ".join(__import__("app.constants.categories", fromlist=["CATEGORY_LIST"]).CATEGORY_LIST)}],\n'
            '    "descricao": frase profissional breve (3–8 palavras),\n'
            '    "moeda": "BRL"\n'
            "  }\n"
            "]\n"
            "Regras: evite 'outros' ao máximo; use 'duvida' só sem pistas; padronize descrições; reformule termos informais.\n"
            "Exemplos: 'feira' → 'alimentacao'/'Feira'; 'supermercado' → 'alimentacao'/'Supermercado'; 'mandei pro cara do uber' → 'transporte'/'Transporte por Aplicativo'.\n"
            "Texto transcrito:\n"
            f"{transcrito}"
        )
        contents_map = _contents(prompt_json, None)
        try:
            map_json = client.models.generate_content(
                model='gemini-1.5-flash',
                contents=contents_map,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    temperature=0.0,
                    max_output_tokens=800,
                ),
            )
        except Exception:
            map_json = client.models.generate_content(
                model='gemini-2.5-flash',
                contents=contents_map,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    temperature=0.0,
                    max_output_tokens=800,
                ),
            )
        texto2 = (map_json.text or "").strip()
        try:
            dados_lista2 = json.loads(texto2)
        except:
            try:
                txt2 = texto2.replace('```json', '').replace('```', '').strip()
                m2 = re.search(r'\[\s*\{[\s\S]*?\}\s*\]', txt2)
                dados_lista2 = json.loads(m2.group(0)) if m2 else []
            except:
                dados_lista2 = []
        if isinstance(dados_lista2, dict):
            dados_lista2 = [dados_lista2]
        if isinstance(dados_lista2, list) and dados_lista2:
            out2 = []
            for item in dados_lista2:
                try:
                    tipo = '0' if str(item.get('tipo')).strip() == '0' else ('1' if str(item.get('tipo')).strip() == '1' else '0')
                    valor = float(item.get('valor', 0) or 0)
                    cat = str(item.get('categoria', 'outros')).strip().lower() or 'outros'
                    desc_raw = str(item.get('descricao', '')).strip()
                    out2.append({
                        "tipo": tipo,
                        "valor": valor,
                        "categoria": cat,
                        "descricao": desc_raw,
                        "moeda": "BRL",
                        "confidence_score": float(0.9 if cat not in ("outros", "duvida") else 0.6),
                        "pendente_confirmacao": (cat in ("outros", "duvida")),
                    })
                except:
                    continue
            if out2:
                return out2
        rb = extrair_informacoes_financeiras(transcrito) or []
        if rb:
            try:
                fctx = _infer_fields(transcrito, str(rb[0].get("tipo", "0")))
            except:
                fctx = {"metodo_pagamento": "", "estabelecimento": "", "recebedor": "", "data_transacao": ""}
            out_rb = []
            for it in rb:
                try:
                    o = dict(it)
                    o.setdefault("metodo_pagamento", fctx.get("metodo_pagamento", ""))
                    o.setdefault("estabelecimento", fctx.get("estabelecimento", ""))
                    o.setdefault("recebedor", fctx.get("recebedor", ""))
                    o.setdefault("data_transacao", fctx.get("data_transacao", ""))
                    cr = str(o.get("categoria", "outros")).strip().lower()
                    if cr in ('duvida', 'outros'):
                        o["pendente_confirmacao"] = True
                        o["confidence_score"] = float(0.6)
                    else:
                        o["pendente_confirmacao"] = False
                        o["confidence_score"] = float(0.9)
                    out_rb.append(o)
                except:
                    continue
            if out_rb:
                return out_rb
        return []
    except:
        return []
