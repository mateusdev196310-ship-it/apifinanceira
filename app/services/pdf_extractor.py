import os
import io
import re
import json
from typing import List, Dict, Optional, Tuple, Union
from app.services.rule_based import parse_value, detect_category_with_confidence, clean_desc, naturalize_description
from app.services.deepseek import generate_json as ds_generate
from app.services.gemini import get_client
from google.genai import types
import numpy as np

def _read_file_bytes(path: str) -> bytes:
    with open(path, "rb") as f:
        return f.read()

def _try_pypdf_text(path: Optional[str] = None, data: Optional[bytes] = None, max_pages: int = 200) -> str:
    try:
        import pypdf
        if path:
            reader = pypdf.PdfReader(path)
        else:
            reader = pypdf.PdfReader(io.BytesIO(data or b""))
        out = []
        n = min(len(reader.pages), max_pages)
        for i in range(n):
            try:
                t = reader.pages[i].extract_text() or ""
                out.append(t)
            except:
                continue
        return "\n\n".join(out).strip()
    except:
        pass
    try:
        import PyPDF2
        reader = PyPDF2.PdfReader(path or io.BytesIO(data or b""))
        out = []
        n = min(len(reader.pages), max_pages)
        for i in range(n):
            try:
                t = reader.pages[i].extract_text() or ""
                out.append(t)
            except:
                continue
        return "\n\n".join(out).strip()
    except:
        return ""

def _try_pdfminer_text(path: Optional[str] = None, data: Optional[bytes] = None) -> str:
    try:
        from pdfminer.high_level import extract_text
        if path:
            return (extract_text(path) or "").strip()
        else:
            fp = io.BytesIO(data or b"")
            return (extract_text(fp) or "").strip()
    except:
        return ""

def _try_pymupdf_text(path: Optional[str] = None, data: Optional[bytes] = None, max_pages: int = 200) -> str:
    try:
        import fitz
        if path:
            doc = fitz.open(path)
        else:
            doc = fitz.open(stream=(data or b""), filetype="pdf")
        out = []
        n = min(doc.page_count, max_pages)
        for i in range(n):
            try:
                page = doc.load_page(i)
                t = page.get_text("text") or ""
                out.append(t)
            except:
                continue
        doc.close()
        return "\n\n".join(out).strip()
    except:
        return ""

def _try_pil_ocr_text(path: Optional[str] = None, data: Optional[bytes] = None, max_pages: int = 20) -> str:
    try:
        from PIL import Image, ImageSequence
        import easyocr
        reader = easyocr.Reader(lang_list=['pt', 'en'], gpu=False)
        if path:
            im = Image.open(path)
        else:
            im = Image.open(io.BytesIO(data or b""))
        out = []
        count = 0
        for frame in ImageSequence.Iterator(im):
            if count >= max_pages:
                break
            img = frame.convert("RGB")
            arr = np.array(img)
            try:
                rs = reader.readtext(arr, detail=0, paragraph=True)
                out.append("\n".join(rs))
            except:
                pass
            count += 1
        return "\n\n".join(out).strip()
    except:
        return ""

def _clean_pdf_text(raw: str) -> str:
    t = raw or ""
    t = re.sub(r'[^\S\r\n]+', ' ', t)
    t = re.sub(r'[•·▪●■□◇◆◦]+', ' ', t)
    t = re.sub(r'[_]{2,}', ' ', t)
    t = re.sub(r'\s{2,}', ' ', t)
    t = re.sub(r'(\n\s*){2,}', '\n', t)
    t = re.sub(r'\u00A0', ' ', t)
    t = re.sub(r'(?i)\b(p[aá]gina|page)\s+\d+\b', '', t)
    t = re.sub(r'(?i)\b(cnpj|cpf|endere[cç]o|emitente|nota fiscal|nf-e)\b.*', '', t)
    return t.strip()

def extract_text_from_pdf(path: Optional[str] = None, data: Optional[bytes] = None) -> str:
    pipelines = [
        lambda: _try_pypdf_text(path=path, data=data),
        lambda: _try_pdfminer_text(path=path, data=data),
        lambda: _try_pymupdf_text(path=path, data=data),
        lambda: _try_pil_ocr_text(path=path, data=data),
    ]
    for fn in pipelines:
        try:
            txt = fn() or ""
            if txt and len(txt) >= 20:
                return _clean_pdf_text(txt)
        except:
            continue
    return _clean_pdf_text("")

_PDF_SKIP_LINE = re.compile(r'(?i)\b(saldo|totais?|limite|fatura|vencimento|vence\s+em|cnpj|cpf|nota\s*fiscal|nf[\- ]?e|emitente|endere[cç]o|endereço|cliente|resumo|inform[aã]coes|informações|parcelamento|juros|multa|tarifas?|encargos?|pagamento m[ií]nimo|total a pagar|saque total|fatura atual|emitido|pagamentos?\\s+e\\s+cr[eé]ditos?|cart[aã]o\\s+visa|visa)\b')
_AMOUNT_RE = re.compile(r'(?:R?\$?\s*)?(\(?-?\s*\)?)?(\d{1,3}(?:[.\s]\d{3})*(?:,\d{2})|\d+[.,]\d{2}|\d+)(?=\D|$)')
_INCOME_HINT = re.compile(r'(?i)\b(recebi|recebido|recebimento|credito|cr[eé]dito|dep[óo]sito|entrada|sal[áa]rio|pix\\s+recebido|transfer[eê]ncia\\s+recebida)\b')
_EXPENSE_HINT = re.compile(r'(?i)\b(d[eé]bito|debito|compra|pagamento|fatura|boleto|saque|tarifa|juros|servi[cç]o|assinatura|mensalidade|lan[cç]amento|pix\\s+enviado|pix\\s+para|transfer[eê]ncia)\b')
def parse_pdf_text_to_transactions(text: str) -> List[Dict]:
    if not text or len(text) < 20:
        return []
    lines = [l.strip() for l in (text or "").splitlines() if l.strip()]
    out = []
    for i, ln in enumerate(lines):
        if _PDF_SKIP_LINE.search(ln):
            continue
        for m in _AMOUNT_RE.finditer(ln):
            raw = m.group(2)
            val = parse_value(raw)
            if val is None:
                continue
            tipo = '0'
            ctx_prev = lines[i - 1] if i > 0 else ""
            ctx_next = lines[i + 1] if (i + 1) < len(lines) else ""
            if _INCOME_HINT.search(ln) or _INCOME_HINT.search(ctx_prev) or _INCOME_HINT.search(ctx_next):
                tipo = '1'
            elif _EXPENSE_HINT.search(ln) or _EXPENSE_HINT.search(ctx_prev) or _EXPENSE_HINT.search(ctx_next):
                tipo = '0'
            tl_san = re.sub(r'assinatura\s+eletr[ôo]nica|assinatura\s+digital', '', ln, flags=re.IGNORECASE)
            tl_san = re.sub(r'\binternet\s+banking\b', '', tl_san, flags=re.IGNORECASE)
            cat, _ = detect_category_with_confidence(clean_desc(tl_san))
            s = (ln or "").lower()
            base = ""
            mm = re.search(r'\b(mercado|supermercado|farmacia|farmácia|restaurante|padaria|loja|posto)\b\s+([a-z0-9\u00c0-\u017f][a-z0-9\u00c0-\u017f\s]{0,20})', s, re.IGNORECASE)
            if mm:
                base = f"{mm.group(1)} {mm.group(2)}".strip()
            else:
                if tipo == '1':
                    if re.search(r'sal[áa]ri', s):
                        base = 'salário'
                    elif re.search(r'vend', s):
                        base = 'vendas'
                    elif re.search(r'\bpix\b', s, re.IGNORECASE) or 'depósito' in s or 'deposito' in s:
                        base = 'pix recebido'
                    else:
                        base = 'receita'
                else:
                    if re.search(r'\bpix\b', s, re.IGNORECASE):
                        base = 'pix'
                    elif re.search(r'\btransfer\w*\b', s, re.IGNORECASE):
                        base = 'transferência'
                    elif re.search(r'\bcart[ãa]o\b|\bcr[eé]dito\b|\bd[ée]bito\b', s, re.IGNORECASE):
                        base = 'cartão'
                    elif re.search(r'\bboleto\b', s, re.IGNORECASE):
                        base = 'boleto'
                    elif re.search(r'\bmercado\b|\bsupermercado\b', s, re.IGNORECASE):
                        base = 'mercado'
                    elif re.search(r'\bfarm[áa]cia\b', s, re.IGNORECASE):
                        base = 'farmácia'
                    elif re.search(r'\brestaurante\b|\bpizza\b|\blanche\b|\bhamburg\w*\b', s, re.IGNORECASE):
                        base = 'restaurante'
                    elif re.search(r'\buber\b', s, re.IGNORECASE):
                        base = 'uber'
                    elif re.search(r'\bgasolina\b|\bcombust[ií]vel\b', s, re.IGNORECASE):
                        base = 'combustível'
                    elif re.search(r'\binternet\b|\bstreaming\b|\bassinatura\b|\btelefonia\b', s, re.IGNORECASE):
                        base = 'internet'
                    elif re.search(r'\baluguel\b|\bcondom[ií]nio\b|\benergia\b|\b[áa]gua\b|\bluz\b', s, re.IGNORECASE):
                        base = 'moradia'
                    else:
                        base = cat or 'outros'
            desc = naturalize_description(tipo, cat, base)
            out.append({
                "tipo": tipo,
                "valor": float(val),
                "categoria": str(cat or 'outros').strip().lower() or 'outros',
                "descricao": desc,
                "moeda": "BRL",
            })
    return out

def _dedup_transacoes(items: List[Dict]) -> List[Dict]:
    d = {}
    for item in items or []:
        tipo_n = str(item.get('tipo')).strip()
        valor_n = float(item.get('valor', 0))
        desc_raw = str(item.get('descricao', ''))
        cat_n = str(item.get('categoria', '')).strip().lower()
        desc_final = re.sub(r'\s+', ' ', desc_raw).strip()
        toks = desc_final.split()
        if len(toks) > 8:
            desc_final = ' '.join(toks[:8])
        k = (tipo_n, valor_n, cat_n)
        cur = d.get(k)
        if cur is None or len(desc_final) < len(str(cur.get('descricao', ''))):
            novo = dict(item)
            novo['descricao'] = desc_final
            novo['categoria'] = cat_n
            d[k] = novo
    return list(d.values())

def _try_gemini_transacoes(texto: str) -> Optional[List[Dict]]:
    if not texto or len(texto) < 20:
        return None
    prompt = (
        "Analise o conteúdo textual extraído do PDF (comprovantes, faturas, extratos) e extraia TODAS as transações financeiras.\n"
        "Retorne um objeto JSON com a chave 'transacoes' contendo uma lista de itens com:\n"
        "{ tipo: '0' para despesa, '1' para receita, valor: número, descricao: frase profissional (3–8 palavras), categoria: uma categoria específica }\n"
        "Diretrizes: evite 'outros' ao máximo; use categorias lógicas; padronize descrições; reformule termos informais.\n"
        "Exemplos: 'feira' → categoria 'alimentacao' e descricao 'Feira'; 'supermercado' → categoria 'alimentacao' e descricao 'Supermercado'; "
        "'mandei pro cara do uber' → categoria 'transporte' e descricao 'Transporte por Aplicativo'.\n"
        "Entrada:\n" + texto[:20000]
    )
    try:
        out = ds_generate(prompt, temperature=0.0, max_tokens=800, timeout=25, system_instruction="Responda apenas JSON com chave 'transacoes'.") or ""
        if not out:
            raise Exception("ds_empty")
        j = None
        try:
            j = json.loads(out)
        except:
            m = re.search(r'\{[\s\S]*\}', out)
            if m:
                try:
                    j = json.loads(m.group(0))
                except:
                    j = None
        if not j or not isinstance(j, dict):
            raise Exception("ds_invalid")
        arr = j.get("transacoes") or j.get("transactions") or []
        if not isinstance(arr, list):
            raise Exception("ds_no_list")
        return arr
    except:
        try:
            cli = get_client()
        except:
            cli = None
        if not cli:
            return None
        try:
            resposta = cli.models.generate_content(
                model="gemini-2.5-flash",
                contents=prompt,
            )
            out = (getattr(resposta, "text", "") or "").strip()
            if not out:
                return None
            j = None
            try:
                j = json.loads(out)
            except:
                m = re.search(r'\{[\s\S]*\}', out)
                if m:
                    try:
                        j = json.loads(m.group(0))
                    except:
                        j = None
            if not j or not isinstance(j, dict):
                return None
            arr = j.get("transacoes") or j.get("transactions") or []
            if not isinstance(arr, list):
                return None
            return arr
        except:
            return None

def extrair_transacoes_de_pdf(path: Optional[str] = None, data: Optional[bytes] = None) -> List[Dict]:
    if path and not os.path.exists(path):
        return []
    if data is None and path:
        data = _read_file_bytes(path)
    txt = extract_text_from_pdf(path=path, data=data)
    trans_gemini = _try_gemini_transacoes(txt)
    if trans_gemini:
        return _dedup_transacoes(trans_gemini)
    rb = parse_pdf_text_to_transactions(txt)
    if rb:
        return _dedup_transacoes(rb)
    return []

_TOT_PATTERNS = [
    re.compile(r'(?i)total\s+a\s+pagar\s*[:\-]?\s*(?:R?\$|\bRM\b)?\s*(\d{1,3}(?:[.\s]\d{3})*(?:,\d{2})|\d+[.,]\d{2})'),
    re.compile(r'(?i)total\s+da\s+fatura\s*[:\-]?\s*(?:R?\$|\bRM\b)?\s*(\d{1,3}(?:[.\s]\d{3})*(?:,\d{2})|\d+[.,]\d{2})'),
]
_PAG_PATTERNS = [
    re.compile(r'(?i)pagamentos?\s+e\s+cr[eé]ditos?\s*[:\-]?\s*(?:R?\$|\bRM\b)?\s*(\d{1,3}(?:[.\s]\d{3})*(?:,\d{2})|\d+[.,]\d{2})'),
    re.compile(r'(?i)pagamento\s+efetuado\s*[:\-]?\s*(?:R?\$|\bRM\b)?\s*(\d{1,3}(?:[.\s]\d{3})*(?:,\d{2})|\d+[.,]\d{2})'),
    re.compile(r'(?i)valor\s+pag(o|o)\s*[:\-]?\s*(?:R?\$|\bRM\b)?\s*(\d{1,3}(?:[.\s]\d{3})*(?:,\d{2})|\d+[.,]\d{2})')
]
_VENC_PATTERNS = [
    re.compile(r'(?i)vence\s+em\s+(\d{2}[./-]\d{2}[./-]\d{4})'),
    re.compile(r'(?i)vencimento\s*[:\-]?\s*(\d{2}[./-]\d{2}[./-]\d{4})'),
]

def _try_gemini_totais(texto: str) -> Optional[Dict]:
    if not texto or len(texto) < 20:
        return None
    prompt = (
        "Você receberá o texto extraído de uma fatura bancária/cartão em português.\n"
        "Extraia APENAS os campos a seguir com máxima precisão:\n"
        "{ total_a_pagar: number, pagamento: number|null, vencimento: string|null }\n"
        "- total_a_pagar: valor do 'Total a pagar' ou 'Total da fatura'.\n"
        "- pagamento: valor em 'Pagamentos e créditos' ou 'Pagamento efetuado'; se não houver, null.\n"
        "- vencimento: data do vencimento ('Vence em' ou 'Vencimento') no formato DD/MM/AAAA; se não houver, null.\n"
        "Ignore limite, saldo, tarifas, encargos, consomos e quaisquer outros campos.\n"
        "Responda em JSON puro, sem comentários nem explicações.\n"
        "Texto:\n" + texto[:20000]
    )
    try:
        out = ds_generate(prompt, temperature=0.0, max_tokens=400, timeout=20, system_instruction="Responda apenas JSON com as chaves solicitadas.") or ""
        if not out:
            raise Exception("ds_empty")
        j = None
        try:
            j = json.loads(out)
        except:
            m = re.search(r'\{[\s\S]*\}', out)
            if m:
                try:
                    j = json.loads(m.group(0))
                except:
                    j = None
        if not j or not isinstance(j, dict):
            raise Exception("ds_invalid")
        return {
            "total_a_pagar": j.get("total_a_pagar"),
            "pagamento": j.get("pagamento"),
            "vencimento": j.get("vencimento"),
        }
    except:
        try:
            cli = get_client()
        except:
            cli = None
        if not cli:
            return None
        try:
            resposta = cli.models.generate_content(
                model="gemini-2.5-flash",
                contents=prompt,
            )
            out = (getattr(resposta, "text", "") or "").strip()
            if not out:
                return None
            j = None
            try:
                j = json.loads(out)
            except:
                m = re.search(r'\{[\s\S]*\}', out)
                if m:
                    try:
                        j = json.loads(m.group(0))
                    except:
                        j = None
            if not j or not isinstance(j, dict):
                return None
            return {
                "total_a_pagar": j.get("total_a_pagar"),
                "pagamento": j.get("pagamento"),
                "vencimento": j.get("vencimento"),
            }
        except:
            return None

def parse_pdf_totais(text: str) -> Dict:
    if not text or len(text) < 10:
        return {"total_a_pagar": None, "pagamento": None, "vencimento": None}
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    total_val = None
    pag_val = None
    venc = None
    for i, ln in enumerate(lines):
        for vp in _VENC_PATTERNS:
            vm = vp.search(ln)
            if vm and not venc:
                venc = vm.group(1).replace("-", "/").replace(".", "/")
        for tp in _TOT_PATTERNS:
            tm = tp.search(ln)
            if tm:
                raw = tm.group(1)
                val = parse_value(raw)
                if val is not None and total_val is None:
                    total_val = val
        if total_val is None and re.search(r'(?i)total\s+a\s+pagar|total\s+da\s+fatura', ln):
            j = i + 1
            while j < len(lines) and (j - i) <= 2:
                nxt = lines[j]
                m = _AMOUNT_RE.search(nxt)
                if m:
                    raw = m.group(2)
                    if re.search(r'[.,]\d{2}\b', raw):
                        val = parse_value(raw)
                        if val is not None:
                            total_val = val
                            break
                j += 1
        for pp in _PAG_PATTERNS:
            pm = pp.search(ln)
            if pm:
                raw = pm.group(1)
                val = parse_value(raw)
                if val is not None and pag_val is None:
                    pag_val = val
        if pag_val is None and re.search(r'(?i)pagamentos?\s+e\s+cr[eé]ditos?|pagamento\s+efetuado', ln):
            j = i + 1
            while j < len(lines) and (j - i) <= 2:
                nxt = lines[j]
                if re.search(r'(?i)total|fatura', nxt):
                    break
                m = _AMOUNT_RE.search(nxt)
                if m:
                    raw = m.group(2)
                    if re.search(r'[.,]\d{2}\b', raw):
                        if not re.search(r'(?i)\b(R\\$|RM)\b', nxt):
                            break
                        val = parse_value(raw)
                        if val is not None:
                            pag_val = val
                            break
                j += 1
    return {"total_a_pagar": total_val, "pagamento": pag_val, "vencimento": venc}

def extrair_totais_a_pagar_de_pdf(path: Optional[str] = None, data: Optional[bytes] = None) -> Dict:
    if path and not os.path.exists(path):
        return {"total_a_pagar": None, "pagamento": None, "vencimento": None, "doc_tipo": None, "instituicao": None, "bandeira": None}
    if data is None and path:
        data = _read_file_bytes(path)
    texto = extract_text_from_pdf(path=path, data=data)
    via_ai = _try_gemini_totais(texto)
    if via_ai and isinstance(via_ai.get("total_a_pagar"), (int, float)):
        info = _detect_card_info(texto)
        via_ai.update(info)
        return via_ai
    base = parse_pdf_totais(texto)
    info = _detect_card_info(texto)
    base.update(info)
    return base

def _detect_card_info(texto: str) -> Dict:
    t = (texto or "").lower()
    inst = None
    brand = None
    doc_tipo = None
    if re.search(r'(?i)\bfatura\b', texto) and re.search(r'(?i)cart[aã]o', texto):
        doc_tipo = "cartao"
    elif re.search(r'(?i)cart[aã]o\s+(visa|mastercard|elo|hipercard|american\s+express|amex)', texto):
        doc_tipo = "cartao"
    # Instituições comuns
    inst_map = [
        (r'\bmercado\s+pago\b', 'Mercado Pago'),
        (r'\bnubank\b', 'Nubank'),
        (r'\bsantander\b', 'Santander'),
        (r'\bita[úu]\b|\bitau\b', 'Itaú'),
        (r'\bbradesco\b', 'Bradesco'),
        (r'\binter\b', 'Banco Inter'),
        (r'\bc6\s+bank\b|\bc6\b', 'C6 Bank'),
        (r'\bpicpay\b', 'PicPay'),
        (r'\bbanco\s+do\s+brasil\b', 'Banco do Brasil'),
        (r'\bcaixa\s+econ[ôo]mica\b|\bcaixa\b', 'Caixa'),
    ]
    for pat, name in inst_map:
        if re.search(pat, t, re.IGNORECASE):
            inst = name
            break
    # Bandeiras
    if re.search(r'\bvisa\b', t, re.IGNORECASE):
        brand = 'Visa'
    elif re.search(r'\bmastercard\b', t, re.IGNORECASE):
        brand = 'Mastercard'
    elif re.search(r'\belo\b', t, re.IGNORECASE):
        brand = 'Elo'
    elif re.search(r'american\s+express|\bamex\b', t, re.IGNORECASE):
        brand = 'American Express'
    elif re.search(r'\bhipercard\b', t, re.IGNORECASE):
        brand = 'Hipercard'
    return {"doc_tipo": doc_tipo, "instituicao": inst, "bandeira": brand}
