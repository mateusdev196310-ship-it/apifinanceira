import re

CATEGORY_PATTERNS = {
  'alimentacao': [
    r'\bmercado\b', r'\bsupermercado\b', r'\bmercadinho\b', r'\bmercearia\b', r'\bpadaria\b',
    r'\brestaurante\b', r'\blanches?\b', r'\bcomida\b', r'\bpizza\b', r'\bhamburguer\b', r'\bhambuguer\b', r'\bhambúrguer\b', r'\bhamburgueria\b',
    r'\bburger\b', r'\bsandu[ií]che\b', r'\bcaf[eé]\b', r'\bchurrasco\b', r'\bvinho\b', r'\bcerveja\b', r'\bbebida\b',
    r'\bmarmita\b', r'\bquentinha\b', r'\bsushi\b', r'\bpastel\b', r'\bdog\b', r'\bifood\b', r'\bdelivery\b'
  ],
  'transporte': [
    r'\bgasolina\b', r'\bcombust[ií]vel\b', r'\buber\b', r'\b99\b', r'\bestacionamento\b', r'\bônibus\b', r'\bonibus\b', r'\bmetr[oô]\b',
    r'\bpassagem\b', r'\btax[ií]\b', r'\bpedag[ií]o\b', r'\bposto\b'
  ],
  'moradia': [r'\baluguel\b', r'\bcondom[ií]nio\b', r'\bcondominio\b', r'\biptu\b', r'\benergia\b', r'\b[áa]gua\b', r'\bluz\b'],
  'saude': [r'\bfarm[áa]cia\b', r'\bm[eé]dic[o]\b', r'\brem[eé]di[o]\b', r'\bdentista\b', r'\bconsulta\b', r'\bexame\b', r'\bplano\s+de\s+sa[úu]de\b', r'\bplano\b'],
  'lazer': [r'\bcinema\b', r'\bstreaming\b', r'\bacademia\b', r'\bnetflix\b', r'\bspotify\b', r'\bjogo\b', r'\baposta[s]?\b', r'\bcassino\b'],
  'vestuario': [r'\broupa\b', r'\bsapato\b', r'\bcamisa\b', r'\bcal[cç]a\b', r'\bmoleton\b', r'\bcamiseta\b', r'\bt[eê]nis\b', r'\bacess[óo]ri[o]\b'],
  'servicos': [
    r'\bassinatura\b', r'\bservi[cç]o\b', r'\binternet\b', r'\btelefonia\b', r'\bcabeleireiro\b', r'\bbarbearia\b', r'\bsal[aã]o\b', r'\bmanicure\b', r'\bpedicure\b',
    r'\bplano\b', r'\btv\b', r'\bnet\b', r'\bvivo\b', r'\bclaro\b', r'\boi\b', r'\bprime\b', r'\bdisney\b', r'\bicloud\b', r'\bgoogle\s+one\b', r'\bspotify\b', r'\byoutube\b'
  ],
  'salario': [r'\bsal[áa]rio\b', r'\bsalario\b', r'\bfreela\b'],
  'vendas': [r'\bvendi\b', r'\bvenda[s]?\b', r'\bvendas\b'],
}

VERB_CATEGORY_DEFAULT = {
    'gastei': 'outros',
    'paguei': 'servicos',
    'comprei': 'outros',
    'custou': 'outros',
    'recebi': 'outros',
    'ganhei': 'outros',
    'vendi': 'vendas',
    'salário': 'salario',
    'salario': 'salario',
    'freela': 'salario',
}

def detect_category_with_confidence(text, verb=None):
    t = (text or '').lower()
    if re.search(r'\bpix\b', t, re.IGNORECASE) or re.search(r'\btransfer\w*\b', t, re.IGNORECASE) or re.search(r'\bdep[óo]sito\b', t, re.IGNORECASE):
        if re.search(r'assinatura\s+eletr[ôo]nica', t, re.IGNORECASE) or re.search(r'assinatura\s+digital', t, re.IGNORECASE) or re.search(r'\binternet\s+banking\b', t, re.IGNORECASE):
            if not re.search(r'(mercado|supermercado|farm[áa]cia|restaurante|padaria|posto|uber|gasolina|combust[ií]vel|aluguel|condom[ií]nio|energia|[áa]gua|luz)', t, re.IGNORECASE):
                return 'outros', 0.4
        if re.search(r'\bservi[çc]o\b', t, re.IGNORECASE):
            if not re.search(r'(netflix|prime|disney|spotify|youtube|telefonia|vivo|claro|oi|internet)', t, re.IGNORECASE):
                if not re.search(r'(mercado|supermercado|farm[áa]cia|restaurante|padaria|posto|uber|gasolina|combust[ií]vel|aluguel|condom[ií]nio|energia|[áa]gua|luz)', t, re.IGNORECASE):
                    return 'outros', 0.4
    # Exact keyword matches with word boundaries
    for cat, patterns in CATEGORY_PATTERNS.items():
        for pat in patterns:
            if re.search(pat, t, re.IGNORECASE):
                # High confidence for exact pattern hit
                return cat, 0.95
    # Additional explicit signals
    if re.search(r'\bsal[áa]rio\b|\bcontracheque\b|\bholerite\b|\bfolha\b', t, re.IGNORECASE):
        return 'salario', 0.95
    if re.search(r'\bvend\w*\b', t, re.IGNORECASE):
        return 'vendas', 0.8
    if re.search(r'\bpix\b', t, re.IGNORECASE) or re.search(r'\btransfer\w*\b', t, re.IGNORECASE) or re.search(r'\bdep[óo]sito\b', t, re.IGNORECASE):
        # PIX/transfer/deposito is ambiguous without context
        return 'outros', 0.4
    # Verb-based default as very low confidence
    if verb:
        v = verb.lower()
        if v in VERB_CATEGORY_DEFAULT:
            return VERB_CATEGORY_DEFAULT[v], 0.3
    return 'outros', 0.2

def detect_category(text, verb=None):
    cat, _ = detect_category_with_confidence(text, verb)
    return cat

def parse_value(raw):
    s = raw.strip()
    s = s.replace('\u00A0', ' ').replace(' ', '')
    if ',' in s:
        s = s.replace('.', '').replace(',', '.')
    else:
        parts = s.split('.')
        if len(parts) > 1 and all(p.isdigit() for p in parts) and len(parts[-1]) != 2:
            s = ''.join(parts)
    try:
        return float(s)
    except:
        return None

HUNDREDS_WORDS = {
    'cem': 100,
    'duzentos': 200,
    'trezentos': 300,
    'quatrocentos': 400,
    'quinhentos': 500,
    'seiscentos': 600,
    'setecentos': 700,
    'oitocentos': 800,
    'novecentos': 900,
}
TENS_WORDS = {
    'dez': 10,
    'vinte': 20,
    'trinta': 30,
    'quarenta': 40,
    'cinquenta': 50,
    'sessenta': 60,
    'setenta': 70,
    'oitenta': 80,
    'noventa': 90,
}
UNITS_WORDS = {
    'um': 1,
    'uma': 1,
    'dois': 2,
    'duas': 2,
    'três': 3,
    'tres': 3,
    'quatro': 4,
    'cinco': 5,
    'seis': 6,
    'sete': 7,
    'oito': 8,
    'nove': 9,
}
VERB_BOUNDARY = re.compile(r'\b(?:gastei|paguei|comprei|custou|recebi|ganhei|vendi|sal[áa]rio|salario|freela|transferi)\b', re.IGNORECASE)
VERB_ONLY_EXP = re.compile(r'\b(gastei|paguei|comprei|custou|transferi)\b', re.IGNORECASE)
VERB_ONLY_INC = re.compile(r'\b(recebi|ganhei|vendi|sal[áa]rio|salario|freela)\b', re.IGNORECASE)
def _word_to_number_simple(w):
    wl = (w or '').strip().lower()
    if wl in HUNDREDS_WORDS:
        return HUNDREDS_WORDS[wl]
    if wl in TENS_WORDS:
        return TENS_WORDS[wl]
    if wl in UNITS_WORDS:
        return UNITS_WORDS[wl]
    return None
def _adjust_magnitude_by_tail(base, tail):
    tl = (tail or '').lower()
    m = re.match(r'^\s*milh(?:[õo]es|[ãa]o(?:es)?|ao(?:es)?)\b(?:\s*e\s*(\d{1,3}(?:[.\s]\d{3})*(?:,\d{2})|\d+[.,]\d{2}|\d+|[a-z\u00c0-\u017f]+))?', tl, re.IGNORECASE)
    if m:
        add_raw = m.group(1)
        add_val = None
        if add_raw:
            add_val = parse_value(add_raw)
            if add_val is None:
                add_val = _word_to_number_simple(add_raw)
        return base * 1000000 + (add_val or 0)
    m = re.match(r'^\s*mil\b(?:\s*e\s*(\d{1,3}(?:[.\s]\d{3})*(?:,\d{2})|\d+[.,]\d{2}|\d+|[a-z\u00c0-\u017f]+))?', tl, re.IGNORECASE)
    if m:
        add_raw = m.group(1)
        add_val = None
        if add_raw:
            add_val = parse_value(add_raw)
            if add_val is None:
                add_val = _word_to_number_simple(add_raw)
        return base * 1000 + (add_val or 0)
    return base
def _extract_additional_amounts(tail):
    out = []
    seen = set()
    tl = (tail or '').strip()
    pat_num = re.compile(r'(?:R?\$?\s*)?(\d{1,3}(?:[.\s]\d{3})*(?:,\d{2})|\d+[.,]\d{2}|\d+)\b', re.IGNORECASE)
    pat_thousand = re.compile(r'(\d+)\s*mil(?:\s*e\s*(\d{1,3}(?:[.\s]\d{3})*(?:,\d{2})|\d+[.,]\d{2}|\d+|[a-z\u00c0-\u017f]+))?', re.IGNORECASE)
    for m in pat_thousand.finditer(tl):
        try:
            base = float(m.group(1))
            add_raw = m.group(2)
            add_val = 0.0
            if add_raw:
                pv = parse_value(add_raw)
                if pv is None:
                    wv = _word_to_number_simple(add_raw)
                    add_val = float(wv or 0)
                else:
                    add_val = float(pv)
            v = base * 1000 + add_val
            key = f"{v:.6f}"
            if key not in seen:
                out.append(v)
                seen.add(key)
        except:
            continue
    for m in pat_num.finditer(tl):
        try:
            raw = m.group(1)
            val = parse_value(raw)
            if val is not None:
                end_idx = m.end()
                tail2 = tl[end_idx:]
                val2 = _adjust_magnitude_by_tail(val, tail2)
                v = float(val2)
                key = f"{v:.6f}"
                if key not in seen:
                    out.append(v)
                    seen.add(key)
        except:
            continue
    return out

TYPO_MAP = {
  'gnahei': 'ganhei',
  'gnhei': 'ganhei',
  'gnh': 'ganhei',
  'gnahe': 'ganhei',
  'ganhe': 'ganhei',
  'vnedi': 'vendi',
  'gostei': 'gastei',
  'gaste': 'gastei',
  'gastai': 'gastei',
  'gasti': 'gastei',
  'gaxtei': 'gastei',
  'pagei': 'paguei',
  'pague': 'paguei',
  'conprei': 'comprei',
  'comprie': 'comprei',
  'recbi': 'recebi',
  'receby': 'recebi',
  'vendy': 'vendi',
  'tyambém': 'também',
  'tambem': 'também',
  'apo': 'após',
  'apó': 'após',
  'apos': 'após',
  'semanl': 'semanal',
  'ocm': 'com',
  'aqilo': 'aquilo',
  'auqilo': 'aquilo',
  'compranod': 'comprando',
  'comprndo': 'comprando',
  'comrpando': 'comprando',
}

FILLER_WORDS = {
    'depois',
    'após',
    'apos',
    'apo',
    'apó',
    'mais',
    'também',
    'tambem',
    'tyambém',
    'hoje',
    'amanha',
    'amanhã',
    'ontem',
    'disso',
    'isso',
    'eh',
    'é',
    'ai',
    'aí',
    'tipo',
    'né',
}

def normalize_text(text):
    parts = text.split()
    out = []
    for p in parts:
        k = p.lower()
        if k in FILLER_WORDS:
            continue
        out.append(TYPO_MAP.get(k, p))
    t = " ".join(out)
    t = re.sub(r'(?:R\$\s*){2,}', 'R$ ', t)
    return t

def clean_desc(s):
    t = (s or "").strip()
    t = re.sub(r'^(?:hoje|amanha|amanhã|ontem)\s+', '', t, flags=re.IGNORECASE)
    t = re.sub(r'^(?:em|no|na|de|do|da|para|pra|por|me)\s+', '', t, flags=re.IGNORECASE)
    t = re.sub(r'^(?:ganhos?|gastos?|receitas?|receita)\s+(?:com|de|do|da)\s+', '', t, flags=re.IGNORECASE)
    t = re.sub(r'\bR\$\s*', '', t, flags=re.IGNORECASE)
    t = re.sub(r'\b(?:reais?|rs)\b', '', t, flags=re.IGNORECASE)
    return t.strip()

def naturalize_description(tipo, categoria, desc):
  d = clean_desc(desc or "")
  dl = d.lower().strip()
  cat = (categoria or "").lower()
  def _canon_income(s):
    tl = s.lower()
    if re.search(r'sal[áa]ri', tl):
      return 'salário'
    if re.search(r'vend', tl):
      return 'vendas'
    if re.search(r'\bpix\b', tl):
      return 'pix recebido'
    if re.search(r'transfer', tl):
      return 'transferência'
    if re.search(r'dep[óo]sito', tl):
      return 'depósito'
    if re.search(r'freela', tl):
      return 'freela'
    if re.search(r'servi[çc]o', tl):
      return 'serviços'
    return ''
  def _canon_expense(s):
    tl = s.lower()
    pats = [
      (r'\blanche\b', 'lanche'),
      (r'\bpizza\b', 'pizza'),
      (r'\bmercado\b', 'mercado'),
      (r'\bsupermercado\b', 'supermercado'),
      (r'\bfarm[áa]cia\b', 'farmácia'),
      (r'\brestaurante\b', 'restaurante'),
      (r'\bpadaria\b', 'padaria'),
      (r'\buber\b', 'uber'),
      (r'gasolina|combust[ií]vel', 'gasolina'),
      (r'internet', 'internet'),
      (r'streaming|netflix|prime|disney|spotify|youtube', 'assinatura'),
      (r'assinatura', 'assinatura'),
      (r'telefonia|vivo|claro|oi', 'telefonia'),
      (r'aluguel', 'aluguel'),
      (r'condom[ií]nio', 'condomínio'),
      (r'energia', 'energia'),
      (r'[áa]gua', 'água'),
      (r'luz', 'luz'),
      (r'estacionamento', 'estacionamento'),
      (r'ônibus|onibus', 'ônibus'),
      (r'metro|metrô', 'metrô'),
      (r'taxi|táxi', 'táxi'),
      (r'pedagio|pedágio', 'pedágio'),
    ]
    for pat, canon in pats:
      if re.search(pat, tl, re.IGNORECASE):
        return canon
    return ''
  def _short_title(s):
    s0 = re.sub(r'\s+', ' ', s.strip())
    s0 = re.sub(r'\bR\$\s*', '', s0, flags=re.IGNORECASE)
    s0 = re.sub(r'\b(?:reais?|rs)\b', '', s0, flags=re.IGNORECASE)
    s0 = ' '.join(w for w in s0.split() if not re.fullmatch(r'\d+(?:[.,]\d+)?', w) and w.lower() not in {'por'})
    toks = []
    seen = set()
    for w in s0.split():
      wl = w.lower()
      if wl in seen:
        continue
      seen.add(wl)
      toks.append(w)
      if len(toks) >= 3:
        break
    if not toks:
      return ''
    return ' '.join(x[:1].upper() + x[1:] for x in toks)
  def _limit_words_unique(s, n=4):
    s0 = re.sub(r'\s+', ' ', s.strip())
    toks = []
    seen = set()
    for w in s0.split():
      wl = w.lower()
      if wl in seen:
        continue
      seen.add(wl)
      toks.append(w)
      if len(toks) >= n:
        break
    return ' '.join(toks)
  def _strip_trailing_prep(s):
    return re.sub(r'\b(?:de|da|do|das|dos|para)\s*$', '', s.strip(), flags=re.IGNORECASE)
  def _strip_leading_prep(s):
    return re.sub(r'^(?:em|no|na|de|do|da|para|pra|por)\s+', '', s.strip(), flags=re.IGNORECASE)
  def _after_verb(s, root):
    m = re.search(r'\b' + root + r'\w*\b\s+(.*)', s, re.IGNORECASE)
    return m.group(1).strip() if m else ''
  def _strip_price(t):
    t0 = re.sub(r'\bpor\b.*$', '', t, flags=re.IGNORECASE)
    t0 = re.sub(r'\bR\$\s*', '', t0, flags=re.IGNORECASE)
    t0 = re.sub(r'\b(?:reais?|rs)\b', '', t0, flags=re.IGNORECASE)
    t0 = re.sub(r'\b\d+(?:[.,]\d+)?\b', '', t0)
    return re.sub(r'\s+', ' ', t0).strip()
  def _normalize_tail_phrase(t):
    t1 = (t or '').strip()
    t1 = re.sub(r'^\s*de\s+(?=(?:da|do|das|dos|para)\b)', '', t1, flags=re.IGNORECASE)
    t1 = re.sub(r'\b(?:meu|minha|meus|minhas)\b', '', t1, flags=re.IGNORECASE)
    t1 = re.sub(r'\s+', ' ', t1).strip()
    return t1
  if not dl or re.fullmatch(r'\d+(?:[.,]\d+)?', dl) or dl in {'gastei', 'paguei', 'comprei', 'custou'}:
    if str(tipo) == '1':
      return 'salário' if cat == 'salario' else 'receita'
    return 'despesa'
  if str(tipo) == '1':
    if re.match(r'^\s*venda\b', dl, re.IGNORECASE):
      t0 = _strip_price(d)
      t0 = _strip_trailing_prep(t0)
      return _limit_words_unique(t0, 8) or 'vendas'
    if re.match(r'^\s*transfer[êe]ncia\b', dl, re.IGNORECASE):
      t0 = _strip_price(d)
      t0 = _normalize_tail_phrase(t0)
      return _limit_words_unique(t0, 8) or 'transferência'
    if re.search(r'\bvend\w*\b', dl):
      tail = _after_verb(d, 'vend')
      tail = _strip_price(tail)
      phrase = _limit_words_unique(tail, 8)
      phrase = _strip_trailing_prep(phrase)
      if phrase:
        return 'Venda de ' + phrase
    if re.search(r'\btransfer', dl, re.IGNORECASE):
      tail = _after_verb(d, 'transfer')
      tail = _strip_price(tail)
      tail = _normalize_tail_phrase(tail)
      phrase = _limit_words_unique(tail, 6)
      phrase = _strip_trailing_prep(phrase)
      phrase = _strip_leading_prep(phrase)
      if phrase:
        return 'Transferência ' + phrase
      return 'Transferência'
    c = _canon_income(dl)
    if c:
      return c
    if cat == 'salario':
      if 'semanal' in dl or 'semana' in dl:
        return 'salário semanal'
      if 'mensal' in dl or 'mes' in dl or 'mês' in dl:
        return 'salário mensal'
      return 'salário'
    if cat == 'vendas':
      return 'vendas'
    if 'aposta' in dl or 'apostas' in dl:
      return 'ganhos com apostas'
    tt = _short_title(d)
    return tt or 'receita'
  else:
    c = _canon_expense(dl)
    if c:
      return c[:1].upper() + c[1:]
    if re.search(r'\btransfer', dl, re.IGNORECASE):
      tail = _after_verb(d, 'transfer')
      tail = _strip_price(tail)
      tail = _normalize_tail_phrase(tail)
      phrase = _limit_words_unique(tail, 8)
      phrase = _strip_trailing_prep(phrase)
      phrase = _strip_leading_prep(phrase)
      if phrase and not re.match(r'^(?:para|pra)\b', phrase, re.IGNORECASE):
        phrase = 'para ' + phrase
      if phrase:
        return 'Transferência ' + phrase
      return 'Transferência'
    if re.search(r'\b(gastei|paguei|comprei|custou)\b', dl, re.IGNORECASE):
      tail = ''
      for root in ('gast', 'pag', 'compr', 'cust'):
        if re.search(r'\b' + root + r'\w*\b', dl, re.IGNORECASE):
          tail = _after_verb(d, root)
          break
      tail = _strip_price(tail)
      phrase = _limit_words_unique(tail, 8)
      phrase = _strip_trailing_prep(phrase)
      phrase = _strip_leading_prep(phrase)
      if phrase.lower().startswith('com '):
        phrase = phrase[4:].strip()
      if phrase:
        return 'Gastos com ' + phrase
    if re.match(r'^\s*com\b', d, re.IGNORECASE):
      tail = _strip_price(d)
      tail = re.sub(r'^\s*com\s+', '', tail, flags=re.IGNORECASE)
      tail = _strip_trailing_prep(tail)
      return 'Gastos com ' + _limit_words_unique(tail, 8)
    tt = _short_title(d)
    return tt or 'Despesa'

def natural_score(s):
  t = (s or "").lower().strip()
  if re.search(r'\b(ganhos?|gastos?)\s+com\b', t) or re.search(r'\breceitas?\s+(de|do|da)\b', t) or re.search(r'\breceita\s+de\b', t) or t in {'despesa', 'receita', 'salário'}:
    return 2
  return 1

EXPENSE_REGEX = re.compile(
    r'(gastei|paguei|comprei|custou)\s*(R?\$?\s*)?(\d{1,3}(?:[.\s]\d{3})+(?:,\d{2})?|\d+[.,]\d{2}|\d+)(?=\D|$)(?:\s*(?:reais?|rs))?(?:\s+(?:(?:em|no|na|de|com)\s+)?([^,;.\n]+?))?\s*(?=$|\s*(?:e\s+)?(?:gastei|paguei|comprei|custou|recebi|ganhei|vendi|sal[áa]rio|salario|freela)|,|;|\.)',
    re.IGNORECASE
)
INCOME_REGEX = re.compile(
    r'(recebi|ganhei|vendi|sal[áa]rio|salario|freela)\s*(R?\$?\s*)?(\d{1,3}(?:[.\s]\d{3})+(?:,\d{2})?|\d+[.,]\d{2}|\d+)(?=\D|$)(?:\s*(?:reais?|rs))?(?:\s+(?:(?:de|do|da)\s+)?([^,;.\n]+?))?\s*(?=$|\s*(?:e\s+)?(?:gastei|paguei|comprei|custou|recebi|ganhei|vendi|sal[áa]rio|salario|freela)|,|;|\.)',
    re.IGNORECASE
)
TRANSFER_OUT_REGEX = re.compile(
    r'(?:transferi|fiz(?:\s+uma)?\s+transfer[êe]ncia)\s*(?:de|do|da)?\s*(R?\$?\s*)?(\d{1,3}(?:[.\s]\d{3})+(?:,\d{2})?|\d+[.,]\d{2}|\d+)(?=\D|$)(?:\s*(?:reais?|rs))?(?:\s+(?:(?:para|pra)\s+)?([^,;.\n]+?))?\s*(?=$|\s*(?:e\s+)?(?:gastei|paguei|comprei|custou|recebi|ganhei|vendi|sal[áa]rio|salario|freela|transferi|transfer[êe]ncia)|,|;|\.)',
    re.IGNORECASE
)

def parse_text_to_transactions(text):
    t = normalize_text(text.strip())
    results = []
    has_any = False
    for m in EXPENSE_REGEX.finditer(t):
        verb, _, val_raw, desc = m.groups()
        val = parse_value(val_raw)
        if val is None:
            continue
        inner_tail = t[m.span(3)[1]:m.end()]
        val = _adjust_magnitude_by_tail(val, inner_tail)
        desc_clean = clean_desc(desc or verb)
        categoria, conf = detect_category_with_confidence(desc_clean or t, verb)
        context = t[m.start():m.end()]
        desc_nat = naturalize_description('0', categoria, context)
        results.append({
            "tipo": '0',
            "valor": val,
            "categoria": categoria,
            "descricao": desc_nat,
            "moeda": "BRL",
            "confidence_score": float(conf),
            "pendente_confirmacao": (categoria == 'outros') or (float(conf) < 0.9),
        })
        outer_tail = t[m.end():]
        next_verb = VERB_BOUNDARY.search(t, m.end())
        if next_verb:
            outer_tail = t[m.end():next_verb.start()]
        extra_vals = _extract_additional_amounts(outer_tail)
        for v2 in extra_vals:
            if v2 == val:
                continue
            results.append({
                "tipo": '0',
                "valor": v2,
                "categoria": categoria,
                "descricao": desc_nat,
                "moeda": "BRL",
                "confidence_score": float(conf),
                "pendente_confirmacao": (categoria == 'outros') or (float(conf) < 0.9),
            })
        has_any = True
    for m in INCOME_REGEX.finditer(t):
        verb, _, val_raw, desc = m.groups()
        val = parse_value(val_raw)
        if val is None:
            continue
        inner_tail = t[m.span(3)[1]:m.end()]
        val = _adjust_magnitude_by_tail(val, inner_tail)
        desc_clean = clean_desc(desc or verb)
        categoria, conf = detect_category_with_confidence(desc_clean or t, verb)
        context = t[m.start():m.end()]
        desc_nat = naturalize_description('1', categoria, context)
        results.append({
            "tipo": '1',
            "valor": val,
            "categoria": categoria,
            "descricao": desc_nat,
            "moeda": "BRL",
            "confidence_score": float(conf),
            "pendente_confirmacao": (categoria == 'outros') or (float(conf) < 0.9),
        })
        outer_tail = t[m.end():]
        next_verb = VERB_BOUNDARY.search(t, m.end())
        if next_verb:
            outer_tail = t[m.end():next_verb.start()]
        extra_vals = _extract_additional_amounts(outer_tail)
        for v2 in extra_vals:
            if v2 == val:
                continue
            results.append({
                "tipo": '1',
                "valor": v2,
                "categoria": categoria,
                "descricao": desc_nat,
                "moeda": "BRL",
                "confidence_score": float(conf),
                "pendente_confirmacao": (categoria == 'outros') or (float(conf) < 0.9),
            })
        has_any = True
    for m in TRANSFER_OUT_REGEX.finditer(t):
        _, val_raw, desc = m.groups()
        val = parse_value(val_raw)
        if val is None:
            continue
        inner_tail = t[m.span(3)[1]:m.end()]
        val = _adjust_magnitude_by_tail(val, inner_tail)
        desc_clean = clean_desc(desc or '')
        categoria, conf = detect_category_with_confidence(desc_clean or t, 'transferi')
        context = t[m.start():m.end()]
        desc_nat = naturalize_description('0', categoria, context)
        results.append({
            "tipo": '0',
            "valor": val,
            "categoria": categoria,
            "descricao": desc_nat,
            "moeda": "BRL",
            "confidence_score": float(conf),
            "pendente_confirmacao": (categoria == 'outros') or (float(conf) < 0.9),
        })
        outer_tail = t[m.end():]
        next_verb = VERB_BOUNDARY.search(t, m.end())
        if next_verb:
            outer_tail = t[m.end():next_verb.start()]
        extra_vals = _extract_additional_amounts(outer_tail)
        for v2 in extra_vals:
            if v2 == val:
                continue
            results.append({
                "tipo": '0',
                "valor": v2,
                "categoria": categoria,
                "descricao": desc_nat,
                "moeda": "BRL",
                "confidence_score": float(conf),
                "pendente_confirmacao": (categoria == 'outros') or (float(conf) < 0.9),
            })
        has_any = True
    if not has_any:
        for m in VERB_ONLY_EXP.finditer(t):
            verb = m.group(1)
            next_verb = VERB_BOUNDARY.search(t, m.end())
            tail = t[m.end(): next_verb.start()] if next_verb else t[m.end():]
            vals = _extract_additional_amounts(tail)
            if not vals:
                continue
            desc_clean = clean_desc(tail or verb)
            categoria, conf = detect_category_with_confidence(desc_clean or t, verb)
            context = t[m.start(): m.end()] + " " + tail
            desc_nat = naturalize_description('0', categoria, context)
            for v in vals:
                results.append({
                    "tipo": '0',
                    "valor": v,
                    "categoria": categoria,
                    "descricao": desc_nat,
                    "moeda": "BRL",
                    "confidence_score": float(conf),
                    "pendente_confirmacao": (categoria == 'outros') or (float(conf) < 0.9),
                })
        for m in VERB_ONLY_INC.finditer(t):
            verb = m.group(1)
            next_verb = VERB_BOUNDARY.search(t, m.end())
            tail = t[m.end(): next_verb.start()] if next_verb else t[m.end():]
            vals = _extract_additional_amounts(tail)
            if not vals:
                continue
            desc_clean = clean_desc(tail or verb)
            categoria, conf = detect_category_with_confidence(desc_clean or t, verb)
            context = t[m.start(): m.end()] + " " + tail
            desc_nat = naturalize_description('1', categoria, context)
            for v in vals:
                results.append({
                    "tipo": '1',
                    "valor": v,
                    "categoria": categoria,
                    "descricao": desc_nat,
                    "moeda": "BRL",
                    "confidence_score": float(conf),
                    "pendente_confirmacao": (categoria == 'outros') or (float(conf) < 0.9),
                })
    return results
