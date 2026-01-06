def formatar_moeda(valor, com_simbolo=True, negrito=False):
    if valor is None or valor == 0:
        texto = "R$ 0,00" if com_simbolo else "0,00"
        return f"*{texto}*" if negrito else texto
    valor_abs = abs(valor)
    valor_str = f"{valor_abs:,.2f}"
    valor_str = valor_str.replace(",", "X").replace(".", ",").replace("X", ".")
    sinal = "- " if valor < 0 else ""
    simbolo = "R$ " if com_simbolo else ""
    texto = f"{sinal}{simbolo}{valor_str}"
    return f"*{texto}*" if negrito else texto

def formatar_percentual(valor, negrito=False):
    texto = f"{valor:.1f}%"
    return f"*{texto}*" if negrito else texto

def criar_linha_tabela(descricao, valor, alinhar_direita=True, emoji="", largura=None):
    FS = "\u2007"
    if alinhar_direita and largura is not None:
        valor_str = str(valor)
        gap = 1
        max_left = max(0, (largura or 0) - len(valor_str) - gap)
        desc_use = (descricao or "")
        if len(desc_use) > max_left:
            desc_use = desc_use[:max_left]
        pad_len = max_left - len(desc_use)
        pad = (FS * pad_len) + (" " * gap)
        base = f"{desc_use}{pad}{valor_str}"
    else:
        base = f"{descricao} {valor}"
    if emoji:
        base = f"{emoji} {base}"
    if largura is not None and len(base) < largura:
        base = base + (" " * (largura - len(base)))
    return base

def criar_cabecalho(titulo, largura=50):
    return f"*{titulo}*"

def criar_secao(titulo):
    return f"\n📌 *{titulo}*\n{'─' * 40}"

def wrap_code_block(texto):
    return f"```text\n{texto}\n```"
