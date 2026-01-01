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
    if alinhar_direita and largura is not None:
        valor_str = str(valor)
        min_right = 12
        max_right = max(min_right, len(valor_str))
        right_width = min(max_right, max(min_right, largura - 10))
        left_width = max(8, largura - 1 - right_width)
        base = f"{descricao:<{left_width}} {valor_str:>{right_width}}"
    else:
        base = f"{descricao} {valor}"
    if emoji:
        base = f"{emoji} {base}"
    if largura is not None:
        if len(base) > largura:
            base = base[:largura]
        elif len(base) < largura:
            base = base + (" " * (largura - len(base)))
    return base

def criar_cabecalho(titulo, largura=50):
    return f"*{titulo}*"

def criar_secao(titulo):
    return f"\n📌 *{titulo}*\n{'─' * 40}"

def wrap_code_block(texto):
    return f"```text\n{texto}\n```"
