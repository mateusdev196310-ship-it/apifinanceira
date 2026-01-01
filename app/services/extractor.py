import re
import json
from app.services.gemini import get_client
from app.constants.categories import CATEGORY_LIST
from google.genai import types
from app.services.rule_based import parse_text_to_transactions, naturalize_description, natural_score
def extrair_informacoes_financeiras(texto_usuario):
    prompt = (f'''ANALISE ESTA MENSAGEM E EXTRAIA TODAS AS INFORMAÇÕES FINANCEIRAS:

Mensagem do usuário: "{texto_usuario}"

RETORNE APENAS UM ARRAY JSON COM OBJETOS DESTA ESTRUTURA:
[
    {
        "tipo": "0" para despesa ou "1" para receita,
        "valor": número decimal com duas casas,
        "categoria": {", ".join(CATEGORY_LIST)},
        "descricao": "descrição breve",
        "moeda": "BRL"
    }
]

REGRAS:
1. Tipo:
   - Despesa (gastei, paguei, comprei, custou, transferi) → "0"
   - Receita (recebi, ganhei, vendi, salário, freela) → "1"
2. Categoria: use apenas as listadas. Se ambígua, use "outros". "vendas" para venda/vendi. "salario" somente com menção explícita a salário/holerite/folha/contracheque.
3. Valor: extraia número decimal com duas casas. Suporta formatos como "1.500", "1,500.00", "477,17", "R$ 50". Converta para 50.00, 477.17 etc.
4. Descrição:
   - Curta (3–6 palavras), clara e objetiva, fiel ao que o usuário disse.
   - Remova números e moeda; não use "por X" (preço vai no campo valor).
   - Complete preposições: use "da/do/das/dos/para" com o objeto correto.
   - Forma nominal:
    • Venda: "Venda de …" (ex.: "Venda de calça", "Venda de peça da moto")
    • Transferência recebida: "Transferência de …" (ex.: "Transferência da prima")
    • Transferência enviada: "Transferência para …" (ex.: "Transferência para a prima")
    • Despesa: "Gastos com …" (ex.: "Gastos com peça da moto", "Gastos com internet")
   - Preserve substantivos principais e nomes próprios/marcas (ex.: "Netflix", "Banco do Brasil"). Evite adjetivos e palavras vazias ("meu", "minha", "uma", "um"), use apenas se necessário para fluência.
5. Múltiplas transações: mensagens podem conter várias; retorne um array com todas.
6. Deduplicação: se houver transações repetidas (mesmo tipo/valor/categoria), mantenha apenas uma com a descrição mais curta e clara.
7. Saída: retorne SOMENTE o ARRAY JSON, sem textos extras.

EXEMPLOS:
- "gastei 50 no mercado"
[
  {
    "tipo": "0",
    "valor": 50.00,
    "categoria": "alimentacao",
    "descricao": "Gastos com mercado",
    "moeda": "BRL"
  }
]

- "recebi 1000 de salário"
[
  {
    "tipo": "1",
    "valor": 1000.00,
    "categoria": "salario",
    "descricao": "salário",
    "moeda": "BRL"
  }
]

- "recebi uma transferência de 500 da minha prima"
[
  {
    "tipo": "1",
    "valor": 500.00,
    "categoria": "outros",
    "descricao": "Transferência da prima",
    "moeda": "BRL"
  }
]

- "recebi pix do joao 100"
[
  {
    "tipo": "1",
    "valor": 100.00,
    "categoria": "outros",
    "descricao": "Transferência de João",
    "moeda": "BRL"
  }
]

- "vendi uma calça por 50"
[
  {
    "tipo": "1",
    "valor": 50.00,
    "categoria": "vendas",
    "descricao": "Venda de calça",
    "moeda": "BRL"
  }
]

- "transferi 500 para a minha prima"
[
  {
    "tipo": "0",
    "valor": 500.00,
    "categoria": "outros",
    "descricao": "Transferência para a prima",
    "moeda": "BRL"
  }
]

- "gastei 300 com uma peça da moto"
[
  {
    "tipo": "0",
    "valor": 300.00,
    "categoria": "outros",
    "descricao": "Gastos com peça da moto",
    "moeda": "BRL"
  }
]

- "gastei 120 internet e ganhei 200 de vendas"
[
  {
    "tipo": "0",
    "valor": 120.00,
    "categoria": "servicos",
    "descricao": "Gastos com internet",
    "moeda": "BRL"
  },
  {
    "tipo": "1",
    "valor": 200.00,
    "categoria": "vendas",
    "descricao": "vendas",
    "moeda": "BRL"
  }
]

RETORNE SOMENTE O JSON ARRAY, SEM TEXTOS ADICIONAIS.'''
    '''EXEMPLOS ADICIONAIS:
- "gastei 60 na padaria"
[
  {
    "tipo": "0",
    "valor": 60.00,
    "categoria": "alimentacao",
    "descricao": "Gastos com padaria",
    "moeda": "BRL"
  }
]
- "gastei 200 com gasolina"
[
  {
    "tipo": "0",
    "valor": 200.00,
    "categoria": "transporte",
    "descricao": "Gasolina",
    "moeda": "BRL"
  }
]
- "gastei 15 no uber"
[
  {
    "tipo": "0",
    "valor": 15.00,
    "categoria": "transporte",
    "descricao": "uber",
    "moeda": "BRL"
  }
]
- "custou 35 estacionamento"
[
  {
    "tipo": "0",
    "valor": 35.00,
    "categoria": "transporte",
    "descricao": "Estacionamento",
    "moeda": "BRL"
  }
]
- "paguei 89,90 na internet"
[
  {
    "tipo": "0",
    "valor": 89.90,
    "categoria": "servicos",
    "descricao": "Internet",
    "moeda": "BRL"
  }
]
- "paguei 100 de água"
[
  {
    "tipo": "0",
    "valor": 100.00,
    "categoria": "moradia",
    "descricao": "Água",
    "moeda": "BRL"
  }
]
- "paguei 80 de luz"
[
  {
    "tipo": "0",
    "valor": 80.00,
    "categoria": "moradia",
    "descricao": "Luz",
    "moeda": "BRL"
  }
]
- "comprei 120 em farmácia"
[
  {
    "tipo": "0",
    "valor": 120.00,
    "categoria": "saude",
    "descricao": "Farmácia",
    "moeda": "BRL"
  }
]
- "recebi depósito do banco 200"
[
  {
    "tipo": "1",
    "valor": 200.00,
    "categoria": "outros",
    "descricao": "Depósito do banco",
    "moeda": "BRL"
  }
]
- "recebi 450 de freela"
[
  {
    "tipo": "1",
    "valor": 450.00,
    "categoria": "salario",
    "descricao": "freela",
    "moeda": "BRL"
  }
]
- "transferência de 300 da tia"
[
  {
    "tipo": "1",
    "valor": 300.00,
    "categoria": "outros",
    "descricao": "Transferência da tia",
    "moeda": "BRL"
  }
]
- "transferi 95 para o barbeiro"
[
  {
    "tipo": "0",
    "valor": 95.00,
    "categoria": "outros",
    "descricao": "Transferência para o barbeiro",
    "moeda": "BRL"
  }
]
- "vendi celular por 1.200"
[
  {
    "tipo": "1",
    "valor": 1200.00,
    "categoria": "vendas",
    "descricao": "Venda de celular",
    "moeda": "BRL"
  }
]
- "gastei 35 no táxi e recebi pix 50"
[
  {
    "tipo": "0",
    "valor": 35.00,
    "categoria": "transporte",
    "descricao": "Táxi",
    "moeda": "BRL"
  },
  {
    "tipo": "1",
    "valor": 50.00,
    "categoria": "outros",
    "descricao": "pix recebido",
    "moeda": "BRL"
  }
]
'''
    '''EXEMPLOS COM MARCAS E GRANDES VALORES:
- "paguei 39,90 da Netflix"
[
  {
    "tipo": "0",
    "valor": 39.90,
    "categoria": "servicos",
    "descricao": "Assinatura Netflix",
    "moeda": "BRL"
  }
]
- "paguei 19,90 Spotify"
[
  {
    "tipo": "0",
    "valor": 19.90,
    "categoria": "servicos",
    "descricao": "Assinatura Spotify",
    "moeda": "BRL"
  }
]
- "assinatura YouTube Premium 24,90"
[
  {
    "tipo": "0",
    "valor": 24.90,
    "categoria": "servicos",
    "descricao": "Assinatura YouTube Premium",
    "moeda": "BRL"
  }
]
- "recebi 1.500 do Banco do Brasil por pix"
[
  {
    "tipo": "1",
    "valor": 1500.00,
    "categoria": "outros",
    "descricao": "Transferência do Banco do Brasil",
    "moeda": "BRL"
  }
]
- "transferi 250 para o Nubank"
[
  {
    "tipo": "0",
    "valor": 250.00,
    "categoria": "outros",
    "descricao": "Transferência para o Nubank",
    "moeda": "BRL"
  }
]
- "ganhei três milhões"
[
  {
    "tipo": "1",
    "valor": 3000000.00,
    "categoria": "outros",
    "descricao": "receita",
    "moeda": "BRL"
  }
]
- "recebi 2 mil e 300"
[
  {
    "tipo": "1",
    "valor": 2300.00,
    "categoria": "outros",
    "descricao": "receita",
    "moeda": "BRL"
  }
]
-
 "paguei 29,90 Prime Video"
[
  {
    "tipo": "0",
    "valor": 29.90,
    "categoria": "servicos",
    "descricao": "Assinatura Prime Video",
    "moeda": "BRL"
  }
]
-
 "paguei 9,90 Disney+"
[
  {
    "tipo": "0",
    "valor": 9.90,
    "categoria": "servicos",
    "descricao": "Assinatura Disney+",
    "moeda": "BRL"
  }
]
-
 "paguei 3,90 iCloud"
[
  {
    "tipo": "0",
    "valor": 3.90,
    "categoria": "servicos",
    "descricao": "Assinatura iCloud",
    "moeda": "BRL"
  }
]
-
 "paguei 29,90 Google One"
[
  {
    "tipo": "0",
    "valor": 29.90,
    "categoria": "servicos",
    "descricao": "Assinatura Google One",
    "moeda": "BRL"
  }
]
-
 "recebi 1.200 do Itaú"
[
  {
    "tipo": "1",
    "valor": 1200.00,
    "categoria": "outros",
    "descricao": "Transferência do Itaú",
    "moeda": "BRL"
  }
]
-
 "recebi 800 do Bradesco"
[
  {
    "tipo": "1",
    "valor": 800.00,
    "categoria": "outros",
    "descricao": "Transferência do Bradesco",
    "moeda": "BRL"
  }
]
-
 "transferi 400 para o Santander"
[
  {
    "tipo": "0",
    "valor": 400.00,
    "categoria": "outros",
    "descricao": "Transferência para o Santander",
    "moeda": "BRL"
  }
]
'''
    )
    try:
        rb = parse_text_to_transactions(texto_usuario)
        if rb:
            try:
                print(f"[extrair_informacoes_financeiras] fonte=local-regra qtd={len(rb)}")
            except:
                pass
            return rb
        client = get_client()
        ai_resultados = []
        if client is not None:
            resposta = client.models.generate_content(
                model='gemini-2.5-flash',
                contents=prompt,
                config=types.GenerateContentConfig(
                    system_instruction="Você é um extrator financeiro PT-BR. Siga rigorosamente as regras do prompt e retorne somente JSON válido, sem texto extra.",
                    temperature=0.1,
                    max_output_tokens=512,
                ),
            )
            resposta_texto = resposta.text.strip()
            resposta_texto = resposta_texto.replace('```json', '').replace('```', '').strip()
            json_match = re.search(r'\[.*\]', resposta_texto, re.DOTALL)
            if json_match:
                resposta_texto = json_match.group(0)
            try:
                dados_lista = json.loads(resposta_texto)
                if isinstance(dados_lista, list):
                    normalizados = []
                    for item in dados_lista:
                        if 'valor' in item:
                            item['valor'] = float(item['valor'])
                        tipo_n = str(item.get('tipo')).strip()
                        cat_n = str(item.get('categoria', '')).strip().lower()
                        desc_raw = str(item.get('descricao', ''))
                        desc_final = desc_raw if natural_score(desc_raw) >= 2 else naturalize_description(tipo_n, cat_n, desc_raw)
                        desc_final = re.sub(r'\s+', ' ', desc_final).strip()
                        toks = desc_final.split()
                        if len(toks) > 6:
                            desc_final = ' '.join(toks[:6])
                        item['descricao'] = desc_final
                        normalizados.append(item)
                    dedup = {}
                    for item in normalizados:
                        tipo_n = str(item.get('tipo')).strip()
                        valor_n = float(item.get('valor', 0))
                        cat_n = str(item.get('categoria', '')).strip().lower()
                        k = (tipo_n, valor_n, cat_n)
                        cur = dedup.get(k)
                        if cur is None or len(str(item.get('descricao', ''))) < len(str(cur.get('descricao', ''))):
                            dedup[k] = item
                    ai_resultados = list(dedup.values())
                elif isinstance(dados_lista, dict):
                    if 'valor' in dados_lista:
                        dados_lista['valor'] = float(dados_lista['valor'])
                    tipo_n = str(dados_lista.get('tipo')).strip()
                    cat_n = str(dados_lista.get('categoria', '')).strip().lower()
                    desc_raw = str(dados_lista.get('descricao', ''))
                    desc_final = desc_raw if natural_score(desc_raw) >= 2 else naturalize_description(tipo_n, cat_n, desc_raw)
                    desc_final = re.sub(r'\s+', ' ', desc_final).strip()
                    toks = desc_final.split()
                    if len(toks) > 6:
                        desc_final = ' '.join(toks[:6])
                    dados_lista['descricao'] = desc_final
                    ai_resultados = [dados_lista]
            except:
                ai_resultados = []
        if ai_resultados:
            try:
                print(f"[extrair_informacoes_financeiras] fonte=gemini qtd={len(ai_resultados)}")
            except:
                pass
            return ai_resultados
        return []
    except Exception:
        return parse_text_to_transactions(texto_usuario)
