import re
import json
from app.services.deepseek import generate_json as ds_generate
from app.services.gemini import get_client, set_cooldown
from app.constants.categories import CATEGORY_LIST
from app.services.rule_based import parse_text_to_transactions, naturalize_description, natural_score
def extrair_informacoes_financeiras(texto_usuario):
    cat_list = ", ".join(CATEGORY_LIST)
    prompt = (
        "Analise a ENTRADA multimodal do usuário (texto/áudio/imagem/PDF) e extraia TODAS as transações financeiras com categorização e descrição profissionais.\n\n"
        f'Entrada do usuário: "{texto_usuario}"\n\n'
        "Retorne SOMENTE um ARRAY JSON com objetos desta estrutura:\n"
        "[\n"
        "  {\n"
        '    "tipo": "0" para despesa ou "1" para receita,\n'
        '    "valor": número decimal,\n'
        f'    "categoria": uma das [{cat_list}],\n'
        '    "descricao": frase breve profissional (3–8 palavras),\n'
        '    "moeda": "BRL"\n'
        "  }\n"
        "]\n\n"
        "Diretrizes de categorização e descrição:\n"
        "• Liberdade e contexto total: interprete o conteúdo e defina categoria e descrição conforme o contexto real (texto, áudio, comprovantes em imagem ou PDFs).\n"
        "• Exemplos de mapeamento: 'feira' → categoria 'alimentacao' com descrição 'Feira'; 'supermercado' → categoria 'alimentacao' com descrição 'Supermercado'.\n"
        "• Reformule termos informais em nomes profissionais: 'mandei pro cara do uber' → categoria 'transporte', descrição 'Transporte por Aplicativo'.\n"
        "• Evite 'outros' ao máximo. Use 'duvida' somente quando realmente não houver pistas, e prefira uma categoria lógica quando houver indícios.\n"
        "• Mantenha padronização de nomes independentemente da modalidade de entrada.\n"
        "• Não inclua números, moeda ou preços na descrição; complete preposições ('de/da/do/para') corretamente; preserve marcas e nomes próprios.\n"
        "• Se houver múltiplas transações, retorne todas. Dedup: itens iguais (tipo/valor/categoria) devem aparecer uma vez, com descrição mais curta e clara.\n"
        "• Saída: SOMENTE o ARRAY JSON, sem texto extra.\n\n"
        "EXEMPLOS:\n"
        '- "gastei 50 no mercado"\n'
        "[\n"
        "  {\n"
        '    "tipo": "0",\n'
        '    "valor": 50.00,\n'
        '    "categoria": "alimentacao",\n'
        '    "descricao": "Gastos com mercado",\n'
        '    "moeda": "BRL"\n'
        "  }\n"
        "]\n\n"
        '- "recebi 1000 de salário"\n'
        "[\n"
        "  {\n"
        '    "tipo": "1",\n'
        '    "valor": 1000.00,\n'
        '    "categoria": "salario",\n'
        '    "descricao": "salário",\n'
        '    "moeda": "BRL"\n'
        "  }\n"
        "]\n\n"
        '- "recebi uma transferência de 500 da minha prima"\n'
        "[\n"
        "  {\n"
        '    "tipo": "1",\n'
        '    "valor": 500.00,\n'
        '    "categoria": "duvida",\n'
        '    "descricao": "Transferência da prima",\n'
        '    "moeda": "BRL"\n'
        "  }\n"
        "]\n\n"
        '- "recebi pix do joao 100"\n'
        "[\n"
        "  {\n"
        '    "tipo": "1",\n'
        '    "valor": 100.00,\n'
        '    "categoria": "duvida",\n'
        '    "descricao": "Transferência de João",\n'
        '    "moeda": "BRL"\n'
        "  }\n"
        "]\n\n"
        '- "vendi uma calça por 50"\n'
        "[\n"
        "  {\n"
        '    "tipo": "1",\n'
        '    "valor": 50.00,\n'
        '    "categoria": "vendas",\n'
        '    "descricao": "Venda de calça",\n'
        '    "moeda": "BRL"\n'
        "  }\n"
        "]\n\n"
        '- "transferi 500 para a minha prima"\n'
        "[\n"
        "  {\n"
        '    "tipo": "0",\n'
        '    "valor": 500.00,\n'
        '    "categoria": "duvida",\n'
        '    "descricao": "Transferência para a prima",\n'
        '    "moeda": "BRL"\n'
        "  }\n"
        "]\n\n"
        '- "gastei 300 com uma peça da moto"\n'
        "[\n"
        "  {\n"
        '    "tipo": "0",\n'
        '    "valor": 300.00,\n'
        '    "categoria": "servicos",\n'
        '    "descricao": "Gastos com peça da moto",\n'
        '    "moeda": "BRL"\n'
        "  }\n"
        "]\n\n"
        '- "gastei 120 internet e ganhei 200 de vendas"\n'
        "[\n"
        "  {\n"
        '    "tipo": "0",\n'
        '    "valor": 120.00,\n'
        '    "categoria": "servicos",\n'
        '    "descricao": "Gastos com internet",\n'
        '    "moeda": "BRL"\n'
        "  },\n"
        "  {\n"
        '    "tipo": "1",\n'
        '    "valor": 200.00,\n'
        '    "categoria": "vendas",\n'
        '    "descricao": "vendas",\n'
        '    "moeda": "BRL"\n'
        "  }\n"
        "]\n\n"
        "RETORNE SOMENTE O JSON ARRAY, SEM TEXTOS ADICIONAIS.\n"
    )
    prompt += '''EXEMPLOS ADICIONAIS:
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
    "descricao": "Transporte por Aplicativo",
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
    "categoria": "duvida",
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
    "categoria": "duvida",
    "descricao": "Transferência da tia",
    "moeda": "BRL"
  }
]
- "transferi 95 para o barbeiro"
[
  {
    "tipo": "0",
    "valor": 95.00,
    "categoria": "duvida",
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
    "categoria": "duvida",
    "descricao": "pix recebido",
    "moeda": "BRL"
  }
]
'''
    prompt += '''EXEMPLOS COM MARCAS E GRANDES VALORES:
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
    "categoria": "duvida",
    "descricao": "Transferência do Banco do Brasil",
    "moeda": "BRL"
  }
]
- "transferi 250 para o Nubank"
[
  {
    "tipo": "0",
    "valor": 250.00,
    "categoria": "duvida",
    "descricao": "Transferência para o Nubank",
    "moeda": "BRL"
  }
]
- "ganhei três milhões"
[
  {
    "tipo": "1",
    "valor": 3000000.00,
    "categoria": "duvida",
    "descricao": "receita",
    "moeda": "BRL"
  }
]
- "recebi 2 mil e 300"
[
  {
    "tipo": "1",
    "valor": 2300.00,
    "categoria": "duvida",
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
- "recebi 1.200 do Itaú"
[
  {
    "tipo": "1",
    "valor": 1200.00,
    "categoria": "duvida",
    "descricao": "Transferência do Itaú",
    "moeda": "BRL"
  }
]
- "recebi 800 do Bradesco"
[
  {
    "tipo": "1",
    "valor": 800.00,
    "categoria": "duvida",
    "descricao": "Transferência do Bradesco",
    "moeda": "BRL"
  }
]
- "transferi 400 para o Santander"
[
  {
    "tipo": "0",
    "valor": 400.00,
    "categoria": "duvida",
    "descricao": "Transferência para o Santander",
    "moeda": "BRL"
  }
]
'''
    # Exemplos extras específicos para reforço de multimodalidade e termos informais
    prompt += '''EXEMPLOS ESPECÍFICOS:
- "mandei pro cara do uber 35"
[
  {
    "tipo": "0",
    "valor": 35.00,
    "categoria": "transporte",
    "descricao": "Transporte por Aplicativo",
    "moeda": "BRL"
  }
]
- "gastei 120 no supermercado"
[
  {
    "tipo": "0",
    "valor": 120.00,
    "categoria": "alimentacao",
    "descricao": "Supermercado",
    "moeda": "BRL"
  }
]
- "recebi 250 pix do João pelo serviço de fotos"
[
  {
    "tipo": "1",
    "valor": 250.00,
    "categoria": "servicos",
    "descricao": "Serviço de fotos",
    "moeda": "BRL"
  }
]
    '''
    try:
        ai_resultados = []
        try:
            client = get_client()
        except:
            client = None
        if client is not None:
            try:
                resposta = client.models.generate_content(
                    model='gemini-2.5-flash',
                    contents=prompt,
                )
                resposta_texto = (getattr(resposta, "text", "") or "").strip()
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
            except Exception as e:
                try:
                    msg = str(e) if e else ""
                    if ("RESOURCE_EXHAUSTED" in msg) or ("429" in msg) or ("Too Many Requests" in msg):
                        try:
                            import os
                            set_cooldown(int(os.getenv("GEMINI_COOLDOWN_SECONDS", "900") or "900"))
                        except:
                            set_cooldown(900)
                    else:
                        pass
                except:
                    pass
                ai_resultados = []
        if not ai_resultados:
            try:
                resposta_texto = ds_generate(
                    prompt,
                    temperature=0.1,
                    max_tokens=512,
                    timeout=25,
                    system_instruction="Você é um extrator financeiro PT-BR. Retorne somente JSON válido sem textos extras."
                ) or ""
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
            except:
                ai_resultados = []
        if not ai_resultados:
            try:
                rb = parse_text_to_transactions(texto_usuario)
            except:
                rb = []
            if rb:
                try:
                    print(f"[extrair_informacoes_financeiras] fonte=local-regra qtd={len(rb)}")
                except:
                    pass
                return rb
        if ai_resultados:
            try:
                out2 = []
                for it in ai_resultados:
                    c = str(it.get("categoria", "outros") or "outros").strip().lower()
                    if c in ("duvida", "outros"):
                        it["pendente_confirmacao"] = True
                        it["confidence_score"] = 0.6
                    else:
                        it["pendente_confirmacao"] = False
                        it["confidence_score"] = 0.9
                    out2.append(it)
                ai_resultados = out2
            except:
                pass
            try:
                print(f"[extrair_informacoes_financeiras] fonte=ia qtd={len(ai_resultados)}")
            except:
                pass
            return ai_resultados
        return []
    except Exception:
        return []
