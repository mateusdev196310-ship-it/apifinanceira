import requests
from app.config import api_url

def processar_mensagem(texto, timeout=10, cliente_id=None):
    url = f"{api_url()}/processar"
    try:
        from app.services.database import ensure_cliente  # lazy import
        payload = {"mensagem": texto}
        if cliente_id:
            payload["cliente_id"] = str(cliente_id)
            try:
                import os
                nm = os.getenv("CLIENTE_NOME") or None
                un = os.getenv("CLIENTE_USERNAME") or None
                if nm or un:
                    payload["cliente_nome"] = nm
                    payload["username"] = un
                ensure_cliente(str(cliente_id), nome=nm, username=un)
            except:
                pass
        response = requests.post(url, json=payload, timeout=timeout)
        if response.status_code == 200:
            data = response.json()
            trans = data.get("transacoes", [])
            dedup = {}
            for item in trans:
                tipo_n = str(item.get('tipo')).strip()
                valor_n = float(item.get('valor', 0))
                cat_n = str(item.get('categoria', '')).strip().lower()
                k = (tipo_n, valor_n, cat_n)
                cur = dedup.get(k)
                if cur is None or len(str(item.get('descricao', ''))) < len(str(cur.get('descricao', ''))):
                    dedup[k] = item
            data["transacoes"] = list(dedup.values())
            data["total"] = len(data["transacoes"])
            return data
        return {"sucesso": False, "erro": f"status {response.status_code}"}
    except Exception as e:
        return {"sucesso": False, "erro": str(e)}
