import json
import re
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from app.services.deepseek import generate_json as ds_generate
from app.config import DEEPSEEK_API_KEY
import requests

def main():
    prompt = (
        "Com base no TEXTO do comprovante abaixo, extraia transações financeiras e "
        "retorne SOMENTE um ARRAY JSON com itens contendo tipo (\"0\" despesa, \"1\" receita), "
        "valor decimal, categoria, descricao breve e moeda \"BRL\".\n"
        "Texto:\n"
        "Pix de R$ 17,00 para Jose C B Pereira em 04/01/2026."
    )
    out = ds_generate(
        prompt,
        temperature=0.0,
        max_tokens=300,
        timeout=25,
        system_instruction="Retorne apenas um ARRAY JSON."
    )
    print("RAW:", out)
    txt = (out or "").replace("```json", "").replace("```", "").strip()
    m = re.search(r"\[\s*\{[\s\S]*?\}\s*\]", txt)
    j = json.loads(m.group(0)) if m else (json.loads(txt) if txt else [])
    print(json.dumps(j, ensure_ascii=False, indent=2))
    if not out:
        headers = {
            "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
            "Content-Type": "application/json",
        }
        body = {
            "model": "deepseek-chat",
            "messages": [
                {"role": "system", "content": "Retorne apenas um ARRAY JSON."},
                {"role": "user", "content": prompt},
            ],
            "temperature": 0.0,
            "max_tokens": 300,
            "stream": False,
        }
        try:
            r = requests.post("https://api.deepseek.com/v1/chat/completions", headers=headers, json=body, timeout=25)
            print("STATUS:", getattr(r, "status_code", None))
            print("BODY:", (getattr(r, "text", "") or "")[:400])
        except Exception as e:
            print("REQUEST_ERROR:", str(e))

if __name__ == "__main__":
    main()
