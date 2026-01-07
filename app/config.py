import os

def _load_dotenv():
    candidates = [
        os.path.join(os.getcwd(), ".env"),
        os.path.join(os.getcwd(), "__restore_temp", "api_financeira", ".env"),
    ]
    for path in candidates:
        if not os.path.exists(path):
            continue
        try:
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    s = line.strip()
                    if not s or s.startswith("#"):
                        continue
                    if "=" in s:
                        k, v = s.split("=", 1)
                        k = k.strip()
                        v = v.strip().strip('"').strip("'")
                        if k and (k not in os.environ or not os.environ[k]):
                            os.environ[k] = v
        except:
            pass

_load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
API_HOST = os.getenv("API_HOST") or os.getenv("HOST") or "0.0.0.0"
API_PORT = int(os.getenv("API_PORT") or os.getenv("PORT") or "5000")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY")
API_URL_OVERRIDE = os.getenv("API_URL")

def api_url():
    if API_URL_OVERRIDE:
        return API_URL_OVERRIDE
    return f"http://{API_HOST}:{API_PORT}"
