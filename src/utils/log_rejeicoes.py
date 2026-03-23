import json
from datetime import datetime
import os

LOG_DIR = "logs"  # pode ser local antes de enviar para S3
os.makedirs(LOG_DIR, exist_ok=True)

def log_rejeicao(entidade: str, linha: int, campo: str, valor, motivo: str):
    """Log estruturado de registro rejeitado"""
    log_file = os.path.join(LOG_DIR, f"validacao_{entidade}_{datetime.today().strftime('%Y-%m-%d')}.jsonl")
    log_entry = {
        "linha": linha,
        "entidade": entidade,
        "campo": campo,
        "valor": valor,
        "motivo": motivo
    }
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(json.dumps(log_entry, ensure_ascii=False) + "\n")