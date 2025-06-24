from fastapi import FastAPI
from typing import List, Dict, Any

app = FastAPI(title="DashMobi API")


@app.get("/api/v1/concessionarias", response_model=List[Dict[str, Any]])
def get_concessionarias():
    """
    Retorna uma lista de todas as concessionárias.
    """
    # Exemplo de dados
    dados_mock = [
        {"id_concessionaria": 1, "nome_concessionaria": "Consórcio Pampulha"},
        {"id_concessionaria": 2, "nome_concessionaria": "Consórcio BHLeste"},
    ]
    return dados_mock


# Rota raiz
@app.get("/")
def read_root():
    return {"projeto": "DashMobi API"}
