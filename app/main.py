from fastapi import FastAPI
from app.routers import geral, linhas, estudos, ocorrencias, bairros, concessionarias

app = FastAPI(
    title="DashMobi API",
    description="API para fornecer dados analíticos de mobilidade urbana de Belo Horizonte.",
    version="1.0.0"
)

app.include_router(geral.router)
app.include_router(linhas.router)
app.include_router(ocorrencias.router)
app.include_router(bairros.router)
app.include_router(concessionarias.router)
app.include_router(estudos.router)


@app.get("/")
def read_root():
    return {"message": "Bem-vindo à DashMobi API!"}
