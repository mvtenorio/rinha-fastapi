from datetime import date
from uuid import UUID, uuid4

from fastapi import FastAPI, HTTPException, Response, status
from pydantic import BaseModel

app = FastAPI()


class PessoaIn(BaseModel):
    apelido: str
    nome: str
    nascimento: date
    stack: list[str] | None = None


class PessoaOut(PessoaIn):
    id: UUID


@app.post("/pessoas", response_model=PessoaOut, status_code=status.HTTP_201_CREATED)
def create_pessoa(response: Response, pessoa_in: PessoaIn):
    pessoa_out = PessoaOut(id=uuid4(), **pessoa_in.dict())
    response.headers["Location"] = app.url_path_for(
        "show_pessoa", pessoa_id=pessoa_out.id
    )

    return pessoa_out


@app.get("/pessoas/{pessoa_id}")
def show_pessoa(pessoa_id: str):
    return {}


@app.get("/pessoas")
def search_pessoas(t: str):
    return {}


@app.get("/contagem-pessoas")
def contagem_pessoas():
    return 0
