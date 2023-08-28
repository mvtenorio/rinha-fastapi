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
def create_pessoa(pessoa_in: PessoaIn, response: Response):
    pessoa_out = PessoaOut(id=uuid4(), **pessoa_in.dict())
    response.headers["Location"] = app.url_path_for(
        "show_pessoa", pessoa_id=pessoa_out.id
    )

    return pessoa_out


@app.get("/pessoas/{pessoa_id}", response_model=PessoaOut)
def show_pessoa(pessoa_id: UUID):
    return PessoaOut(
        id=pessoa_id,
        nome="Nome",
        apelido="Apelido",
        nascimento="2023-08-28",
        stack=["Python"],
    )


@app.get("/pessoas", response_model=list[PessoaOut])
def search_pessoas(t: str):
    return [
        PessoaOut(
            id="f7379ae8-8f9b-4cd5-8221-51efe19e721b",
            apelido="josé",
            nome="José Roberto",
            nascimento="2000-10-01",
            stack=["C#", "Node", "Oracle"],
        ),
        PessoaOut(
            id="5ce4668c-4710-4cfb-ae5f-38988d6d49cb",
            apelido="ana",
            nome="Ana Barbosa",
            nascimento="1985-09-23",
            stack=["Node", "Postgres"],
        ),
    ]


@app.get("/contagem-pessoas")
def contagem_pessoas():
    return 0
