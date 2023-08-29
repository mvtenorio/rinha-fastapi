from datetime import date
from uuid import UUID, uuid4

from fastapi import Depends, FastAPI, HTTPException, Response, status
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool
from pydantic import BaseModel

app = FastAPI()


async def get_db():
    pool = AsyncConnectionPool(conninfo="postgresql://postgres:postgres@db:5432/rinha")
    async with pool.connection() as conn:
        yield conn


class PessoaIn(BaseModel):
    apelido: str
    nome: str
    nascimento: date
    stack: list[str] | None = None


class PessoaOut(PessoaIn):
    id: UUID


class PessoaNotFound(Exception):
    ...


async def insert_pessoa(pessoa_in, db):
    pessoa_out = PessoaOut(id=uuid4(), **pessoa_in.dict())

    await db.execute(
        "INSERT INTO pessoas (id, apelido, nome, nascimento, stack) "
        "VALUES (%(id)s, %(apelido)s, %(nome)s, %(nascimento)s, %(stack)s)",
        pessoa_out.model_dump(),
    )
    return pessoa_out


async def select_pessoa(pessoa_id, db):
    cursor = await db.cursor(row_factory=dict_row).execute(
        "SELECT id, apelido, nome, nascimento, stack FROM pessoas p WHERE p.id = %(id)s LIMIT 1",
        {"id": str(pessoa_id)},
    )
    pessoa = await cursor.fetchone()

    if not pessoa:
        raise PessoaNotFound()

    return PessoaOut(
        id=pessoa_id,
        apelido=pessoa["apelido"],
        nome=pessoa["nome"],
        nascimento=pessoa["nascimento"],
        stack=pessoa["stack"][1:-1].split(",") if pessoa["stack"] else None,
    )


@app.post("/pessoas", response_model=PessoaOut, status_code=status.HTTP_201_CREATED)
async def create_pessoa(pessoa_in: PessoaIn, response: Response, db=Depends(get_db)):
    pessoa_out = await insert_pessoa(pessoa_in, db)

    response.headers["Location"] = app.url_path_for(
        "show_pessoa", pessoa_id=pessoa_out.id
    )

    return pessoa_out


@app.get("/pessoas/{pessoa_id}", response_model=PessoaOut)
async def show_pessoa(pessoa_id: UUID, db=Depends(get_db)):
    try:
        return await select_pessoa(pessoa_id, db)
    except PessoaNotFound:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Pessoa não encontrada"
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
