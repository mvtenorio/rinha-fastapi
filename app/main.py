from datetime import date
from uuid import UUID, uuid4

from fastapi import Depends, FastAPI, HTTPException, Response, status
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool
from pydantic import BaseModel, constr

app = FastAPI()


async def get_db():
    pool = AsyncConnectionPool(conninfo="postgresql://postgres:postgres@db:5432/rinha")
    async with pool.connection() as conn:
        yield conn


class PessoaIn(BaseModel):
    apelido: constr(max_length=32)
    nome: constr(max_length=100)
    nascimento: date
    stack: list[str] | None = None


class PessoaOut(PessoaIn):
    id: UUID

    @classmethod
    def from_dict(cls, pessoa):
        return cls(
            id=pessoa["id"],
            apelido=pessoa["apelido"],
            nome=pessoa["nome"],
            nascimento=pessoa["nascimento"],
            stack=pessoa["stack"][1:-1].split(",") if pessoa["stack"] else None,
        )


@app.post("/pessoas", response_model=PessoaOut, status_code=status.HTTP_201_CREATED)
async def create_pessoa(pessoa_in: PessoaIn, response: Response, db=Depends(get_db)):
    pessoa_out = PessoaOut(id=uuid4(), **pessoa_in.dict())

    await db.execute(
        "INSERT INTO pessoas (id, apelido, nome, nascimento, stack) "
        "VALUES (%(id)s, %(apelido)s, %(nome)s, %(nascimento)s, %(stack)s)",
        pessoa_out.model_dump(),
    )

    response.headers["Location"] = app.url_path_for(
        "show_pessoa", pessoa_id=pessoa_out.id
    )

    return pessoa_out



@app.get("/pessoas/{pessoa_id}", response_model=PessoaOut)
async def show_pessoa(pessoa_id: UUID, db=Depends(get_db)):
    cursor = await db.cursor(row_factory=dict_row).execute(
        "SELECT id, apelido, nome, nascimento, stack FROM pessoas p WHERE p.id = %(id)s LIMIT 1",
        {"id": str(pessoa_id)},
    )
    pessoa = await cursor.fetchone()

    if not pessoa:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Pessoa não encontrada"
        )

    return PessoaOut.from_dict(pessoa)


@app.get("/pessoas", response_model=list[PessoaOut])
async def search_pessoas(t: str, db=Depends(get_db)):
    cursor = await db.cursor(row_factory=dict_row).execute(
        "SELECT id, apelido, nome, nascimento, stack FROM pessoas p WHERE p.busca_trgm LIKE %(t)s",
        {"t": f"%{t}%"},
    )
    pessoas = await cursor.fetchall()

    return [PessoaOut.from_dict(pessoa) for pessoa in pessoas]


@app.get("/contagem-pessoas", response_model=int)
async def contagem_pessoas(db=Depends(get_db)):
    cursor = await db.execute("SELECT COUNT(*) FROM pessoas")
    result = await cursor.fetchone()
    return result[0]

