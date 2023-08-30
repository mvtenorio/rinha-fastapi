from datetime import date
from uuid import UUID, uuid4

from fastapi import Depends, FastAPI, HTTPException, Response, status
from psycopg.errors import UniqueViolation
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool
from pydantic import BaseModel, Field, constr
from redis import Redis

app = FastAPI()


async def get_db():
    pool = AsyncConnectionPool(conninfo="postgresql://postgres:postgres@db:5432/rinha")
    async with pool.connection() as conn:
        yield conn


async def get_redis():
    with Redis(host="redis", port=6379, decode_responses=True) as redis:
        yield redis


class PessoaIn(BaseModel):
    apelido: constr(max_length=32)
    nome: constr(max_length=100)
    nascimento: date
    stack: list[str] | None = None


class PessoaOut(PessoaIn):
    id: UUID = Field(default_factory=uuid4)

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
async def create_pessoa(
    pessoa_in: PessoaIn,
    response: Response,
    db=Depends(get_db),
    redis=Depends(get_redis),
):
    pessoa_out = PessoaOut(**pessoa_in.dict())

    try:
        await db.execute(
            "INSERT INTO pessoas (id, apelido, nome, nascimento, stack) "
            "VALUES (%(id)s, %(apelido)s, %(nome)s, %(nascimento)s, %(stack)s)",
            pessoa_out.model_dump(),
        )
    except UniqueViolation:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Já existe uma pessoa com esse apelido",
        )

    redis.set(f"pessoas:{pessoa_out.id}", pessoa_out.model_dump_json())

    response.headers["Location"] = app.url_path_for(
        "show_pessoa", pessoa_id=pessoa_out.id
    )

    return pessoa_out


@app.get("/pessoas/{pessoa_id}", response_model=PessoaOut)
async def show_pessoa(pessoa_id: UUID, db=Depends(get_db), redis=Depends(get_redis)):
    pessoa_from_cache = redis.get(f"pessoas:{pessoa_id}")
    if pessoa_from_cache:
        return PessoaOut.model_validate_json(pessoa_from_cache)

    cursor = await db.cursor(row_factory=dict_row).execute(
        "SELECT id, apelido, nome, nascimento, stack FROM pessoas p WHERE p.id = %(id)s LIMIT 1",
        {"id": str(pessoa_id)},
    )
    pessoa = await cursor.fetchone()

    if not pessoa:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Pessoa não encontrada"
        )

    pessoa_out = PessoaOut.from_dict(pessoa)
    redis.set(f"pessoas:{pessoa_out.id}", pessoa_out.model_dump_json())

    return pessoa_out


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
