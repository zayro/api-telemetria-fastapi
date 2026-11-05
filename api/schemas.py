from typing import Any
from pydantic import BaseModel, Field


class ItemIn(BaseModel):
    name: str = Field(min_length=1, max_length=100)


class EventIn(BaseModel):
    name: str = Field(min_length=1, max_length=100)


class EventUpdate(BaseModel):
    name: str = Field(min_length=1, max_length=100)


class ProtocolRAGIn(BaseModel):
    query: str = Field(
        ...,
        min_length=1,
        max_length=2000,
        description="Consulta o pregunta sobre el corpus de protocolos",
    )
    top_k: int = Field(
        3,
        ge=1,
        le=20,
        description="Cantidad máxima de fragmentos o protocolos a recuperar",
    )
    category: str | None = Field(
        None,
        description="Filtro opcional por categoría (ej. seguridad, redes, operaciones)",
    )
    generate_answer: bool = Field(
        True,
        description="Si es True, conecta y consulta a Gemini para generar respuesta con RAG",
    )
    temperature: float = Field(
        0.2,
        ge=0.0,
        le=1.0,
        description="Temperatura de generación para el modelo Gemini",
    )


class ProtocolDocumentOut(BaseModel):
    id: str
    title: str
    content: str
    source_file: str
    category: str
    doc_type: str = "text"
    metadata: dict[str, Any] = Field(default_factory=dict)


class ProtocolSourceOut(BaseModel):
    protocol_id: str
    title: str
    content: str
    source_file: str
    category: str
    doc_type: str = "text"
    score: float
    metadata: dict[str, Any] = Field(default_factory=dict)


class ProtocolRAGOut(BaseModel):
    ok: bool = True
    query: str
    answer: str | None = None
    model: str | None = None
    total_sources: int
    sources: list[ProtocolSourceOut]


class IngestResponse(BaseModel):
    ok: bool = True
    source_file: str
    doc_type: str
    chunks_count: int
    protocol_ids: list[str]
