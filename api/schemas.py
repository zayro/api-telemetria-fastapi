from pydantic import BaseModel, Field


class ItemIn(BaseModel):
    name: str = Field(min_length=1, max_length=100)


class EventIn(BaseModel):
    name: str = Field(min_length=1, max_length=100)


class EventUpdate(BaseModel):
    name: str = Field(min_length=1, max_length=100)
