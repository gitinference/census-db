from re import T
from pyarrow import table
from sqlmodel import Field, SQLModel


class regions(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    name_abv: str
    name_full: str


class divisions(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    region_id: int = Field(default=None, foreign_key="regions.id")
    name_abv: str
    name_full: str


class states(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    region_id: int = Field(default=None, foreign_key="regions.id")
    division_id: int = Field(default=None, foreign_key="divisions.id")
    name_abv: str
    name_full: str


class county(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    state_id: int = Field(default=None, foreign_key="states.id", primary_key=True)
    name_full: str
