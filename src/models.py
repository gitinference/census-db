from sqlmodel import Field, SQLModel, create_engine
from sqlmodel import SQLModel, Field


class states(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    region_id: int = Field(default=None, foreign_key="regions.id")
    division_id: int = Field(default=None, foreign_key="divisions.id")
    states_fips: int
    name_abv: str
    name_full: str


class regions(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    name_abv: str
    name_full: str


class divisions(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    region_id: int = Field(default=None, foreign_key="regions.id")
    name_abv: str
    name_full: str
