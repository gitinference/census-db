from sqlmodel import Field, SQLModel, create_engine
from sqlmodel import SQLModel, Field


class states(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    region_id: int
    division_id: int
    states_fips: int
    name_abv: str
    name_full: str
