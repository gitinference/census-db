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
    state_id: int = Field(default=None, foreign_key="states.id")
    name_full: str


class track(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    county_id: int = Field(default=None, foreign_key="county.id")
    state_id: int = Field(default=None, foreign_key="states.id")
    name_full: str


class year_table(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    year: int


class geo_table(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    geo_name: str
    geo_url: str


class concept_table(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    name: str


class required_table(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    name: str


class dataset_type(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    type: str


class predicate_table(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    predicate_type: str


class variable_table(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    name: str
    label: str
    cconcept_id: int = Field(default=None, foreign_key="concept_table.id")
    required_id: int = Field(default=None, foreign_key="required_table.id")
    limit: int | None = None
    predicate_id: int = Field(default=None, foreign_key="predicate_table.id")


class variable_interm(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    interm_id: int
    year_id: int = Field(default=None, foreign_key="year_table.id")
    var_id: int = Field(default=None, foreign_key="variable_table.id")


class year_interm(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    year_id: int = Field(default=None, foreign_key="year_table.id")
    interm_id: int


class geo_interm(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    geo_id: int = Field(default=None, foreign_key="geo_table.id")
    interm_id: int


class data(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    year_interm_id: int = Field(default=None, foreign_key="year_interm.interm_id")
    type_id: int = Field(default=None, foreign_key="dataset_type.id")
    geo_interm_id: int = Field(default=None, foreign_key="geo_interm.interm_id")
    var_interm_id: int = Field(default=None, foreign_key="variable_interm.interm_id")
