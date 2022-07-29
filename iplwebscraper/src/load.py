"""Load functions to save the transformed_data csv to a database.

Functions:
    setup()
    load_csv(str) -> list
    define_tables(MetaData) -> Table
    prepare_batters(list) -> Batters
    convert_to_mappings(list) -> list
    load(str) -> None

Classes:
    Batters(Base)
"""
# pylint: disable=redefined-outer-name
# pylint: disable=unused-variable
from sqlalchemy import MetaData, Table, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pandas as pd


base = declarative_base()


def setup():
    """Sets up the sqlacademy engine, session, base and metadata.

    Returns:
        Engine: sqlalchemy engine.
        Session: sqlalchemy session.
        MetaData: sqlalchemy metadata.
        _DeclarativeBase: sqlalchemy base.
    """
    engine = create_engine("sqlite:///data/ipl_2022.db")
    sess = sessionmaker(bind=engine)
    session = sess()
    base = declarative_base()
    meta = MetaData()
    return engine, session, meta, base


def load_csv(file_name: str) -> list:
    """Loads a csv and returns the data and headers.

    Args:
        file_name (str): The file path and name for the csv.

    Returns:
        list: A list of the file records.
    """
    csv_data: pd.DataFrame = pd.read_csv(file_name)
    data: list = csv_data.values.tolist()
    return data


def define_tables(meta: MetaData) -> Table:
    """Defines the tables to be created.

    Args:
        meta (MetaData): sqlalchemy metadata.

    Returns:
        Table: the Batters table schema.
    """
    batters = Table(
        "Batters",
        meta,
        Column("player", String(100), primary_key=True, nullable=False),
        Column("runs", Integer),
        Column("balls", Integer),
        Column("fours", Integer),
        Column("sixes", Integer),
        Column("not_out", Integer),
    )
    return batters


class Batters(base):
    """Defines the table for SQLAlchemy.

    Args:
        base (_DeclarativeBase): Needed for SQLAlchemy.
    """
    __tablename__ = "Batters"
    __table_args__ = {"sqlite_autoincrement": True}
    id = Column(Integer, primary_key=True, nullable=False)
    player = Column(String(100), nullable=False)
    runs = Column(Integer)
    balls = Column(Integer)
    fours = Column(Integer)
    sixes = Column(Integer)
    not_out = Column(Integer)


def prepare_batters(row: list) -> Batters:
    """Converts a list into a table mapping.

    Args:
        row (list): A list containing record values.

    Returns:
        Batters: A table mapping of the list passed in
    """
    record = {
        "player": row[0],
        "runs": row[1],
        "balls": row[2],
        "fours": row[3],
        "sixes": row[4],
        "not_out": row[5],
    }
    return Batters(**record)


def convert_to_mappings(data: list) -> list:
    """Converts batting data to mappings.

    Args:
        data (list): A 2D list of batter information

    Returns:
        list: mappings of batter information
    """
    mapped_data = [prepare_batters(row) for row in data]
    return mapped_data


def load(file_name: str):
    """Loads the csv into a local sqlite db."

    Args:
        file_name (str): The location of the csv.
    """
    engine, session, meta, base = setup()
    batters = define_tables(meta)
    meta.create_all(engine)
    data = load_csv(file_name)
    tabled_data = convert_to_mappings(data)
    session.add_all(tabled_data)
    session.commit()
