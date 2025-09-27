from pydantic import BaseModel


class MongoDBConfig(BaseModel):
    uri : str
    db: str


class MongoDBs(BaseModel):
    dag_workflow : MongoDBConfig

class DBsConfig(BaseModel):
    mongodb: MongoDBs

class Config(BaseModel):
    db: DBsConfig

class Error:
    def __init__(self, message: str):
        self.message = message