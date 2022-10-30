from pydantic import BaseModel

class Player(BaseModel):
    PartitionKey: str
    RowKey: str
    name: str
    position: str
    age: int