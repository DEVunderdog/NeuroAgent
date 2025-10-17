from pydantic import BaseModel
from schema.schema import ClientRoleEnum

class PayloadData(BaseModel):
    user_id: int
    role: ClientRoleEnum


class TokenData(PayloadData):
    pass

class ApiData(PayloadData):
    pass

