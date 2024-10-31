from pydantic import BaseModel
import datetime

class ChuvaObsReq(BaseModel):
    cd_subbacia:int
    dt_observado:datetime.datetime
    vl_chuva:float
