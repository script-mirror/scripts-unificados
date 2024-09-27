from pydantic import BaseModel
import datetime

class ChuvaPrevisaoCriacaoMembro(BaseModel):
    cd_subbacia:int
    dt_prevista: datetime.date
    vl_chuva: float
    modelo: str
    membro: str
    dt_hr_rodada: datetime.datetime
    
