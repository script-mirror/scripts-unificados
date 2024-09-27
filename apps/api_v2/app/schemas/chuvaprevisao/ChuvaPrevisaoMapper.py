from sys import path
from dataclasses import dataclass

path.insert(1,"/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/api_v2")
from app.schemas.chuvaprevisao.ChuvaPrevisaoCriacao import ChuvaPrevisaoCriacao
from app.schemas.chuvaprevisao.ChuvaPrevisaoResposta import ChuvaPrevisaoResposta

@dataclass
class ChuvaPrevisaoMapper():
    def resposta_para_criacao(resposta: ChuvaPrevisaoResposta):
        criacao = ChuvaPrevisaoCriacao()
        criacao.cd_subbacia = resposta.id
        criacao.dt_prevista = resposta.dt_prevista
        criacao.vl_chuva = resposta.vl_chuva
        
    pass