
const URL_API = "/api/v2"
export const API_ENDPOINTS = {
  rodadasDoDia: `${URL_API}/rodadas`,
  previsaoChuva: {
    post:`${URL_API}/rodadas/chuva/previsao/modelos`,
    subbacia:`${URL_API}/rodadas/chuva/previsao/pesquisa/subbacia`,
    submercado: `${URL_API}/rodadas/chuva/previsao/pesquisa/submercado`,
    bacia: `${URL_API}/rodadas/chuva/previsao/pesquisa/bacia`
  },
  submercados: `${URL_API}/ons/submercados`,
  bacias: `${URL_API}/ons/bacias`,
  subbacias: `${URL_API}/rodadas/subbacias`,
  smap:`${URL_API}/rodadas/smap`  
};

