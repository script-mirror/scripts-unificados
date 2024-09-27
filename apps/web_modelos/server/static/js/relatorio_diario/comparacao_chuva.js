const auxSubmercado = { "N": "Norte", "NE": "Nordeste", "S": "Sul", "SE": "Suldeste" };

// const cores = ["#F1C80E", "#288EA5"];
let cor = 0;

class DatasetComparacaoChuva {
    constructor(label, data, cor) {
        this.label = label;
        this.data = data;
        this.backgroundColor = cor;
        this.borderColor = cor;
        this.borderWidth = 5;
        this.fill = false;
        this.hidden = false;
    }
}
class Data {
    constructor(labels, datasets) {
        this.labels = labels;
        this.datasets = datasets;
    }
}

async function getChuvaObservada(dataRodada) {
    let dadosChuva;
    let parametrosUrl;
    if (sessionStorage.getItem(`chuva_observado_${dataRodada}`) == null) {
        parametrosUrl = new URLSearchParams({
            granularidade: "bacia",
            data_inicial: dataRodada
        });

        let promise = await fetch("/middle/API/getchuvaObservada?" + parametrosUrl);
        let response = await promise.json();
        let dadosChuva = await response;
        sessionStorage.setItem(`chuva_observado_${dataRodada}`, JSON.stringify(await dadosChuva));
    }

    dadosChuva = sessionStorage.getItem(`chuva_observado_${dataRodada}`);
    dadosChuva = JSON.parse(dadosChuva);
    return dadosChuva;
}

async function getChuvaPrevisao(dataRodada, modelo) {
    const formatoData = "YYYY-MM-DD"
    dataRodada = moment(dataRodada, formatoData).subtract(1, 'days').format(formatoData)
    let dadosChuva;
    let parametrosUrl;
    if (sessionStorage.getItem(`chuva_${modelo}_${dataRodada}`) == null) {
        parametrosUrl = new URLSearchParams({
            granularidade: "bacia",
            dt_rodada: dataRodada,
            modelo: modelo
        });

        let promise = await fetch("/middle/API/get/dados-rodadas-chuva?" + parametrosUrl)
        if(promise.status == '200'){

            let response = await promise.json()
            let dadosChuva = await response
            sessionStorage.setItem(`chuva_${modelo}_${dataRodada}`, JSON.stringify(await dadosChuva['PREVISAO_CHUVA'][0]['valores']));
        }else{
            return '204';
        }
    }
    dadosChuva = sessionStorage.getItem(`chuva_${modelo}_${dataRodada}`);
    dadosChuva = JSON.parse(dadosChuva);
    return dadosChuva;
}

async function getChartDataComparacao(dataRodada, modelo) {
    const bacias = ['DOCE', 'GRANDE', 'IGUAÇU', 'JACUÍ', 'AMAZONAS', 'PARAÍBA DO SUL', 'ALTO PARANÁ', 'PARANAÍBA', 'PARANAPANEMA', 'SÃO FRANCISCO (NE)', 'TIETÊ', 'TOCANTINS (N)', 'URUGUAI', 'XINGU']
    let dadosPrevisao = await getChuvaPrevisao(dataRodada, modelo);
    if(dadosPrevisao == '204'){
        return '204';
    }
    let dadosObservado = await getChuvaObservada(dataRodada);


    for (let bacia in dadosPrevisao) {
        if (!bacias.includes(bacia)) {
            delete dadosPrevisao[bacia];
            delete dadosObservado[bacia];
        }
    }

    for (let data in dadosPrevisao[Object.keys(dadosPrevisao)[0]]) {
        for (let bacia in dadosPrevisao) {
            if (!Object.keys(dadosObservado[bacia]).includes(data)) {
                delete dadosPrevisao[bacia][data];
            }
        }
    }
    let mmChuvaPrevisao = []
    let mmChuvaObservado = []
    for(let bacia in dadosPrevisao){
        let somaMmChuvaPrevisao = 0;
        let somaMmChuvaObservado = 0;
        for(let data in dadosPrevisao[bacia]){
            somaMmChuvaPrevisao += dadosPrevisao[bacia][data]
            somaMmChuvaObservado += dadosObservado[bacia][data]
        }
        mmChuvaPrevisao.push(somaMmChuvaPrevisao);
        mmChuvaObservado.push(somaMmChuvaObservado);
    }



    let label = Object.keys(dadosObservado);
    let datasets = [];

    datasets.push(new DatasetComparacaoChuva(`Previsto (${modelo})`, mmChuvaPrevisao, '#F1C80E'))
    datasets.push(new DatasetComparacaoChuva("Observado", mmChuvaObservado, '#288EA5'))

    return new Data(label, datasets)
}
function trocarTipo(chart) {
    chart.chart.config.type = chart.chart.config.type == "bar" ? "line" : "bar";
    chart.update()
}
function formatarNumero(num) {
    num += "";

    num = num.split('')

    for (let i = num.length - 1; i >= 0; i -= 3) {
        num[i] += "."
    }
    num = num.toString().replaceAll(',', '')
    num = num.substring(0, num.length - 1)

    return num;
}


async function updateChartComparacao(chart, data, modelo){
    const divModelo = document.getElementById(`chart-${modelo}`)
    let dados = await getChartDataComparacao(data, modelo);
    if(dados == '204'){
        // alert('204')
        divModelo.style.border = '1px solid red';
    }else{
    chart.config.data = dados;
    divModelo.style.border = 'none';
    chart.update()
}
}