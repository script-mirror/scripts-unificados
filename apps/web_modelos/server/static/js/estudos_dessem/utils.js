const coresDessem = {
    geracao_termica: "#d61f03",
    geracao_renovaveis: "#119e37",
    geracao_hidreletrica: "#0d6abf",
    intercambio: "#ED956E",
    demanda: "#000000",
    pld: "#6b007b",
    cmo: "#6b007b",
    N: "#118dff",
    NE: "#12239e",
    S: "#e66c37",
    SE: "#6b007b",
    limite_utilizado: "#41a4ff",
    limite_superior: "#d64550",
    limite_inferior: "#0de75d",
    gtmax: "#e044a7"
}
function formatNumber(num) {
    return (num + "").length == 1 ? `0${num}:00` : `${num}:00`;
}
function arraySum(array) {
    return array.reduce((partialSum, a) => partialSum + a, 0);
}

class Dataset {
    constructor(label, data, fill, color, type) {
        this.label = label;
        this.data = data;
        this.pointStyle = false;
        this.borderColor = color;
        this.backgroundColor = color;
        this.fill = fill;
        this.type = type;
        this.borderWidth = 1.5;
        this.stack = label == 'gtmax' ? 'stack 2' : label == 'geracao termica' ? 'stack 1': label == 'demanda' ? 'stack 3' : 'stack 4'
    }
}

class Options {
    constructor(xTitle, yTitle, stacked) {
        this.animation = true;
        this.plugins = {
            tooltip: true, legend: { display: true },
            hover: { mode: null }
        }
        this.scales = {
            x: {
                title: { display: true, text: "Data/Hora" },
                scaleLabel: { display: true, labelString: "Data" },
                ticks: { maxTicksLimit: 30 },
                type: 'time',
                time: { displayFormats: { quarter: 'MMM YYYY', hour: "HH:00", minute:"MM", second:"SS" } },
                title: { display: true, text: xTitle },
                scaleLabel: { display: true, labelString: "Data" },
                stacked: stacked,
                categoryPercentage: 1.0,
                barPercentage: 1.0

            },
            y: {
                title: { display: true, text: yTitle },
                stacked: stacked,
            }
        }
        this.interaction = {
            intersect: false,
            mode: 'index',
        },
        this.tooltips = {
            mode: 'x'
        },
        this.hover = {
            intersect: false,
            mode: 'x',
            axis: 'x',
        },

        this.responsive = true;
        this.maintainAspectRatio = false;
    }
}

function toObject(keys, values) {
    let result = {};
    keys.forEach((key, i) => result[key] = values[i]);
    return result;
}


// DADOS ALEATORIOS PARA TESTE
function fakeRequest(dataInicial, dataFinal) {
    let response = {};
    let labels = [];
    let qtdDias = moment(dataFinal).diff(moment(dataInicial), 'days');
    for (let i = 0; i < qtdDias; i++) {
        let proximaData = moment(dataInicial).add(i, 'days');
        for (let j = 0; j < 24; j++) {
            let proximaDataHora = moment(proximaData.format()).add(j, 'hours');
            labels.push(proximaDataHora.format('YYYY-MM-DD HH:mm:ss'));
        }
    }
    let dadosDemanda = Array.from({ length: labels.length }, () => Math.floor(Math.random() * 30) + 15);
    let dadosPld = Array.from({ length: labels.length }, () => Math.floor(Math.random() * 600) + 100);
    let dadosIntercambio = [];
    let dadosHidreletrica = [];
    let dadosRenovaveis = [];
    let dadosTermica = [];
    let dadosGtMax = [];
    
    let dadosLimiteInferior = [];
    let dadosLimiteSuperior = [];
    dadosDemanda.forEach((e) => {
        dadosHidreletrica.push(e);
        dadosRenovaveis.push(e / 1.5);
        dadosIntercambio.push(0);
        dadosTermica.push(e / 3);
        dadosGtMax.push((e * 6) + 100);
        dadosLimiteInferior.push(0)
        dadosLimiteSuperior.push(6300)
    })
    response["pld"] = toObject(labels, dadosPld);
    response["gtmax"] = toObject(labels, dadosGtMax);

    response["termica"] = toObject(labels, dadosTermica);
    response["renovaveis"] = toObject(labels, dadosRenovaveis);
    response["hidreletrica"] = toObject(labels, dadosHidreletrica);
    response["intercambio"] = toObject(labels, dadosIntercambio);
    response["demanda"] = toObject(labels, dadosDemanda);

    response["limite_inferior"] = toObject(labels, dadosLimiteInferior);
    response["limite_superior"] = toObject(labels, dadosLimiteSuperior);



    response["subsistema"] = "Todos";
    return response;
}

// DADOS ALEATORIOS PARA TESTE

// { MODELO PLANEJADO DOS DADOS
// subsistema: SE,
// termica:{
//  "2024-01-01 00:00": 45.0,
// "2024-01-02 01:00": 76.0,
// },
// eolica:{}
// }
