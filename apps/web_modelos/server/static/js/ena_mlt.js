const auxSubmercado = {"N":"Norte", "NE":"Nordeste", "S":"Sul", "SE":"Suldeste"};
const cores = ["#ea5545", "#f46a9b", "#ef9b20", "#edbf33", "#ede15b", "#bdcf32", "#87bc45", "#27aeef", "#b33dc6", "#e60049", "#0bb4ff", "#50e991", "#e6d800", "#9b19f5", "#ffa300", "#dc0ab4", "#b3d4ff", "#00bfa0", "#b30000", "#7c1158", "#4421af", "#1a53ff", "#0d88e6", "#00b7c7", "#5ad45a", "#8be04e", "#ebdc78"];
let cor = 0;

class Dataset {
    constructor(label, data) {
        this.label = label;
        this.data = data;
        // this.backgroundColor = cores[cor];
        // this.borderColor = cores[cor];
        this.borderWidth = 3;
        this.pointRadius = 1;
        this.pointHitRadius = 30;
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


async function getDataMlt(granularidade){
    const meses = ["janeiro", "fevereiro", "março", "abril", "maio", "junho", "julho", "agosto", "setembro", "outubro", "novembro", "dezembro"]
    let datasets = [];
    if(localStorage.getItem(`ena-mlt-${granularidade}`) == undefined){
        const promise = await fetch(`/middle/API/get/ena-mlt/${granularidade}`);
        const response = await promise.json();
        const mlt = response
        localStorage.setItem(`ena-mlt-${granularidade}`, JSON.stringify(mlt));
        
    }
    let enaMlt = JSON.parse(localStorage.getItem(`ena-mlt-${granularidade}`));
    let regiaoAux;
    for(let regiao in enaMlt){
        datasets.push(new Dataset(regiao, Object.values(enaMlt[regiao])))
        regiaoAux = regiao;
        cor++
    }

    const data = new Data(meses, datasets)
    return data;
}

function trocarTipo(chart){
    chart.config.type = chart.config.type == "bar" ? "line" : "bar"; 
    chart.update()
    chart.render()
}
function formatarNumero(num){
    num += "";

    num = num.split('')

    for(let i = num.length-1; i >= 0;i-=3){
        num[i] += "."
    }
    num = num.toString().replaceAll(',', '')
    num = num.substring(0, num.length - 1)

    return num;
}

async function filtrarDatasets(granularidade, darUpdate){
    let indexDatasets = $(`#options-${granularidade}`).val()
    let dados = await getDataMlt(granularidade)
    let dadosFiltrados = {
        datasets: [],
        labels: dados.labels
    }
    for(index in indexDatasets){
        dadosFiltrados.datasets.push(dados.datasets[indexDatasets[index]])
    }
    if(darUpdate){
        let chart = Chart.getChart(`chart-${granularidade}`);
        chart.data = dadosFiltrados;
        chart.update()
    }
    return dadosFiltrados

}

