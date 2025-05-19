moment.locale('pt-br');
const subsistemas = ['SE', 'S', 'NE', 'N']
let charts = []
const controller = new AbortController();
const signal = controller.signal;
async function getDadosDessem(apiPath, dataInicial, dataFinal) {
    let local;
    local = apiPath == 'limites-intercambio' ? getRe() : getSubsistema();
    local = local.replace('/', 'BARRA')

    let url;
    if (dataInicial == undefined || dataFinal == undefined) {
        const datas = await getDatas()
        dataInicial = datas['range_datas'][datas['range_datas'].length - 2];
        dataFinal = datas['range_datas'][datas['range_datas'].length - 1];
    }
    let queryParams = new URLSearchParams({
        dtInicial: dataInicial,
        dtFinal: dataFinal
    })
    console.log(apiPath)
    if(local == 'todos'){
        url = `/middle/API/get/${apiPath}?${queryParams}`
    }else{
        url = `/middle/API/get/${apiPath}/${local}?${queryParams}`
    }
    let promise = await fetch(url, {signal});
    const response = await promise.json();
    return response;
}
async function getDatas(){
    if(sessionStorage.getItem('datas-dessem') == undefined || sessionStorage.getItem('datas-dessem') == 'undefined'){
        const url = '/middle/API/get/datas-atualizadas-dessem'
        const promise = await fetch(url, {signal});
        const response = await promise.json();
        sessionStorage.setItem('datas-dessem', JSON.stringify(response));        
    }
    return JSON.parse(sessionStorage.getItem('datas-dessem'))
}
function plotarDonutGeracao(dados) {
    const ctx = document.getElementById('donut-geracao');
    const data = {
        labels: ['Renovaveis', 'Termica', 'Hidreletrica'],
        datasets: [
            { data: [
                dados['geracao_renovaveis'], 
                dados['geracao_termica'], 
                dados["geracao_hidreletrica"]
            ],
                hoverOffset: 4, backgroundColor: ["#119E37", "#D61F03", "#0d6abf"] }
        ]
    };
    const config = {
        type: 'doughnut',
        data: data,
        options: {
            plugins: {
                legend: { display: false },
                tooltip: { position: 'nearest'}
            },
            responsive: true, maintainAspectRatio: false
        }
    };
    new Chart(ctx, config);
}
function plotarLine(ctx, dados, nome) {
    dados = dados[nome];

    let datasets = [];
    for(let subsistema of subsistemas){
        datasets.push(new Dataset(subsistema, dados[subsistema], false, coresDessem[subsistema], "line"));
    }
    const data = {
        datasets: datasets
    };
    const config = {
        type: "line", data: data, options: new Options("Data/Hora", nome, false)
    };
    return new Chart(ctx, config);
}
  
function plotarDemandaGeracaoIntercambio(ctx, dados, nome) {
    const labels = ['geracao_termica', 'geracao_renovaveis', 'geracao_hidreletrica', 'intercambio', 'demanda'];
    let datasets = [];
    for (let tipo in labels) {
                datasets[tipo] = new Dataset(labels[tipo], dados[labels[tipo]], false, coresDessem[labels[tipo]], labels[tipo] == "demanda" ? "line" : "bar");
    }
    const data = {
        datasets: datasets
    };
    const config = {
        data: data, options: new Options("Data/Hora", nome, true)
    };
    return new Chart(ctx, config);
}

async function loadGraficos() {
    let loadingDiv = document.getElementById('loading');
    let datas = await getDatas();
    datas = datas['range_datas']
    loadingDiv.style.display = 'block';
    const dadosGeracao = await getDadosDessem('demanda-geracao-intercambio');
    const dadosPld = await getDadosDessem('pld-cmo');
    // const dadosGeracaoTotal = await getDadosDessem('total-geracao');
    const datePickerInicio = document.getElementById("data-inicio");
    const datePickerFim = document.getElementById("data-final");
    
    charts.push(plotarDemandaGeracaoIntercambio(document.getElementById('line-geracao'), dadosGeracao, 'Geracao Termica Renovavel Hidreletrica Intercambio Demanda'));
    charts.push(plotarLine(document.getElementById('line-pld'), dadosPld, "pld"));
    // plotarDonutGeracao(dadosGeracaoTotal);

    new Slider(datas,document.getElementById("slider"),datePickerInicio, datePickerFim, filtrarGraficos);
    loadingDiv.style.display = 'none';
    
}

async function filtrarGraficos() {
    let loadingDiv = document.getElementById('loading');
    const inicio =  document.getElementById("data-inicio").value;
    const fim = document.getElementById("data-final").value;
    loadingDiv.style.display = 'block';
    await filtrarGraficoGeracao(inicio, fim)
    await filtrarGraficoPld(inicio, fim)
    await filtrarGeracaoTermica(inicio, fim)
    await filtrarLimiteIntercambio(inicio, fim)
    loadingDiv.style.display = 'none';

}

async function filtrarGraficoGeracao(inicio, fim){
    const tiposLabel = ['geracao_termica', 'geracao_renovaveis', 'geracao_hidreletrica', 'intercambio', 'demanda'];
    const dadosDemandaGeracaoIntercambio = await getDadosDessem('demanda-geracao-intercambio', inicio, fim);
    const lineGeracao = Chart.getChart('line-geracao');
    let lineGeracaoDatasets = lineGeracao['config']['_config']['data']['datasets'];
    for (let tipo in tiposLabel) {
                lineGeracaoDatasets[tipo] = new Dataset(tiposLabel[tipo], dadosDemandaGeracaoIntercambio[tiposLabel[tipo]], false, coresDessem[tiposLabel[tipo]], tiposLabel[tipo] == "demanda" ? "line" : "bar");
    }
    lineGeracao['config']['_config']['data']['datasets'] = lineGeracaoDatasets;
    lineGeracao.update();
}

async function filtrarTotalGeracao(inicio, fim){
    const dadosGeracaoTotal = await getDadosDessem('total-geracao', inicio, fim);
    const donutGeracao = Chart.getChart('donut-geracao');
    donutGeracao['config']['_config']['data']['datasets'][0]['data'] = [dadosGeracaoTotal['geracao_renovaveis'], dadosGeracaoTotal['geracao_termica'], dadosGeracaoTotal["geracao_hidreletrica"]];
    donutGeracao.update();
}
async function filtrarGraficoPld(inicio, fim){
    const dadosPldCmo = await getDadosDessem('pld-cmo',inicio, fim);
    const linePld = Chart.getChart('line-pld');
    const dadosPld = dadosPldCmo['pld']
    for(let subsistema in subsistemas){
        linePld['config']['_config']['data']['datasets'][subsistema]['data'] = dadosPld[subsistemas[subsistema]];
    }
    linePld.update();
}



function radioToCheckbox() {
    let inputs = Array.from(document.querySelectorAll('input[name="radioSubsistema"]'));
    let labels = Array.from(document.querySelectorAll('label[name="labelSubsistema"]'));
    const selecaoMultipla = document.getElementById("selecao-multipla");

    let inputTodos = inputs.shift();
    let labelTodos = labels.shift();

    inputs.forEach((input) => {
        selecaoMultipla.checked ? input.type = "checkbox" : input.type = "radio";
    })

    labels.forEach((label) => {
        selecaoMultipla.checked ? label.className += '' : label.className = label.className.replace(' active', '');
    })

    if (selecaoMultipla.checked) {
        inputTodos.disabled = true;
        labelTodos.className = labelTodos.className.replace(' active', '');
        inputTodos.selected = false;
        inputs[0].selected = true;
        labels[0].className += ' active';

    } else {
        inputTodos.disabled = false;
        labelTodos.className += ' active';
        inputTodos.selected = true;
    }
}

function getSubsistema() {
    return document.querySelector('input[name="radioSubsistema"]:checked').value;
}

function enableTooltip(){

}
document.getElementById('tooltipConfig').addEventListener("click", ()=>{
    charts.forEach((chart)=>{
        chart['config']['_config']['options']['animations'] = !chart['config']['_config']['options']['animations'];
        chart['config']['_config']['options']['events'] = chart['config']['_config']['options']['animations'] ? [] : Chart.defaults.events;
        chart.update();
    })
})

window.addEventListener("load", async () => {
    await loadGraficos();
    await loadLimiteIntercambio();
    await loadGeracaoTermica();
})