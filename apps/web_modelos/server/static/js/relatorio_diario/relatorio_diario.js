const grafico_ena = plotarGrafico();

let submercado_aux = { 'N': 'NORTE', 'NE': 'NORDESTE', 'S': 'SUL', 'SE': 'SUDESTE' };


function buscarEna() {
    let modeloSelecionado = document.getElementById("selectModelo").value;
    let regiaoSelecionada = document.querySelector('input[name="radioRegiao"]:checked');
    let granularidade = "submercado"
    let hrRodada = document.getElementById("selectHrRodada").value;
    let qtdDiasSelecionados = document.getElementById("selectQtdDias").value;
    let sessionStorageItem = modeloSelecionado + granularidade + hrRodada + qtdDiasSelecionados


    if (sessionStorage.getItem(sessionStorageItem) == null) {
        getEna(qtdDiasSelecionados, modeloSelecionado, granularidade, hrRodada, sessionStorageItem).then(
            () => {
                filtroRegiao(regiaoSelecionada, sessionStorageItem)
            }
        );

    } else {

        filtroRegiao(regiaoSelecionada, sessionStorageItem);

    }
}

function DatasetData(label, data){
    this.x = label;
    this.y = data;
}

function Dataset(label, data) {
    this.label = label;
    this.data = data;
    let dataLabel = Object.keys(data)
    let dataDados = Object.values(data)
    this.data = [];
    for(dados in dataLabel){
        this.data.push(new DatasetData(dataLabel[dados], dataDados[dados]))
    }

}




async function getEna(ndias, modelo, granularidade, hrRodada, sessionStorageItem) {

    let response = await fetch(`/middle/API/get/dados-rodadas-ndias-atras?` + new URLSearchParams({
        ndias: ndias,
        modelo: modelo,
        granularidade: granularidade,
        hr_rodada: hrRodada
    }));
    // ERRO NO SESSION STORAGE

    let dados_rodadas_ena = await response.json()
    dados_rodadas_ena = (JSON.stringify(dados_rodadas_ena))
    sessionStorage.setItem(sessionStorageItem, dados_rodadas_ena)
}
function filtroRegiao(element, sessionStorageItem) {

    let data_inicial_rodada
    let regiao = element.value;
    let datasets = [];
    let dados_rodadas_ena = sessionStorage.getItem(sessionStorageItem)
    dados_rodadas_ena = JSON.parse(dados_rodadas_ena)

    data_inicial_rodada = Object.keys(dados_rodadas_ena["ACOMPH"][regiao])
    data_inicial_rodada = data_inicial_rodada[0].substring(5, 10)
    data_inicial_rodada = data_inicial_rodada.split('/').reverse().join("")
    

    datasets.push(new Dataset("ACOMPH", dados_rodadas_ena["ACOMPH"][regiao]))

    for (let previsao_ena of dados_rodadas_ena["PREVISAO_ENA"]) {
        data_inicial_rodada = Object.keys(previsao_ena.valores[regiao]);
        data_inicial_rodada = data_inicial_rodada[0].substring(5, 10)
        // data_inicial_rodada = data_inicial_rodada.split('/').reverse().join("")
        datasets.push(new Dataset(previsao_ena.modelo + data_inicial_rodada, previsao_ena["valores"][regiao]))
    }



    

    atualizarGrafico(grafico_ena, datasets, submercado_aux[regiao]);

}


function plotarGrafico() {

    let regiao = "";
    const ctx = document.getElementById("grafico");
    const config = {
        type: 'line',
        data: {
            fill: false
        },
        options: {
            scales: {
                xAxes: [{
                    type: 'time',
                    time: {
                        unit: 'day'
                    },
                    ticks: {
                        // callback: function(value, index, values) {
                        //     return index % 2 === 0 ? value : '';
                        // },
                        source:'data'
                    }
                }],
                yAxes: [{
                    title: {
                        display: true,
                        text: "ENA",
                        font: {
                            size: 20
                        }
                    },
                    beginAtZero: false
                }]
            },
            responsive: true,
            maintainAspectRatio: false,
            tooltips: {
                mode:'x'
            },
            hover:{
                intersect:false,
                mode:'x',
                axis:'x',
            },
            plugins: {
                title: {
                    font: {
                        size: 20
                    },
                    display: true,
                    text: regiao
                },
                legend: {
                    position: "bottom"
                }
            },
            elements: {
                borderWidth: 20
            }
        }
    };

    return new Chart(ctx, config);
}

function atualizarGrafico(grafico, datasets, regiao) {

    let rgb = "rgb(872, 216, 864)"

    grafico.data.datasets = []
    datasets = Object.values(datasets);
    for (dataset in datasets) {
        rgb = modifyColor(rgb, false)
        datasets[dataset].backgroundColor = rgb;
        datasets[dataset].borderColor = rgb;
        datasets[dataset].fill = false;

        grafico.data.datasets.push(datasets[dataset])
    }
    // grafico.data.datasets.forEach(dataset => {
        //     dataset.data = dados;
        // });
    grafico.data.datasets[0].backgroundColor = rgb;
    grafico.data.datasets[0].borderColor = rgb;

    grafico.options.title.text = regiao;
    grafico.update();
}

function qtdDeDiasASelecionar(qtdDias) {
    let selectQtdDias = document.getElementById("selectQtdDias");
    for (let i = 2; i < qtdDias + 2; i++) {
        option = document.createElement("option");
        option.value = i;
        option.text = `N - ${i - 1}`;
        selectQtdDias.appendChild(option)
        if (i == qtdDias + 1) {
            selectQtdDias.value = i
        }
    }
}


function modifyColor(rgb, lighten) {
    const [r, g, b] = rgb.match(/\d+/g).map(Number);

    const factor = lighten ? 1.2 : 0.8;

    const modifiedR = Math.round(r * factor);
    const modifiedG = Math.round(g * factor);
    const modifiedB = Math.round(b * factor);

    return `rgb(${modifiedR}, ${modifiedG}, ${modifiedB})`;
}
