
const formControlRange = document.getElementById("formControlRange")
const msgRangeDias = document.getElementById("msgRangeDias")

const sliderInicio = document.getElementById("slider-inicio")
const sliderFinal = document.getElementById("slider-final")
const parametrosMinimosDeDiferenca = [-7.7, -6.6, -5.5, -4.4, -3.3, -2.2, -1.1, 0, 1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7]
const parametrosMapaObs = [0, 1, 5, 10, 15, 20, 25, 30, 40, 50, 75, 100, 150, 200]

const esquemaCoresObs = ["#ffffff", "#e1ffff", "#b3f0fb", "#95d2f9", "#2585f0", "#0c68ce", "#73fd8b", "#39d52b", "#3ba933", "#ffe67b", "#FFBD4A", "#fd5c22", "#B91D22", "#F7596F", "#a9a9a9"]
// const esquemaCoresPrevisao = ['#4444ff', '#445fe4', '#4479ca', '#4494af', '#44af94', '#44ca79', '#44e45f', '#44ff44', '#5fe444', '#79ca44', '#94af44', '#af9444', '#ca7944', '#e45f44', '#ff4444'];
const esquemaCoresPrevisao = ["rgba(255, 0, 0, 1)", "rgba(255, 0, 0, 0.857)", "rgba(255, 0, 0, 0.714)", "rgba(255, 0, 0, 0.571)", "rgba(255, 0, 0, 0.429)", "rgba(255, 0, 0, 0.286)", "rgba(255, 0, 0, 0.143)", "rgba(255, 0, 0, 0)", "rgba(0, 0, 255, 0.143)", "rgba(0, 0, 255, 0.286)", "rgba(0, 0, 255, 0.429)", "rgba(0, 0, 255, 0.571)", "rgba(0, 0, 255, 0.714)", "rgba(0, 0, 255, 0.857)", "rgba(0, 0, 255, 1)"]

class Mapa {
    constructor(modelo, esquemaDeCores, zoomSetting = 3) {
        this.id = `mapa-${modelo}`;
        this.modelo = modelo;
        this.zoomSetting = zoomSetting;
        this.coresIndicadores = esquemaDeCores;
        this.mapa = L.map(this.id, {
            center: [-14.24, -23.18],
            zoom: this.zoomSetting,
            worldCopyJump: true,
            zoomControl: false,
            attributionControl: false,
        });
        this.subbacias;
        this.geoJsonLayer = undefined;

        L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
            attribution: 'Map data &copy; <a href="https://www.openstreetmap.org/">OpenStreetMap</a> contributors',
            maxZoom: 18,
            minZoom: this.zoomSetting
        }).addTo(this.mapa);

        this.southWest = L.latLng(-56.05, -99.14),
            this.northEast = L.latLng(11.45, -34.10),
            this.bounds = L.latLngBounds(this.southWest, this.northEast);

        this.mapa.setMaxBounds(this.bounds);


        L.Control.textbox = L.Control.extend({
            onAdd: (e) => {

                let text = L.DomUtil.create('div');
                text.id = `title-${this.modelo}`;
                text.style = 'font-size:15px;font-weight:bolder;'
                text.innerText = `${capitalizeFirstLetter(this.modelo)}`
                return text;
            },

            onRemove: (e) => {

            }
        });
        L.control.textbox = function (opts) { return new L.Control.textbox(opts); }
        L.control.textbox({ position: 'topleft' }).addTo(this.mapa);

        if (this.modelo != 'observado') {
            let legend = L.control({ position: 'bottomleft' });
            legend.onAdd = function (map) {

                let div = L.DomUtil.create('div', 'info-legend');
                let labels = [this.modelo],
                    categories = ['Superestimado', 'Subestimado'];
                let cores_label = ['#4444FF', '#FF4444'];

                for (let i = 0; i < categories.length; i++) {

                    div.innerHTML +=
                        labels.push(
                            '<i class="circle" style="background:' + cores_label[i] + '"></i> ' +
                            (categories[i] ? categories[i] : '+'));

                }
                div.innerHTML = labels.join('<br>');
                return div;
            };
            legend.addTo(this.mapa);
        }


    }

    carregarBacias = (inicio, fim) => {
        let dadosChuva;
        if (this.geoJsonLayer != undefined) {
            this.geoJsonLayer.remove()
        }
        if (this.modelo != 'observado') {
            dadosChuva = sessionStorage.getItem(`chuva_${this.modelo}_${inicio}`)
        } else {
            dadosChuva = sessionStorage.getItem(`chuva_${this.modelo}`)
        }
        dadosChuva = JSON.parse(dadosChuva)
        let mm_chuva = 0;
        if (this.subbacias == undefined) {
            fetch("/static/js/subbacias.json")
                .then(response => response.json())
                .then(data => {
                    this.subbacias = data;
                    for (let feature in this.subbacias["features"]) {
                        mm_chuva = 0
                        for (let dia in dadosChuva) {
                            if (dia >= inicio && dia <= fim) {
                                if (mm_chuva + dadosChuva[dia][this.subbacias["features"][feature]["id"]] != undefined) {
                                    mm_chuva += dadosChuva[dia][this.subbacias["features"][feature]["id"]]
                                }
                            }
                        }
                        this.subbacias["features"][feature]["properties"]["mm_chuva"] = Number((mm_chuva).toFixed(1))
                        this.subbacias["features"][feature]["qtd_dias_plotados"] = Object.keys(dadosChuva).length
                    }
                    this.plotarMapa(this.subbacias)
                })
        }
        else {
            for (let feature in this.subbacias["features"]) {
                mm_chuva = 0
                let dia;
                for (dia in dadosChuva) {
                    if (dia >= inicio && dia <= fim) {
                        if (mm_chuva + dadosChuva[dia][this.subbacias["features"][feature]["id"]] != undefined) {
                            mm_chuva += dadosChuva[dia][this.subbacias["features"][feature]["id"]]
                        }
                    }
                }
                this.subbacias["features"][feature]["properties"]["mm_chuva"] = Number((mm_chuva).toFixed(1))
                this.subbacias["features"][feature]["qtd_dias_plotados"] = Object.keys(dadosChuva).length
            }
            this.plotarMapa(this.subbacias)
        }
    }

    plotarMapa = (data) => {
        this.geoJsonLayer = L.geoJSON(data, {
            onEachFeature: this.onEachFeature,
            style: this.style
        }).addTo(this.mapa);
    }
    formatarDadosComoTabela = (dados) => {
        let tabela = '<table>';
        for (let chave in dados) {
            tabela += '<tr><td>' + chave + '</td><td>' + dados[chave] + '</td></tr>';
        }
        tabela += '</table>';
        return tabela;
    }

    getColor = (mm_chuva, qtd_dias_plotados) => {
        if (this.modelo == "observado") {
        return mm_chuva > 200 ? this.coresIndicadores[14] :
        mm_chuva > 150 ? this.coresIndicadores[13] :
        mm_chuva > 100 ? this.coresIndicadores[12] :
        mm_chuva > 75 ? this.coresIndicadores[11] :
        mm_chuva > 50 ? this.coresIndicadores[10] :
        mm_chuva > 40 ? this.coresIndicadores[9] :
        mm_chuva > 30 ? this.coresIndicadores[8] :
        mm_chuva > 25 ? this.coresIndicadores[7] :
        mm_chuva > 20 ? this.coresIndicadores[6] :
        mm_chuva > 15 ? this.coresIndicadores[5] :
        mm_chuva > 10 ? this.coresIndicadores[4] :
        mm_chuva > 5 ? this.coresIndicadores[3] :
        mm_chuva > 1 ? this.coresIndicadores[2] :
        mm_chuva > 0 ? this.coresIndicadores[1] :
        this.coresIndicadores[0];
        } else {
                console.log(mm_chuva)
                return mm_chuva < parametrosMinimosDeDiferenca[0] * qtd_dias_plotados ? this.coresIndicadores[0] :
                mm_chuva < parametrosMinimosDeDiferenca[1] * qtd_dias_plotados ? this.coresIndicadores[1] :
                mm_chuva < parametrosMinimosDeDiferenca[2] * qtd_dias_plotados ? this.coresIndicadores[2] :
                mm_chuva < parametrosMinimosDeDiferenca[3] * qtd_dias_plotados ? this.coresIndicadores[3] :
                mm_chuva < parametrosMinimosDeDiferenca[4] * qtd_dias_plotados ? this.coresIndicadores[4] :
                mm_chuva < parametrosMinimosDeDiferenca[5] * qtd_dias_plotados ? this.coresIndicadores[5] :
                mm_chuva < parametrosMinimosDeDiferenca[6] * qtd_dias_plotados ? this.coresIndicadores[6] :
                mm_chuva < parametrosMinimosDeDiferenca[7] * qtd_dias_plotados ? this.coresIndicadores[7] :
                mm_chuva < parametrosMinimosDeDiferenca[8] * qtd_dias_plotados ? this.coresIndicadores[8] :
                mm_chuva < parametrosMinimosDeDiferenca[9] * qtd_dias_plotados ? this.coresIndicadores[9] :
                mm_chuva < parametrosMinimosDeDiferenca[10] * qtd_dias_plotados ? this.coresIndicadores[10] :
                mm_chuva < parametrosMinimosDeDiferenca[11] * qtd_dias_plotados ? this.coresIndicadores[11] :
                mm_chuva < parametrosMinimosDeDiferenca[12] * qtd_dias_plotados ? this.coresIndicadores[12] :
                mm_chuva < parametrosMinimosDeDiferenca[13] * qtd_dias_plotados ? this.coresIndicadores[13] :
                this.coresIndicadores[14];
        }
    }
    onEachFeature = (feature, layer) => {
        layer.on('mouseover', (e) => {
            layer.bindTooltip(`${this.formatarDadosComoTabela(feature.properties)}`).openTooltip();
        });
    }

    style = (feature) => {
        return {
            fillColor: this.getColor(feature.properties.mm_chuva, feature.qtd_dias_plotados),
            weight: 0.5,
            opacity: 1,
            color: 'black',
            dashArray: '1',
            fillOpacity: 0.7
        }
    }



    updateColors = () => {
        this.geoJsonLayer.setStyle(
            {
                fillColor: () => { this.getColor(feature["properties"]["mm_chuva"]) },
                weight: 2,
                opacity: 1,
                color: 'black',
                dashArray: '3',
                fillOpacity: 0.7
            }
        )
    }

    getChuvaObservadaPorDia = (rangeDias, dataFinal) => {
        let dadosChuva;
        let parametrosUrl;

        if (sessionStorage.getItem(`chuva_${this.modelo}`) == null || Object.keys(JSON.parse(sessionStorage.getItem("chuva_" + this.modelo))).length != rangeDias) {
            if (dataFinal != undefined) {
                parametrosUrl = new URLSearchParams({
                    range_dias: rangeDias,
                    data_final: dataFinal,
                    granularidade: "subbacia"
                })
            } else {
                parametrosUrl = new URLSearchParams({
                    range_dias: rangeDias,
                    granularidade: "subbacia"
                })
            }
            dadosChuva = fetch("/middle/API/getchuvaObservada?" + parametrosUrl)
                .then(response => response.json())
                .then(response => { dadosChuva = response }).then(
                    () => {
                        sessionStorage.setItem(`chuva_${this.modelo}`, JSON.stringify(dadosChuva));
                        criarSlider(rangeDias - 1, dadosChuva)

                    })
        } else {
            dadosChuva = sessionStorage.getItem(`chuva_${this.modelo}`)
            dadosChuva = JSON.parse(dadosChuva)
            criarSlider(rangeDias - 1)


        }
    }

    getChuvaPrevisao(dataRodada) {
        let dadosChuva;
        let parametrosUrl;
        const tituloMapa = document.getElementById(`title-${this.modelo}`)
        if (sessionStorage.getItem(`chuva_${this.modelo}_${dataRodada}`) == null) {
            let divMapa = document.getElementById(`mapa-${this.modelo}`)
            let divMapaLoading = document.getElementById(`mapa-${this.modelo}-loading`)
            parametrosUrl = new URLSearchParams({
                modelo: this.modelo,
                granularidade: "subbacia",
                data_rodada: dataRodada
            });

            dadosChuva = fetch("/middle/API/get/diferenca-chuva-prevista-observada?" + parametrosUrl)
                .then(response => response.json(), divMapa.style.display = 'none', divMapaLoading.style.display = 'block')
                .then(response => { dadosChuva = response }).then(
                    () => {
                        if(Object.keys(dadosChuva)[0] == "erro"){
                            return
                        }
                        divMapa.style.display = 'block';
                        divMapaLoading.style.display = 'none'
                        sessionStorage.setItem(`chuva_${this.modelo}_${dataRodada}`, JSON.stringify(dadosChuva));
                        this.carregarBacias(Object.keys(dadosChuva)[0], Object.keys(dadosChuva)[Object.keys(dadosChuva).length - 1])
                        tituloMapa.innerHTML = `${capitalizeFirstLetter(this.modelo)} ${dataRodada} ${Object.keys(dadosChuva).length} dia(s)`
                    });
        } else {
            dadosChuva = sessionStorage.getItem(`chuva_${this.modelo}_${dataRodada}`);
            dadosChuva = JSON.parse(dadosChuva);
            this.carregarBacias(Object.keys(dadosChuva)[0], Object.keys(dadosChuva)[Object.keys(dadosChuva).length - 1]);
            tituloMapa.innerHTML = `${capitalizeFirstLetter(this.modelo)} ${dataRodada} ${Object.keys(dadosChuva).length} dia(s)`

        }

    }

    buscarChuva = () => {
        let datasChuvaObservada = Object.keys(JSON.parse(sessionStorage.getItem(`chuva_${this.modelo}`)))

        let dataInicialAtual = datasChuvaObservada[0];
        let dataFinalAtual = datasChuvaObservada[datasChuvaObservada.length - 1];

        let dataInicialBusca = sliderInicio.value;
        let dataFinalBusca = sliderFinal.value;

        if (dataInicialBusca != dataInicialAtual || dataFinalBusca != dataFinalAtual) {
            let dataInicial = new Date(dataInicialBusca);
            let dataFinal = new Date(dataFinalBusca);
            let diferencaEmMilissegundos = dataFinal.getTime() - dataInicial.getTime();

            let diferencaEmDias = diferencaEmMilissegundos / (1000 * 3600 * 24);

            diferencaEmDias = Math.floor(diferencaEmDias) + 1;
            this.getChuvaObservadaPorDia(diferencaEmDias, dataFinalBusca);
        }
    }

    highlightFeature = (e) => {
        var layer = e.target;
        layer.setStyle({
            weight: 5,
            color: '#666',
            fillOpacity: 0.7
        });

        layer.bringToFront();
    }
}



function criarSlider(maxRange) {
    let sliderAtual = document.getElementById("flat-slider")
    let sliderSpan = document.getElementById("slider-span")
    sliderAtual.remove()

    let sliderNovo = document.createElement('div')

    sliderNovo.id = "flat-slider"

    sliderSpan.appendChild(sliderNovo)

    $("#flat-slider")
        .slider({
            max: maxRange,
            min: 0,
            range: true,
            values: [0, maxRange],
            slide: function (event, ui) {
                exibirDiasSlider(ui.values);
            }
        })
        .on("click"), exibirDiasSlider($("#flat-slider").slider("values"))

    $(".ui-slider-handle").addClass("custom-slider-handle");

    $(".ui-slider-range").addClass("custom-slider-range");

}

function exibirDiasSlider(values) {
    let datas = Object.keys(JSON.parse(sessionStorage.getItem(`chuva_${this.modelo}`)))
    sliderInicio.value = datas[values[0]]
    sliderFinal.value = datas[values[1]]
    mapaObs.carregarBacias(datas[values[0]], datas[values[1]])
}


function criarLabelMapaObs() {
    const divCoresMapaObs = document.getElementById("cores-mapa-obs")
    const divParametrosMapaObs = document.getElementById("parametros-mapa-obs")
    let widthDiv = 100 / esquemaCoresObs.length;
    for (let cor in esquemaCoresObs) {
        let divCores = document.createElement('div')
        divCores.style.backgroundColor = esquemaCoresObs[cor]
        divCores.style.width = `${widthDiv}%`;
        divCores.style.height = "100%"
        divCores.style.display = "flex"
        divCoresMapaObs.appendChild(divCores)

        let divParametros = document.createElement('div')
        divParametros.style.width = `${widthDiv}%`;
        divParametros.style.height = "100%"
        divParametros.style.display = "flex"
        if (cor > 0) {
            divParametros.innerHTML = parametrosMapaObs[cor - 1]
        }
        divParametrosMapaObs.appendChild(divParametros)
    }

}




let mapasPrevisao = []
// const nomesModelos = ["gefs", "gfs", "eta40", "pconjunto"]
const nomesModelos = ["gefs"]

// mapaGefs.getChuvaPrevisao("2024-01-01")
// mapaGfs.getChuvaPrevisao("2024-01-01")
// mapaEta.getChuvaPrevisao("2024-01-01")
// mapaPconjunto.getChuvaPrevisao("2024-01-01")




function exibirDiasSlider(values) {
    datas = Object.keys(JSON.parse(sessionStorage.getItem(`chuva_observado`)))
    sliderInicio.value = datas[values[0]]
    sliderFinal.value = datas[values[1]]
    mapaObs.carregarBacias(datas[values[0]], datas[values[1]])

}
function procurarData(nome, data) {
    for (let modelo in nomesModelos) {
        if (nomesModelos[modelo] == nome) {
            mapasPrevisao[modelo].getChuvaPrevisao(data)
        }
    }
}

function capitalizeFirstLetter(string) {
    return string.charAt(0).toUpperCase() + string.slice(1);
}

function formatarData(data) {
    var dia = String(data.getDate()).padStart(2, '0');
    var mes = String(data.getMonth() + 1).padStart(2, '0');
    var ano = data.getFullYear();
    return ano + '-' + mes + '-' + dia;
}

// TODO function criarMetricaMapaDiferenca(dados){


// }


function max(array) {
    return Math.max.apply(null, array);
};

function min(array) {
    return Math.min.apply(null, array);
};
