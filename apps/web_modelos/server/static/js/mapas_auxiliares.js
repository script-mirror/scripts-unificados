class Mapa {
    constructor(granularidade, cores, regiaoHighlight) {
        this.id = `por-${granularidade}`;
        this.granularidade = granularidade;
        this.zoomSetting = 4;
        this.cores = cores;
        this.mapa = L.map(this.id, {
            center: [-14.24, -51.93],
            zoom: this.zoomSetting,
            worldCopyJump: true,
            zoomControl: false,
            attributionControl: false,
        });
        this.subbacias;
        this.geoJsonLayer = undefined;
        this.regiaoHighlight = regiaoHighlight;

        L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
            attribution: 'Map data &copy; <a href="https://www.openstreetmap.org/">OpenStreetMap</a> contributors',
            maxZoom: 18,
            minZoom: this.zoomSetting
        }).addTo(this.mapa);



        L.Control.textbox = L.Control.extend({
            onAdd: (e) => {

                let text = L.DomUtil.create('div');
                text.id = `title-${this.granularidade}`;
                text.style = 'font-size:15px;font-weight:bolder;'
                text.innerText = this.granularidade
                return text;
            },

            onRemove: (e) => {

            }
        });
        L.control.textbox = function (opts) { return new L.Control.textbox(opts); }
        L.control.textbox({ position: 'topleft' }).addTo(this.mapa);
        this.carregarBacias();
    }

    carregarBacias = () => {
        fetch("/static/js/subbacias.json").then((promise) => {
            promise.json().then((response) => {
                this.subbacias = response;
                this.plotarMapa(this.subbacias)
            })
        })
    }

    plotarMapa = (data) => {
        this.geoJsonLayer = L.geoJSON(data, {
            onEachFeature: this.onEachFeature,
            style: this.style
        }).addTo(this.mapa);
    }

    atualizarCores = (regiaoHighlight) =>{
        this.regiaoHighlight = regiaoHighlight;
        this.geoJsonLayer.remove();
        this.plotarMapa(this.subbacias)
    }
    formatarDadosComoTabela = (dados) => {
        let tabela = '<table>';
        for (let chave in dados) {
            tabela += '<tr><td>' + chave + '</td><td>' + dados[chave] + '</td></tr>';
        }
        tabela += '</table>';
        return tabela;
    }

    getColor = (granularidade) => {
        return this.cores[granularidade]
    }
    onEachFeature = (feature, layer) => {
        layer.on('mouseover', (e) => {
            layer.bindTooltip(`${this.formatarDadosComoTabela(feature.properties)}`).openTooltip();
        });
    }

    style = (feature) => {
        let cor = this.getColor(feature.properties[this.granularidade.toLowerCase()]);
        cor += feature.properties[this.granularidade.toLowerCase()] == this.regiaoHighlight || this.regiaoHighlight == 'Todas' ? "FF" :"44";
        return {
            fillColor: cor,
            weight: "1",
            opacity:  1,
            color: "black",
            dashArray: "1",
            fillOpacity: 0.7
        }
    }

    updateColors = () => {
        console.log(this.geoJsonLayer)
        this.geoJsonLayer.setStyle(
            {
                weight: 2,
                opacity: 1,
                color: 'black',
                dashArray: '3',
                fillOpacity: 0.7
            }
        )
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



const cores = {
    "Norte": "#98DB11",
    "Nordeste": "#FB9701",
    "Sul": "#FED42A",
    "Sudeste": "#FE0000",
    "Doce": "#FF5733",
    "Grande": "#FFC300",
    "Iguaçu": "#DAF7A6",
    "Itaipu": "#581845",
    "Madeira": "#C70039",
    "Capivari": "#900C3F",
    "Itajaí-Açu": "#FFC0CB",
    "Antas": "#9FE2BF",
    "Jacuí": "#40E0D0",
    "Uatuamã": "#6495ED",
    "Curuá-Una": "#CCCCFF",
    "Araguari": "#FFD700",
    "Jequitinho": "#FF6666",
    "Jari": "#B03060",
    "Parnaíba": "#FF4500",
    "Itabapoana": "#008080",
    "Mucuri": "#BDB76B",
    "Itiquira": "#9ACD32",
    "Jauru": "#8A2BE2",
    "Manso": "#A52A2A",
    "Correntes": "#DEB887",
    "Paranaíba": "#5F9EA0",
    "Paraná": "#7FFF00",
    "Paranapane": "#D2691E",
    "São Franci": "#FF7F50",
    "Tapajós": "#6495ED",
    "Tietê": "#FFF8DC",
    "Tocantins": "#DC143C",
    "Uruguai": "#00FFFF",
    "Xingu": "#00008B",
    "Paraguaçu": "#008B8B",
    "Paraíba do Sul": "#B8860B",
    "Sta. Maria da Vitória": "#A9A9A9"
}

const submercado = new Mapa('submercado', cores, 'Todas')
const bacia = new Mapa('bacia', cores, 'Todas')

function filtrarMapa(mapaObj, regiao){
    mapaObj.atualizarCores(regiao);
}


$('#select-bacia').on('change', function(e){
    filtrarMapa(bacia, this.value)
  });

  $('#select-submercado').on('change', function(e){
    filtrarMapa(submercado, this.value)
  });