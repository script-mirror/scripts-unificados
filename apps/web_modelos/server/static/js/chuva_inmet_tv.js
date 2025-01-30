const mmChuvaMinimo = 5;

const urlParams = new URLSearchParams(window.location.search);
const paramZoom = urlParams.get('zoom');

let zommSetting = 0;
let coords = [];

if (paramZoom == "sul") {
  zommSetting = 6;
  coords = [-23.40, -51.93];

}else{
  zommSetting = 5;
  coords = [-14.24, -51.93];

}

class Mapa {
    constructor(id, cores, dtInicial, dtFinal) {
      this.id = id;
      this.zoomSetting = zommSetting;
      this.cores = cores;
      this.mapa = L.map(this.id, {
        center: coords,
        zoom: this.zoomSetting,
        worldCopyJump: true,
        zoomControl: false,
        attributionControl: false,
      });
      this.id = id
      this.bacias;
      this.markers = []
  
      L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
        attribution: 'Map data &copy; <a href="https://www.openstreetmap.org/">OpenStreetMap</a> contributors',
        maxZoom: 18,
        minZoom: this.zoomSetting
      }).addTo(this.mapa);
  
  
  
      L.Control.textbox = L.Control.extend({
        onAdd: (e) => {
  
          let labelColors = L.DomUtil.create('div');
          let labelText = L.DomUtil.create('div');
          let div = L.DomUtil.create('div');
          div.style = 'width:100%; margin:0;font-weight:bold;'
          labelColors.id = 'labelColors';
          labelColors.style = 'border: 1px solid; height: 20px;';
          labelColors.className = 'row';
          labelText.id = 'labelText';
          labelText.className = 'row bg-light';
          div.append(labelColors);
          div.append(labelText);

          return div;
        },
  
        onRemove: (e) => {
  
        }
      });
      L.control.textbox = function (opts) { return new L.Control.textbox(opts); }
      L.control.textbox({ position: 'topleft' }).addTo(this.mapa);
  
      // fetchData(`estacoes-meteorologicas/chuva-acumulada?dtInicial=${dtInicial}&dtFinal=${dtFinal}`).then((response)=>{
      //     this.criarMarcadores(response, dtInicial, dtFinal);
      // })
      this.carregarBacias()
    }
  
    criarMarcadores = (data, dtInicial, dtFinal) => {
      this.markers.forEach(marker => this.mapa.removeLayer(marker));
      this.markers = [];
      let baciasSelecionadas = $("#select-bacia").val();
      baciasSelecionadas = baciasSelecionadas.map(v => v.toUpperCase());
      if (baciasSelecionadas.length > 0) {
        data.forEach((estacao, index) => {
          if (baciasSelecionadas.includes(estacao.bacia.toUpperCase()) && estacao.chuva_acumulada >= mmChuvaMinimo) {
            const icon = this.createIcon(this.getColor(estacao.chuva_acumulada), estacao.chuva_acumulada, index);
            const marker = L.marker([estacao.vl_lat, estacao.vl_lon], { icon: icon, forceZIndex: index })
              .addTo(this.mapa)
              .bindTooltip(`
                      Bacia: ${estacao.bacia}<br>
                      Chuva acumulada: ${estacao.chuva_acumulada}<br>
                      Cod_estacao: ${estacao.cod_estacao}<br>
                      Nome: ${estacao.nome}<br>
                      Estado: ${estacao.estado}<br>
                      Periodo: De <strong><u>${moment(dtInicial).format('DD/MM/YYYY HH:mm')}</u></strong> Até <strong><u>${moment(dtFinal).format('DD/MM/YYYY HH:mm')}</u></strong> <br>
                      `, {
                permanent: false,
                direction: 'bottom'
              });
            this.markers.push(marker);
  
          }
  
        })
      } else {
  
        data.forEach((estacao, index) => {
          if (estacao.chuva_acumulada >= mmChuvaMinimo) {
  
            const icon = this.createIcon(this.getColor(estacao.chuva_acumulada), estacao.chuva_acumulada, index);
            const marker = L.marker([estacao.vl_lat, estacao.vl_lon], { icon: icon, forceZIndex: index })
              .addTo(this.mapa)
              .bindTooltip(`
                      Bacia: ${estacao.bacia}<br>
                      Chuva acumulada: ${estacao.chuva_acumulada}<br>
                      Cod_estacao: ${estacao.cod_estacao}<br>
                      Nome: ${estacao.nome}<br>
                      Estado: ${estacao.estado}<br>
                      Periodo: De <strong><u>${moment(dtInicial).format('DD/MM/YYYY HH:mm')}</u></strong> Até <strong><u>${moment(dtFinal).format('DD/MM/YYYY HH:mm')}</u></strong> <br>
                      `, {
                permanent: false,
                direction: 'bottom'
              });
            this.markers.push(marker);
          }
        })
      }
    }
    createIcon(color, text, zindex) {
      return L.divIcon({
        className: 'custom-div-icon',
        html: `<div style="background-color:${color};z-index:-${zindex}" class="marker-pin">${text}</div>`,
        iconSize: [30, 30],
      });
    }
    carregarBacias = () => {
  
      fetch("/static/js/bacias.json").then((promise) => {
        promise.json().then((response) => {
          this.bacias = response;
          this.plotarMapa(this.bacias)
        })
      })
    }
  
    plotarMapa = (data) => {
      this.geoJsonLayer = L.geoJSON(data, {
        onEachFeature: this.onEachFeature,
        style: this.style
      }).addTo(this.mapa);
    }
    atualizarCores = () => {
      this.criarMarcadores()
    }
    formatarDadosComoTabela = (dados) => {
      let tabela = '<table>';
      for (let chave in dados) {
        if (chave != 'ID') {
          tabela += '<tr><td>' + chave + '</td><td>' + dados[chave] + '</td></tr>';
  
        }
  
      }
      tabela += '</table>';
      return tabela;
    }
  
    getColor = (value) => {
      return value > 200 ? this.cores[14] :
        value > 150 ? this.cores[13] :
          value > 100 ? this.cores[12] :
            value > 75 ? this.cores[11] :
              value > 50 ? this.cores[10] :
                value > 40 ? this.cores[9] :
                  value > 30 ? this.cores[8] :
                    value > 25 ? this.cores[7] :
                      value > 20 ? this.cores[6] :
                        value > 15 ? this.cores[5] :
                          value > 10 ? this.cores[4] :
                            value > 5 ? this.cores[3] :
                              value > 1 ? this.cores[2] :
                                value > 0 ? this.cores[1] :
                                  this.cores[0];
    }
    onEachFeature = (feature, layer) => {
      layer.on('mouseover', (e) => {
        layer.bindTooltip(`${this.formatarDadosComoTabela(feature.properties)}`).openTooltip();
      });
    }
    style = (feature) => {
      // let cor = this.getColor(feature.properties[this.id.toLowerCase()]);
      // cor += feature.properties[this.id.toLowerCase()] == this.regiaoHighlight || this.regiaoHighlight == 'Todas' ? "FF" :"44";
      return {
        fillColor: '#fff',
        weight: "1",
        opacity: 1,
        color: "black",
        dashArray: "1",
        fillOpacity: 0.7
      }
    }
    updateColors = () => {
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
  const cores = ["#ffffff", "#e1ffff", "#b3f0fb", "#95d2f9", "#2585f0", "#0c68ce", "#73fd8b", "#39d52b", "#3ba933", "#ffe67b", "#FFBD4A", "#fd5c22", "#B91D22", "#F7596F", "#a9a9a9"]
  
  async function fetchData(path) {
    const loading = document.getElementById('loading');
    loading.style.display = 'block';
    const promise = await fetch(`API/get/${path}`);
    loading.style.display = 'none';
    return await promise.json()
  }
  
  window.addEventListener('load', async function () {
    const dtInicial = document.getElementById("data-inicio");
    const dtFinal = document.getElementById("data-final");
  
    // listar bacias
    (() => {
      const selectBacias = document.getElementById("select-bacia");
      $('#select-bacia').empty();
  
      fetchData(`estacoes-meteorologicas/bacias`).then((response) => {
        response.forEach((bacia) => {
          const option = document.createElement('option');
          option.innerText = bacia.nome;
          option.value = bacia.nome;
          selectBacias.appendChild(option);
        })
  
        $('#select-bacia').selectpicker('refresh');
      })
    })();
  

  
    const atualizarMapa = () => {
      const atualizar = document.getElementById('forcarAtualizacao').checked;
      fetchData(`estacoes-meteorologicas/chuva-acumulada?dtInicial=${dtInicial.value}&dtFinal=${dtFinal.value}&atualizar=${atualizar}`).then((response) => {
        mapa.criarMarcadores(response, dtInicial.value, dtFinal.value);
      })
    }
  
    let datas = await fetchData('estacoes-meteorologicas/datas-coleta');
    let slider = new Slider(datas['datas'], document.getElementById("slider"), dtInicial, dtFinal, () => { atualizarMapa() }, 24);
  
    slider.filtro();
  
    document.getElementById("buscar-dados").addEventListener('click', () => { slider.filtro() })
    document.getElementById("mm-chuva").addEventListener('change', () => { slider.filtro() })
    const mapa = new Mapa('main', cores, dtInicial.value, dtFinal.value);
    criarLabelCores(cores, 'labelColors', 'labelText')
  
  })
  
  function criarLabelCores(listColors, idColors, idText) {
    const parametros = ['0mm', '1mm', '5mm', '10mm', '15mm', '20mm', '25mm', '30mm', '40mm', '50mm', '75mm', '100mm', '150mm', '200mm']
  
    const elementColors = document.getElementById(idColors)
    const elementText = document.getElementById(idText)
    let widthDiv = 100 / listColors.length;
    for (let cor in listColors) {
      let divCores = document.createElement('div')
      divCores.style.backgroundColor = listColors[cor]
      divCores.style.width = `${widthDiv}%`;
      divCores.style.height = "100%"
      divCores.style.display = "flex"
      elementColors.appendChild(divCores)
  
      let divParametros = document.createElement('div')
      divParametros.style.width = `${widthDiv}%`;
      divParametros.style.height = "100%"
      divParametros.style.display = "flex"
      // divParametros.style.border = "1px solid #0005"
  
      // labelText.style = 'border: 1px solid #000A';
  
      if (cor > 0) {
        divParametros.innerHTML = parametros[cor - 1]
      }
      elementText.appendChild(divParametros)
    }
  
  }
  
  // fonte/origem da funcao para setar o zindex do marker do leaflet manualmente: 
  // https://gis.stackexchange.com/a/263252
  ((global) => {
    var MarkerMixin = {
      _updateZIndex: function (offset) {
        this._icon.style.zIndex = this.options.forceZIndex ? (this.options.forceZIndex + (this.options.zIndexOffset || 0)) : (this._zIndex + offset);
      },
      setForceZIndex: function (forceZIndex) {
        this.options.forceZIndex = forceZIndex ? forceZIndex : null;
      }
    };
    if (global) global.include(MarkerMixin);
  })(L.Marker);
  