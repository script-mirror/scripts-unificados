let tipos = ['ear', 'vaz_afluente', 'vaz_turbinada', 'vaz_vertida', 'vaz_defluente', 'vaz_incremental']
const meses = { 'Janeiro': 1, 'Fevereiro': 2, 'Março': 3, 'Abril': 4, 'Maio': 5, 'Junho': 6, 'Julho': 7, 'Agosto': 8, 'Setembro': 9, 'Outubro': 10, 'Novembro': 11, 'Dezembro': 12 }
let tipo_names = {
  'ear': "Volume Armazenado %",
  "vaz_afluente": "Afluência (m³/s)",
  "vaz_turbinada": "Turbinamento (m³/s) ",
  "vaz_vertida": "Vertimento (m³/s)",
  "vaz_defluente": "Defluência (m³/s)",
  "vaz_incremental": "Incremental (m³/s)",
}

// function filterDataOnDate(chart) {
//   chart.config.data.datasets.forEach((dataset) => {
    
//     const startdate = moment(''+meses[document.getElementById('data-inicio').value]).startOf('month').format('MM-DD');
//     const enddate = moment(''+meses[document.getElementById('data-fim').value]).endOf('month').format('MM-DD');
//     const resultado = _.pickBy(dataset.data, (valor, chave) => {
//       return moment(chave).format('MM-DD') >= startdate && moment(chave).format('MM-DD') <= enddate;
//     });
//     dataset.data = resultado;

//   })
//   chart.update();
// }
function filterDataOnDate(dataset) {
    const startdate = moment(''+meses[document.getElementById('data-inicio').value]).startOf('month').format('MM-DD');
    const enddate = moment(''+meses[document.getElementById('data-fim').value]).endOf('month').format('MM-DD');
    const resultado = _.pickBy(dataset.data, (valor, chave) => {
      return moment(chave).format('MM-DD') >= startdate && moment(chave).format('MM-DD') <= enddate;
    });
    dataset.data = resultado;
    return dataset;

}

moment.locale('pt-br');

let anos = [];
class Dataset {
  constructor(label, data, fill, color, type) {
    this.label = label;
    this.data = data;
    this.pointStyle = false;
    this.borderColor = color;
    this.backgroundColor = color;
    this.fill = fill;
    this.type = type;
    this.borderWidth = 2;
    this.pointRadius = false
  }
}
let gradientChartOptionsConfiguration = {
  responsive: true, maintainAspectRatio: false,
  plugins: {
    legend: {display: true,position: 'bottom'},
    tooltip: {intersect: false,mode: 'index', callbacks: {title: function (e) {return e[0].label.split(',')[0]}}},
  },
  scales: {
    y: {
      type: 'linear',position: 'left', 
      title: {display: true,font: {size: 20,}}, 
      grid: {drawOnChartArea: true,},
      ticks: {display: true,
        font: {size: 15,},
      beginAtZero: true}},
    x: {type: 'time',
        time: { displayFormats: { month: 'MMM' } }},
  },
};





let charts = {};
let chartEna = {};
let chartEnaMensal = {};
let graphs = document.getElementById("graphs")
function criarEstiloOrientacaoGrafico() {
  const orientacaoGraficos = `
  <style id="orientacao-graficos">
    .tab-pane {
    display: block !important;
    opacity: 1 !important;
    visibility: visible !important;
    }
</style>`
  document.head.innerHTML += orientacaoGraficos
}
function rotateDivCharts() {
  let orientacaoGraficos = document.getElementById("orientacao-graficos")
  orientacaoGraficos == null ? criarEstiloOrientacaoGrafico() : orientacaoGraficos.remove()

}

function create_chart() {

  for (let tipo of tipos) {

    charts[tipo] = {}

    let ctx = document.getElementById("chart" + tipo).getContext("2d");

    gradientChartOptionsConfiguration['scales']['x']['title'] = {
      display: true,
      text: tipo_names[tipo],
      fontSize: 20,
      font: {
        size: 20,
        weight: 'bold',
        lineHeight: 1.2,
      },
      padding: { top: 20, left: 0, right: 0, bottom: 0 }
    }


    charts[tipo] = new Chart(ctx, {
      type: 'bar',
      options: gradientChartOptionsConfiguration,

    });
  }
}


let dadosPorAno = {}



async function httpGet(url) {
  const promisse = await fetch(url, {
    method: "GET",
    headers: {
      contentType: "application/json; charset=utf-8"
    }
  })
  const response = await promisse.json();
  return response;
}


function plotRdh(ano, tipo) {
  modalLoading.style.display = "block";
  const mesInicio = meses[inputMesInicio.value]
  const mesFim = meses[inputMesFim.value]

  modalLoading.style.display = "none";
  httpGet(`/middle/API/get/rdh?ano=${ano}&tipo=${tipo}`).then((objeto) => {


    objeto_armazenamnto = objeto['valores'][tipo]
    if (dadosPorAno[`${ano}${mesInicio}${mesFim}`] == undefined) {
      dadosPorAno[`${ano}${mesInicio}${mesFim}`] = {};
    }
    dadosPorAno[`${ano}${mesInicio}${mesFim}`][tipo] = {}

    for (let posto of Object.keys(objeto_armazenamnto)) {
      dadosPosto = []

      if (getColor_byYear(ano) == undefined) {
        color = ramdomColor();
      }
      else {
        color = getColor_byYear(ano)
      }

      Object.keys(objeto_armazenamnto[posto]).forEach((key) => {
        objeto_armazenamnto[posto][key.slice(5)] = objeto_armazenamnto[posto][key]
        delete objeto_armazenamnto[posto][key]
      })
      let dataset = new Dataset(posto + `(${ano})`, objeto_armazenamnto[posto], false, color[0], 'line')
      dadosPorAno[`${ano}${mesInicio}${mesFim}`][tipo][posto] = filterDataOnDate(dataset)
    }

    let posto = select_posto.value

    charts[tipo]['config']['_config'].data.datasets.push(dadosPorAno[`${ano}${mesInicio}${mesFim}`][tipo][posto])
    charts[tipo].update()
  });
}



function iniciarListeners() {
  $('#sidebarCollapse').on('click', function () {
    $('#sidebar').toggleClass('active');
  });

  $('#btnMaxMin').on('click', rotateDivCharts);
  $('#btnAtualizar').on('click', atualizarCurvas);
  $('#btnListPostos').on('change', atualizarCurvas);
  $('#btnAnos').on('change', atualizarCurvas);

  $('[data-toggle="tooltip"]').tooltip()
}

const modalLoading = document.getElementById("modalLoading");

$(window).on('load', async function () {
  await createSelect()
  await createBtnAno()
  create_chart()
  for await(tipo of tipos) {
    plotRdh(moment().format("YYYY"), tipo)
    plotRdh(moment(moment().year()-1+'').format("YYYY"), tipo)

  }


  botoes = document.querySelectorAll('.btn-anos');

  botoes.forEach(b => {
    if (b.textContent == moment().format("YYYY")) {
      b.classList.remove('btn-light')
      b.classList.add('btn-success');
    }
  });
  iniciarListeners()

})




const btnListDiv = document.getElementById("btnListPostos")
let select_posto = document.getElementById('postos')
async function createSelect() {
  $('#postos').empty();
  const postos = await httpGet('/middle/API/get/rdh/utils/postos')
  for (let posto of postos['postos']) {
    let option = document.createElement('option');
    option.value = posto;
    option.textContent = posto;
    $('#postos').append(option);
  }
  $('#postos').selectpicker('refresh');


  let firstOptionValue = $('#postos option:first').val();
  select_posto.value = firstOptionValue;

  $(select_posto).trigger('change');

}

let btnDiv = document.getElementById("btnAnos");

async function createBtnAno() {
  $('#btnAnos').empty();
  const anosObj = await httpGet('/middle/API/get/rdh/utils/anos');

  for (let ano of anosObj['anos'].reverse()) {
    let btn_aux = document.createElement("option");
    btn_aux.textContent = ano;

    $('#btnAnos').append(btn_aux);

  }
  $('#btnAnos').selectpicker('refresh');

  let firstOptionValue = $('#btnAnos option:eq(0)').val();
  let secondOptionValue = $('#btnAnos option:eq(1)').val();
  $(btnDiv).val([firstOptionValue, secondOptionValue]);
  $(btnDiv).trigger('change');

}


btnDiv.addEventListener('click', (event) => {
  if (event.target.classList.contains('btn-anos')) {

    if (event.target.classList.contains('btn-light')) {
      event.target.classList.remove('btn-light')
      event.target.classList.add('btn-success');
    }

    else if (event.target.classList.contains('btn-success')) {
      event.target.classList.remove('btn-success')
      event.target.classList.add('btn-light');
    }

  }
});


function limparGraficos() {
  for (let tipo of tipos) {

    charts[tipo]['config']['_config'].data.datasets = []
    charts[tipo].update()
  }
}

function atualizarCurvas() {

  limparGraficos();
  const mesInicio = meses[inputMesInicio.value]
  const mesFim = meses[inputMesFim.value]
  let posto = select_posto.value;
  let anosSelecionados = $("#btnAnos").val();
  for (let ano of anosSelecionados) {
    for (let tipo of tipos) {
      if (dadosPorAno[`${ano}${mesInicio}${mesFim}`] == undefined || dadosPorAno[`${ano}${mesInicio}${mesFim}`][tipo] == undefined) {
        plotRdh(ano, tipo)
      } else {
        charts[tipo]['config']['_config'].data.datasets.push(dadosPorAno[`${ano}${mesInicio}${mesFim}`][tipo][posto])
      }
      charts[tipo].update();
    }
  }
}
const inputMesInicio = document.getElementById("data-inicio")
const inputMesFim = document.getElementById("data-fim")


let slider = new Slider(Object.keys(meses), document.getElementById("slider"), inputMesInicio, inputMesFim, atualizarCurvas, 0);
slider.sliderSetup.value([0, Object.keys(meses).length - 1])