let diaExibido;
let diaExibidoTemp = [];
// const timeFormat = 'DD/MM/YYYY';
const subsistemas = { 'SIN': 'SIN', 'SUDESTE': 'Sudeste', 'SUL': 'Sul', 'NORDESTE': 'Nordeste', 'NORTE': 'Norte' }

const coresFixasCargaHoraria = ['rgba(13, 0, 194, 1)', 'rgba(170, 157, 8, 1)', 'rgba(20, 163, 144, 1)', 'rgba(224, 107, 11, 1)', 'rgba(168, 0, 0, 1)', 'rgba(112, 0, 168, 1)', 'rgba(69, 232, 88, 1)', 'rgba(209, 0, 209, 1)']


const estiloLinha = { 'Geração Total': [1, 1], 'Geração Hidráulica': [10, 10], 'Geração Térmica': [15, 3, 3, 3], 'Geração Solar': [1, 1], 'Geração Eólica': [10, 10], 'Geração Nuclear': [15, 3, 3, 3] }

let coresCargaHoraria = {}
let cores_temperatura = {}

let charts = {}
let curvas = { 'Carga': {}, 'Previsão de carga': {}, 'Previsão de carga DP': {}, 'Previsao Carga Liquida DP': {}, 'Geração Total': {}, 'Geração Hidráulica': {}, 'Geração Térmica': {}, 'Geração Eólica': {}, 'Geração Nuclear': {}, 'Geração Solar': {}, 'Previsao Carga Liquida': {}, 'temperatura observada': {}, 'temperatura prevista': {} }


// Configuracoes para abortar todas as requisiçoes quando o usuario pedir para atualizar
$.xhrPool = [];
$.xhrPool.abortAll = function () {
  $(this).each(function (idx, jqXHR) {
    jqXHR.abort();
  });
  $(this).each(function (idx, jqXHR) {
    let index = $.inArray(jqXHR, $.xhrPool);
    if (index > -1) {
      $.xhrPool.splice(index, 1);
    }
  });
};

$.ajaxSetup({
  beforeSend: function (jqXHR) {
    $.xhrPool.push(jqXHR);
  },
  complete: function (jqXHR) {
    let index = $.inArray(jqXHR, $.xhrPool);
    if (index > -1) {
      $.xhrPool.splice(index, 1);
    }
  }
});


// Executar ao abrir a pagina



function iniciarListeners() {
  $('#sidebarCollapse').on('click', function () {
    $('#sidebar').toggleClass('active');
  });

  $('#data-previsao').on('change', getCargaLiquida);
  $('#data-previsao').on('change', getCargaLiquidaDP);


  $('#data-previsao').on('change', getTemperaturaPrev);

  $('#data-previsao').on('change', atualizar);
  $('#data-previsao').on('change', getCargaBlocoDP);
  $('#data-previsao').on('change', atualizarPrevisao);
  // $('#data-previsao').on('change', function () {
  //   filtrarCurvaPrevisao(diaExibido, false)
  //   diaExibido = $('#data-previsao').val()
  //   filtrarCurvaPrevisao(diaExibido, true)
  // });

  $('#btn_comutarLegenda').click(comutarLegenda);

  $('#btn_comutar_grafico_temperatura').on('click', function () {
    for (submercado in charts) {
      for (dataset of charts[submercado].data.datasets) {
        if (dataset.yAxisID != undefined) {

          if (dataset.type == 'bar') {
            dataset.type = 'line'
            charts[submercado].config.options.scales.yAxes[1].ticks = {}

            $('#icon-temperatura-bar')[0].style.display = ''
            $('#icon-temperatura-line')[0].style.display = 'none'
            charts[submercado].config.options.scales.yAxes[1].ticks['reverse'] = false
            charts[submercado].config.options.scales.yAxes[1].ticks.min = 15
            charts[submercado].config.options.scales.yAxes[1].ticks.max = 40
          }
          else {
            dataset.type = 'bar'
            $('#icon-temperatura-bar')[0].style.display = 'none'
            $('#icon-temperatura-line')[0].style.display = ''
            charts[submercado].config.options.scales.yAxes[1].ticks['reverse'] = true
            charts[submercado].config.options.scales.yAxes[1].ticks.min = 15
            charts[submercado].config.options.scales.yAxes[1].ticks.max = 40
          }

          charts[submercado].update()
        }
      }
    }
  });

}

function atualizarPrevisao() {

  data = moment($('#data-previsao').val())
  let $listDiasPrevisao = $('#listaDiasPrev')
  $listDiasPrevisao.empty()

  $.get('/middle/API/database/get/previsaoCargaDs', { 'data': data.format('YYYY-MM-DD') }, function (resposta) {

    let carga = resposta['carga']['PrevCarga']

    let prevCarga = {}
    for (sub in subsistemas) {
      if (sub == 'Temperatura') { }

      prevCarga[sub] = [];
      curvas['Previsão de carga'][sub] = {}

      for (dia in carga[sub]) {
        prevCarga[sub][dia] = [];

        for (hora in carga[sub][dia]) {

          if (carga[sub][dia][hora] != null) {
            prevCarga[sub][dia].push({ x: moment(hora, 'HH:mm'), y: carga[sub][dia][hora].toFixed(2) });
          }
          else {
            prevCarga[sub][dia].push({ x: moment(hora, 'HH:mm'), y: null });
          }
        }

        prevCarga[sub][dia].push({ x: moment('23:59', 'HH:mm'), y: carga[sub][dia][hora].toFixed(2) });


        let label, cor_curva;

        label = dia;

        if (coresCargaHoraria[label] == undefined) {
          if (Object.keys(coresCargaHoraria).length < coresFixasCargaHoraria.length) {
            coresCargaHoraria[label] = coresFixasCargaHoraria[Object.keys(coresCargaHoraria).length]
          }
          else {
            coresCargaHoraria[label] = ramdomColor()
          }
        }

        cor_curva = coresCargaHoraria[label];

        label = `${label}(prev${data.format('DD/MM')})`


        newDataset = {
          label: label,
          type: 'line',
          backgroundColor: cor_curva,
          borderColor: cor_curva,
          fill: false,
          data: prevCarga[sub][dia],
          pointRadius: 0,
          borderWidth: 1
        }

        // if(data.format('DD/MM/YYYY') != dia){
        //   newDataset.steppedLine = true
        // }


        curvas['Previsão de carga'][sub][dia] = newDataset

      }

    }
  })
  // .done(
  //   function(){
  //     tipoPlot()
  //   }
  // )
}


function getCargaBlocoDP() {

  data = moment($('#data-previsao').val())
  let $listDiasPrevisao = $('#listaDiasPrev')
  // $listDiasPrevisao.empty()

  $.get('/middle/API/database/get/previsaoCargaDs_DP', { 'data': data.format('YYYY-MM-DD') }, function (resposta) {

    if (resposta['carga'] === undefined) {
      // exibirAlerta(`Sem informação de previsao de carga do bloco DP para a data ${data.format('YYYY-MM-DD')} !`, 'danger', 5000)
      return
    }

    if (resposta['mensagem'] != undefined) {
      // criarAviso(resposta['mensagem'])
    }

    let carga = resposta['carga']['Bloco_DP']

    let prevCargaDP = {}
    for (sub in subsistemas) {
      if (sub == 'Temperatura') { }

      prevCargaDP[sub] = [];
      curvas['Previsão de carga DP'][sub] = {}

      for (dia in carga[sub]) {
        prevCargaDP[sub][dia] = [];

        for (hora in carga[sub][dia]) {

          if (carga[sub][dia][hora] != null) {
            prevCargaDP[sub][dia].push({ x: moment(hora, 'HH:mm'), y: carga[sub][dia][hora].toFixed(2) });
          }
          else {
            prevCargaDP[sub][dia].push({ x: moment(hora, 'HH:mm'), y: null });
          }
        }

        prevCargaDP[sub][dia].push({ x: moment('23:59', 'HH:mm'), y: carga[sub][dia][hora].toFixed(2) });


        let label, cor_curva;

        label = dia;

        if (coresCargaHoraria[label] == undefined) {
          // if (coresCargaHoraria[label] == undefined ){
          if (Object.keys(coresCargaHoraria).length < coresFixasCargaHoraria.length) {
            coresCargaHoraria[label] = coresFixasCargaHoraria[Object.keys(coresCargaHoraria).length]
          }
          else {
            coresCargaHoraria[label] = ramdomColor()
          }
        }

        cor_curva = coresCargaHoraria[label];

        label = `${label}(prevOficial${data.format('DD/MM')})`


        newDataset = {
          label: label,
          type: 'line',
          backgroundColor: cor_curva,
          borderColor: cor_curva,
          fill: false,
          data: prevCargaDP[sub][dia],
          pointRadius: 0,
          borderWidth: 1
        }

        curvas['Previsão de carga DP'][sub][dia] = newDataset

      }

    }

  })
  .done(
    function () {
      tipoPlot()
    }
  )

}

function filtrarCurvaPrevisao(label, mostrar) {



  for (sub in subsistemas) {
    if (sub == 'Temperatura') { }

    if (mostrar == true) {

      if (label.endsWith('(Liquida)')) {
        label_aux = label.replace(' (Liquida)', '')

        if (curvas['Previsao Carga Liquida'][sub][label_aux] != undefined) {
          charts[sub].data.datasets.push(curvas['Previsao Carga Liquida'][sub][label_aux])
        }
      }
      else if (label.endsWith('(Liquida Oficial)')) {
        label_aux = label.replace(' (Liquida Oficial)', '')

        if (curvas['Previsao Carga Liquida DP'][sub][label_aux] != undefined) {
          charts[sub].data.datasets.push(curvas['Previsao Carga Liquida DP'][sub][label_aux])
        }
      }

      else if (label.endsWith('(temperatura)')) {
        label_aux = label.replace(' (temperatura)', '')

        
        if (curvas['temperatura prevista'][sub] != undefined) {
          charts[sub].data.datasets.push({ ...curvas['temperatura prevista'][sub][label_aux] });
          
          // for(diaPrevisao in curvas['temperatura prevista'][sub][label_aux]){
            // charts[sub].data.datasets.push({ ...curvas['temperatura prevista'][sub][diaPrevisao] });
          // }
        }

      }
      else if (label.endsWith('(Oficial)') && curvas['Previsão de carga DP'][sub][label.replace(' (Oficial)', '')] != undefined) {
        label_aux = label.replace(' (Oficial)', '')
        charts[sub].data.datasets.push({ ...curvas['Previsão de carga DP'][sub][label_aux] });

      }
      else {
        label_aux = label.replace(' (Oficial)', '')
        charts[sub].data.datasets.push({ ...curvas['Previsão de carga'][sub][label_aux] });

      }

    }
    else {

      if (label.endsWith('(Liquida)')) {
        label_aux = label.replace(' (Liquida)', '')

        charts[sub].data.datasets = charts[sub].data.datasets.filter(function (curva) {
          return !curva.label.match(`${label_aux}[(]prevLiquida[0-9]{2}/[0-9]{2}[)]`)
        })

      }
      else if (label.endsWith('(Liquida Oficial)')) {
        label_aux = label.replace(' (Liquida Oficial)', '')

        charts[sub].data.datasets = charts[sub].data.datasets.filter(function (curva) {
          return !curva.label.match(`${label_aux}[(]prevLiquidaOficial[0-9]{2}/[0-9]{2}[)]`)
        })

      }
      else if (label.endsWith('(temperatura)')) {
        label_aux = label.replace(' (temperatura)', '')

        charts[sub].data.datasets = charts[sub].data.datasets.filter(function (curva) {
          return !curva.label.match(`${label_aux}[(]prevTemp[0-9]{2}/[0-9]{2}[)]`)
        })

      }
      else if (label.endsWith('(Oficial)')) {
        label_aux = label.replace(' (Oficial)', '')

        charts[sub].data.datasets = charts[sub].data.datasets.filter(function (curva) {
          return !curva.label.match(`${label_aux}[(]prevOficial[0-9]{2}/[0-9]{2}[)]`)
        })

      }

      else {

        charts[sub].data.datasets = charts[sub].data.datasets.filter(function (curva) {
          return !curva.label.match(`${label}[(]prev[0-9]{2}/[0-9]{2}[)]`)
        })

      }



    }
    charts[sub].update();

  }

}


function atualizar() {
  
  let dtSelecionada;
  dtSelecionada = moment($('#data-previsao').val(), 'YYYY-MM-DD').format('DD/MM/YYYY')
  // $.xhrPool.abortAll();


  let $checkboxCarga = $("input[type='checkbox'][name='carga']:checked");
  let $checkboxGeracao = $("input[type='checkbox'][name='geracao[]']:checked");
  let $checkboxTemperatura = $("input[type='checkbox'][name='temperatura']:checked");

  datasets = {}
  for (sub in subsistemas) {

    datasets[sub] = charts[sub].data.datasets.filter(function (curva) {
      return curva.label.match(`.*(prevLiquida[0-9]{2}/[0-9]{2})`)
    })

    datasets[sub] = datasets[sub].concat(charts[sub].data.datasets.filter(function (curva) {
      return curva.label.match(`.*(prev[0-9]{2}/[0-9]{2})`)
    }))

    datasets[sub] = datasets[sub].concat(charts[sub].data.datasets.filter(function (curva) {
      return curva.label.match(`.*(prevTemp[0-9]{2}/[0-9]{2})`)
    }))

    charts[sub].data.datasets = datasets[sub]
  }


  if(document.querySelector('input[name="radioTipoPlot"]:checked').value == "temp"){
    for (sub in subsistemas) {

      charts[sub].update();
    }
  }else
  if (curvas['Carga'][dtSelecionada] == undefined) {
    getCarga(dtSelecionada)
  }
  else {
    for (sub in subsistemas) {

      charts[sub].data.datasets.push({ ...curvas['Carga'][dtSelecionada][sub] })
      charts[sub].update();
    }
  }


}

function getGeracao(data) {
  data = moment(data, 'DD/MM/YYYY')

  let $checkboxGeracao = $("input[type='checkbox'][name='geracao[]']:checked");
  curvasGeracaoPlot = []
  for (let ckcbox of $checkboxGeracao) {
    curvasGeracaoPlot.push(ckcbox.value)
  }


  $.get('/middle/API/database/get/geracaoHoraria', { 'data': data.format('YYYY-MM-DD') }, function (resposta) {

    if (Object.entries(resposta['geracao']).length === 0) {
      exibirAlerta(`Sem informação de geração para a data ${data.format('DD/MM/YYYY')} !`, 'danger', 5000)

      return
    }

    geracao = {}
    for (sub in subsistemas) {
      if (sub == 'Temperatura') { }

      geracao[sub] = {}
      for (tipoGeracao in resposta['geracao'][sub]) {
        geracao[sub][tipoGeracao] = []
      }

      for (tipoGeracao in resposta['geracao'][sub]) {

        for (hora in resposta['geracao'][sub][tipoGeracao]) {
          if (resposta['geracao'][sub][tipoGeracao][hora] != null) {
            geracao[sub][tipoGeracao].push({ x: moment(hora, 'HH:mm'), y: resposta['geracao'][sub][tipoGeracao][hora].toFixed(2) })
          }
          else {
            geracao[sub][tipoGeracao].push({ x: moment(hora, 'HH:mm'), y: null })
          }

        }
      }

    }


    data_str = data.format('DD/MM/YYYY')
    label = data_str

    for (sub in subsistemas) {
      if (sub == 'Temperatura') { }

      for (tipoGeracao in resposta['geracao'][sub]) {
        tipo = tipoGeracao.substring('Geração '.length)

        labelCurva = `${label} (${tipo})`

        if (coresCargaHoraria[data_str] == undefined) {

          if (Object.keys(coresCargaHoraria).length < coresFixasCargaHoraria.length) {
            coresCargaHoraria[data_str] = coresFixasCargaHoraria[Object.keys(coresCargaHoraria).length]
          }
          else {
            coresCargaHoraria[data_str] = ramdomColor()
          }
        }

        newDataset = {
          label: labelCurva,
          type: 'line',
          backgroundColor: coresCargaHoraria[data_str],
          borderColor: coresCargaHoraria[data_str],
          fill: false,
          pointRadius: 0,
          data: geracao[sub][tipoGeracao],
          borderDash: estiloLinha[tipoGeracao]
        }

        if (curvas[tipoGeracao][data_str] == undefined) {
          curvas[tipoGeracao][data_str] = {}
        }
        curvas[tipoGeracao][data_str][sub] = newDataset

        if (curvasGeracaoPlot.includes(tipoGeracao)) {
          charts[sub].data.datasets.push({ ...newDataset })
        }

      }
      charts[sub].update();
    }

  })

}

function getCarga(data) {

  data = moment(data, 'DD/MM/YYYY')
  const dataPrev = data.format('DD/MM/YYYY');
  $.get('/middle/API/database/get/cargaHoraria', { 'data': data.format('YYYY-MM-DD') }, function (resposta) {
    let carga = {}
    if (Object.entries(resposta.carga).length === 0) {
      exibirAlerta(`Sem informação de carga para a data ${data.format('DD/MM/YYYY')} !`, 'danger', 5000)
      return
    }

    for (sub in subsistemas) {
      if (carga[sub] == undefined) {
        carga[sub] = []
      }

      for (hora in resposta['carga'][sub]) {
        if (resposta['carga'][sub][hora] != null) {
          carga[sub].push({ x: moment(hora, 'HH:mm'), y: resposta['carga'][sub][hora].toFixed(2) })
        }
        else {
          carga[sub].push({ x: moment(hora, 'HH:mm'), y: null })
        }
      }
    }

    let label = data.format('DD/MM/YYYY')

    if (coresCargaHoraria[label] == undefined) {
      if (Object.keys(coresCargaHoraria).length < coresFixasCargaHoraria.length) {
        coresCargaHoraria[label] = coresFixasCargaHoraria[Object.keys(coresCargaHoraria).length]
      }
      else {
        coresCargaHoraria[label] = ramdomColor()
      }
    }

    let cor_curva = coresCargaHoraria[label]

    for (sub in subsistemas) {
      if (sub == 'Temperatura') { }

      newDataset = {
        label: label,
        type: 'line',
        backgroundColor: cor_curva,
        borderColor: cor_curva,
        fill: false,
        data: carga[sub],
        pointRadius: 0,
      }

      if (curvas['Carga'][label] == undefined) {
        curvas['Carga'][label] = {}
      }
      curvas['Carga'][label][sub] = newDataset

      charts[sub].data.datasets.push({ ...newDataset })
      charts[sub].update();

    }
  })
}

function ramdomColor() {

  int1 = Math.floor(255 * Math.random())
  int2 = Math.floor(255 * Math.random())
  int3 = Math.floor(255 * Math.random())
  str_defalt = 'rgba(' + int1 + ',' + int2 + ',' + int3 + ',1)'
  return str_defalt
}

function comutarLegenda() {
  for (subm in charts) {
    charts[subm].options.legend.display = !charts[subm].options.legend.display
    charts[subm].update();
  }
}

function rmElemento(elm, tempo) {
  //elm [element html] -> elemento a ser removido
  // tempo [int] -> numero de milisegundos ate que a messagem desapareca, coloque 0 para nao desaparecer

  setTimeout(
    function () {
      elm.remove();
    }, tempo);

}


let resposta_carga_liquida
let prevCargaLiquida

function getCargaLiquida() {
  data = moment($('#data-previsao').val())

  let $listDiasPrevisao = $('#listaDiasPrevLiquida')

  $listDiasPrevisao.empty()

  $.get('/middle/API/get/prev_carga_liquida', { 'data_referente': data.format('YYYY-MM-DD'), 'fonte_carga': 'PrevCargaDessem' }, function (resposta) {


    resposta_carga_liquida = resposta


    if (Object.entries(resposta_carga_liquida['carga liquida']).length === 0) {
      // exibirAlerta(`Sem informação de carga liquida prevista para a data ${dt_prev.format('DD/MM/YYYY')} !`, 'danger', 5000)

      return
    }

    let carga = resposta_carga_liquida['carga liquida']


    // for(let dt in carga['SUDESTE']){

    //   let $ckb, $label, $li, $div, id;

    //   id = 'prev_' + dt.replace(/[/]/g, '_')

    //   $div = $('<div>')
    //           .addClass('form-check')
    //           .appendTo($listDiasPrevisao);

    //   $ckb = $('<input>')
    //           .addClass('form-check-input')
    //           .attr('type','checkbox')
    //           .attr('id',id)
    //           .val(dt + ' (Liquida)')
    //           .appendTo($div);

    //   $label = $('<label>')
    //           .addClass('form-check-label')
    //           .attr('for', id)
    //           .text(dt)
    //           .appendTo($div);

    //   $ckb.change(
    //     function() {
    //       filtrarCurvaPrevisao(this.value, this.checked)
    //     })

    // }



    prevCargaLiquida = {}
    for (submercado in carga) {

      curvas['Previsao Carga Liquida'][submercado] = {}

      prevCargaLiquida[submercado] = {}

      for (dia in carga[submercado]) {

        prevCargaLiquida[submercado][dia] = []

        for (hora in carga[submercado][dia]) {

          prevCargaLiquida[submercado][dia].push({ x: moment(hora, 'HH:mm'), y: carga[submercado][dia][hora].toFixed(2) })

        }
        prevCargaLiquida[submercado][dia].push({ x: moment('23:59', 'HH:mm'), y: carga[submercado][dia][hora].toFixed(2) });

        let label, cor_curva;

        label = dia;

        if (coresCargaHoraria[label] == undefined) {
          if (Object.keys(coresCargaHoraria).length < coresFixasCargaHoraria.length) {
            coresCargaHoraria[label] = coresFixasCargaHoraria[Object.keys(coresCargaHoraria).length]
          }
          else {
            coresCargaHoraria[label] = ramdomColor()
          }
        }

        cor_curva = coresCargaHoraria[label];

        label = `${label}(prevLiquida${data.format('DD/MM')})`


        newDataset = {
          label: label,
          type: 'line',
          backgroundColor: cor_curva,
          borderColor: cor_curva,
          fill: false,
          data: prevCargaLiquida[submercado][dia],
          pointRadius: 0,
          borderWidth: 1
        }


        curvas['Previsao Carga Liquida'][submercado][dia] = newDataset


      }
    }

  })

}


function getCargaLiquidaDP() {
  data = moment($('#data-previsao').val())

  $.get('/middle/API/get/prev_carga_liquida', { 'data_referente': data.format('YYYY-MM-DD'), 'fonte_carga': 'bloco_dp' }, function (resposta) {


    resposta_carga_liquida_dp = resposta


    if (Object.entries(resposta_carga_liquida_dp['carga liquida']).length === 0) {

      return
    }

    let carga = resposta_carga_liquida_dp['carga liquida']


    prevCargaLiquidaDP = {}
    for (submercado in carga) {

      curvas['Previsao Carga Liquida DP'][submercado] = {}

      prevCargaLiquidaDP[submercado] = {}

      for (dia in carga[submercado]) {

        prevCargaLiquidaDP[submercado][dia] = []

        for (hora in carga[submercado][dia]) {

          prevCargaLiquidaDP[submercado][dia].push({ x: moment(hora, 'HH:mm'), y: carga[submercado][dia][hora].toFixed(2) })

        }
        prevCargaLiquidaDP[submercado][dia].push({ x: moment('23:59', 'HH:mm'), y: carga[submercado][dia][hora].toFixed(2) });

        let label, cor_curva;

        label = dia;

        if (coresCargaHoraria[label] == undefined) {
          if (Object.keys(coresCargaHoraria).length < coresFixasCargaHoraria.length) {
            coresCargaHoraria[label] = coresFixasCargaHoraria[Object.keys(coresCargaHoraria).length]
          }
          else {
            coresCargaHoraria[label] = ramdomColor()
          }
        }

        cor_curva = coresCargaHoraria[label];

        label = `${label}(prevLiquidaOficial${data.format('DD/MM')})`


        newDataset = {
          label: label,
          type: 'line',
          backgroundColor: cor_curva,
          borderColor: cor_curva,
          fill: false,
          data: prevCargaLiquidaDP[submercado][dia],
          pointRadius: 0,
          borderWidth: 1
        }


        curvas['Previsao Carga Liquida DP'][submercado][dia] = newDataset


      }
    }

  })

}



let respostaTemperaturaObs
let temperaturaObs

function getTemperaturaObs(dt) {
  dt_ini = moment(dt, 'DD/MM/YYYY')
  dt_fim = moment(dt, 'DD/MM/YYYY').add(1, 'days')

  $.get('/middle/API/get/temperatura_obs', { 'dt_ini': dt_ini.format('YYYY-MM-DD'), 'dt_fim': dt_fim.format('YYYY-MM-DD') }, function (resposta) {


    respostaTemperaturaObs = resposta


    if (respostaTemperaturaObs['temperatura'] == undefined) {

      return
    }

    let temperatura = respostaTemperaturaObs['temperatura']

    temperaturaObs = {}
    for (submercado in temperatura) {

      curvas['temperatura observada'][submercado] = {}

      temperaturaObs[submercado] = {}

      for (dia in temperatura[submercado]) {

        dia_aux = moment(dia).format('DD/MM/YYYY')

        temperaturaObs[submercado][dia_aux] = []

        for (hora in temperatura[submercado][dia]) {

          temperaturaObs[submercado][dia_aux].push({ x: moment(hora, 'HH:mm'), y: temperatura[submercado][dia][hora].toFixed(1) })

        }
        temperaturaObs[submercado][dia_aux].push({ x: moment('23:59', 'HH:mm'), y: temperatura[submercado][dia][hora].toFixed(1) });

        let label, cor_curva;

        label = dia_aux;

        if (cores_temperatura[label] == undefined) {
          if (Object.keys(coresCargaHoraria).length < cores_temperatura.length) {
            cores_temperatura[label] = cores_temperatura[Object.keys(coresCargaHoraria).length]
          }
          else {
            cores_temperatura[label] = ramdomColor()
          }
        }

        cor_curva = cores_temperatura[label];

        label = `${label}(TempObs${dt_ini.format('DD/MM')})`


        newDataset = {
          label: label,
          type: 'line',
          yAxisID: 'eixo_temperatura',
          backgroundColor: cor_curva,
          borderColor: cor_curva,
          fill: false,
          data: temperaturaObs[submercado][dia_aux],
          borderWidth: 1,
          pointStyle: 'circle',
          stack: 'Stack 1'
        }


        curvas['temperatura observada'][submercado][dia_aux] = newDataset

        charts[submercado].data.datasets.push({ ...newDataset })
        charts[submercado].update();

      }
    }

  })

}



let respostaTemperaturaPrev
let temperaturaPrev

function getTemperaturaPrev() {

  dt_prev = moment($('#data-previsao').val())//.subtract(1,'days')

  $.get('/middle/API/get/temperatura_prev',
    {
      'dt_deck': dt_prev.format('YYYY-MM-DD'),
    },

    function (resposta) {




      let $listDiasPrevisao = $('#listaDiasPrevTemperatura')
      respostaTemperaturaPrev = resposta


      $listDiasPrevisao.empty()

      if (respostaTemperaturaPrev['temperatura prev'] === undefined) {
        // exibirAlerta(`Sem informação de temperatura prevista para a data ${dt_prev.format('DD/MM/YYYY')} !`, 'danger', 5000)

        return
      }

      if (respostaTemperaturaPrev['mensagem'] != undefined) {
        // criarAviso(respostaTemperaturaPrev['mensagem'])
      }

      let temperatura = respostaTemperaturaPrev['temperatura prev'][dt_prev.format('YYYY-MM-DD')]


      temperaturaPrev = {}
      for (submercado in temperatura) {

        curvas['temperatura prevista'][submercado] = {}

        temperaturaPrev[submercado] = {}

        for (dia in temperatura[submercado]) {

          dia_aux = moment(dia).format('DD/MM/YYYY')

          temperaturaPrev[submercado][dia_aux] = []

          for (hora in temperatura[submercado][dia]) {

            temperaturaPrev[submercado][dia_aux].push({ x: moment(hora, 'HH:mm'), y: temperatura[submercado][dia][hora].toFixed(2) })

          }
          temperaturaPrev[submercado][dia_aux].push({ x: moment('23:59', 'HH:mm'), y: temperatura[submercado][dia][hora].toFixed(2) });

          let label, cor_curva;

          label = dia_aux;


          if (cores_temperatura[label] == undefined) {
            if (Object.keys(coresCargaHoraria).length < cores_temperatura.length) {
              cores_temperatura[label] = cores_temperatura[Object.keys(coresCargaHoraria).length]
            }
            else {
              cores_temperatura[label] = ramdomColor()
            }
          }


          cor_curva = cores_temperatura[label];



          label = `${label}(prevTemp${dt_prev.format('DD/MM')})`


          newDataset = {
            label: label,
            type: 'line',
            yAxisID: 'eixo_temperatura',
            backgroundColor: cor_curva,
            borderColor: cor_curva,
            fill: false,
            data: temperaturaPrev[submercado][dia_aux],
            pointRadius: 1,
            borderWidth: 1,
            pointStyle: 'circle',
            stack: 'Stack 0'

          }


          curvas['temperatura prevista'][submercado][dia_aux] = newDataset
        }
      }

    }).done(
      function(){
        diaExibidoTemp = Object.keys(curvas['temperatura prevista']["NORDESTE"])
      }
    )

}

function tipoPlot() {
  let divSin = document.getElementById("div-sin");
  const radioChecado = document.querySelector('input[name="radioTipoPlot"]:checked').value
  const dataPrevisao = document.getElementById('data-previsao');

  if (radioChecado == 'temp') {
    dataPrevisao.readOnly = true;
    divSin.style.display = 'none';
    filtrarCurvaPrevisao(`${diaExibido} (Oficial)`, false)
    diaExibido = moment($('#data-previsao').val()).format('DD/MM/YYYY')

    for(dia in diaExibidoTemp){
      filtrarCurvaPrevisao(`${diaExibidoTemp[dia]} (temperatura)`, true)
    }

  } else if (radioChecado == 'carga') {
    dataPrevisao.readOnly = false;
    divSin.style.display = 'block';
    filtrarCurvaPrevisao(`${diaExibido} (Oficial)`, false)

    for(dia in diaExibidoTemp){
      filtrarCurvaPrevisao(`${diaExibidoTemp[dia]} (temperatura)`, false)
    }
    diaExibido = moment($('#data-previsao').val()).format('DD/MM/YYYY')
    filtrarCurvaPrevisao(`${diaExibido} (Oficial)`, true)
  }

}




function exibirAlerta(msg, tipo, tempo) {
  // msg [str] -> mensagem a ser mostrada
  // tipo [str] -> tipo de messagem  (success, danger, info)
  // tempo [int] -> numero de milisegundos ate que a messagem desapareca, coloque 0 para nao desaparecer
  // exibirAlerta('Mensagem do tipo Danger quer ira desparecer em 3 sec', 'danger', 2000)
  // div_alerta = document.getElementById('alertas')
  // let new_elm = document.createElement('div');
  // new_elm.classList.add('alert', `alert-${tipo}`, 'col-8', 'center', 'mt-3');
  // new_elm.setAttribute('role', 'alert')
  // new_elm.innerText = msg
  // div_alerta.appendChild(new_elm)


  // if (tempo != 0){
  //   rmElemento(new_elm, tempo)
  // }

}


// function criarAviso(msg) {
//   // Criação dos elementos HTML
//   divAlerta = document.getElementById('alertas')
//   let new_elm = document.createElement('div');
//   new_elm.className = 'alert alert-warning alert-dismissible';
//   new_elm.role = 'alert';

//   let botaoFechar = document.createElement('button');
//   botaoFechar.type = 'button';
//   botaoFechar.className = 'close';
//   botaoFechar.setAttribute('data-dismiss', 'alert');
//   botaoFechar.setAttribute('aria-label', 'Close');

//   let spanFechar = document.createElement('span');
//   spanFechar.setAttribute('aria-hidden', 'true');
//   spanFechar.innerHTML = '&times;';

//   let strong = document.createElement('strong');
//   strong.innerHTML = 'Warning!  ';

//   let mensagem = document.createTextNode(msg);

//   // Anexar elementos uns aos outros
//   botaoFechar.appendChild(spanFechar);
//   new_elm.appendChild(botaoFechar);
//   new_elm.appendChild(strong);
//   new_elm.appendChild(mensagem);

//   // Adicionar alerta ao corpo do documento
//   divAlerta.appendChild(new_elm)
// }

function plotarCargaHoraria(){
  hoje = moment()
  str_today = hoje.format('YYYY-MM-DD')
  $('#data-previsao').val(str_today)
  diaExibido = moment($('#data-previsao').val()).format('DD/MM/YYYY')

  // Criação dos canvas para os graficos
  span_graph = document.getElementById('graphs_carga')
  for (sub in subsistemas) {
      let div_aux = document.createElement('div');
      div_aux.className = 'col-12 col-sm-12 col-md-12 col-lg-11 center'
      if (subsistemas[sub] == 'SIN' || subsistemas[sub] == 'Temperatura') {
          div_aux.className = 'col-12';
          div_aux.id = 'div-sin';
          div_aux.style.height = '70vh';
      } else {
          div_aux.className = 'col-6';
          div_aux.style.height = '45vh';
      }

      let canvas_aux = document.createElement('canvas');
      canvas_aux.id = 'chart ' + sub

      div_aux.appendChild(canvas_aux);
      span_graph.appendChild(div_aux);
  }

  for (sub in subsistemas) {
      let ctx = document.getElementById('chart ' + sub).getContext('2d');
      const config = {
          type: 'line',
          options: {
              title: {
                  display: true,
                  text: subsistemas[sub],
              },
              legend: {
                  position: 'bottom',
              },
              scales: {
                  xAxes: [
                      {
                          type: 'time',
                          stacked: true,
                          time: {
                              unit: 'hour',
                              displayFormats: {
                                  hour: 'HH:mm'
                              },
                              tooltipFormat: 'HH:mm',
                          },
                          scaleLabel: {
                              display: true,
                              labelString: 'Data'
                          },
                          ticks: {
                              maxRotation: 45,
                              minRotation: 45   
                          }
                      },
                  ],
                  yAxes: [
                      {

                          id: 'eixo_carga',
                          scaleLabel: {
                              display: true,
                              labelString: 'Carga (MW)'
                          }
                      },

                      {
                          id: 'eixo_temperatura',
                          position: 'right',
                          // stacked: true,
                          scaleLabel: {
                              display: true,
                              labelString: 'Temperatura',
                          },

                          ticks: {
                              display: true,
                              max: 40,
                              min: 15,
                          },
                          gridLines: {
                              drawOnChartArea: false
                          },
                      },

                  ]
              },
              resposive: true,
              maintainAspectRatio: false,
          }
      }
      charts[sub] = new Chart(ctx, config)
  }

  iniciarListeners()
  atualizar()
  atualizarPrevisao()
  getTemperaturaPrev()
  getCargaBlocoDP()
}