const corpoTab = document.getElementById("corpoTabela");
const tabelaRodadas = document.getElementById("tabelaRodadas");
const btnChuva = document.getElementById("btnChuva");
const btnRev = document.getElementById("btnRev");
const GranularidadeSelect = document.getElementById("GranularidadeSelect");
const btnRun = document.getElementById('btn_run');
const inputDate = document.getElementById("inputDate")
GranularidadeSelect.value = "submercado"
var granularidade = GranularidadeSelect.value


var dt_rodada
var Objeto_ids=[]
var dataToSend = {}


var bacia_aux = {'GRANDE':'GRANDE', 'PARANAÍBA':'PARANAÍBA', 'ALTO TIETÊ':'ALTO TIETÊ', 'TIETÊ':'TIETÊ', 'XINGU':'XINGU' ,'PARANAPANEMA':'PARANAPANEMA',  'ALTO PARANÁ':'ALTO PARANÁ', 'BAIXO PARANÁ':'BAIXO PARANÁ', 'SÃO FRANCISCO (SE)':'SÃO FRANCISCO (SUDESTE)', 'TOCANTINS (SE)':'TOCANTINS (SUDESTE)', 'AMAZONAS (SE)':'AMAZONAS (SE)', 'IGUAÇU':'IGUAÇU', 'URUGUAI':'URUGUAI', 'PARANAPANEMA (S)':'PARANAPANEMA (SUL)', 'JACUÍ':'JACUÍ', 'CAPIVARI':'CAPIVARI', 'ITAJAÍ-AÇU':'ITAJAÍ-AÇU', 'SÃO FRANCISCO (NE)':'SÃO FRANCISCO (NORDESTE)', 'TOCANTINS (N)':'TOCANTINS (NORTE)', 'PARAÍBA DO SUL':'PARAÍBA DO SUL'}
var submercado_aux = {'N':'NORTE', 'NE':'NORDESTE', 'S':'SUL', 'SE':'SUDESTE'}

colorRV= getColor_byModelo("RV")[0]
colorMlt = getColor_byModelo("MLT")[0]
colorMerge = getColor_byModelo("MERGE")[0]






function rmElemento(elm, tempo){
    //elm [element html] -> elemento a ser removido
    // tempo [int] -> numero de milisegundos ate que a messagem desapareca, coloque 0 para nao desaparecer

    setTimeout(
        function() {
          elm.remove();
        }, tempo);
    
  }

  function exibirAlerta(msg, tipo, tempo){
    // msg [str] -> mensagem a ser mostrada
    // tipo [str] -> tipo de messagem  (success, danger, info)
    // tempo [int] -> numero de milisegundos ate que a messagem desapareca, coloque 0 para nao desaparecer
    // exibirAlerta('Mensagem do tipo Danger quer ira desparecer em 3 sec', 'danger', 2000)
    div_alerta = document.getElementById('alertas')
    var new_elm = document.createElement('div');
    new_elm.classList.add("alert", `alert-${tipo}`, "col-8", "center", "mt-3");
    new_elm.setAttribute('role', 'alert')
    new_elm.innerText = msg
    div_alerta.appendChild(new_elm)

    if (tempo != 0){
      rmElemento(new_elm, tempo)
    }

  }





function selectColor(rodada, modelo){


  if (getColor_byModelo(modelo) == undefined) {
    var colors = ramdomColor();
    }
  else{
    var colors = getColor_byModelo(modelo)
  }

  if(rodada === "00"){
      color = colors[0]
    }
    else if(rodada === "06"){
      color = colors[3]
    }
    else if(rodada === '12'){
      color = colors[4]
    }
    else if(rodada === '18'){
      color = colors[5]
    }


  return color
}



function change_data_table(table){
  
  if (table == 'tabelaRodadasPreliminares'){
    document.getElementById('tabelaRodadasPreliminares').hidden = false
    document.getElementById('tabelaRodadas').hidden = true
  }else {
    document.getElementById('tabelaRodadas').hidden = false
    document.getElementById('tabelaRodadasPreliminares').hidden = true
  }
}




function create_tables(){
  let tabelaPre = document.getElementById('tabelaRodadasPreliminares');

    var tabelaHTML = `
      <tr id="PRE00">
        <th id="0p" class="py-0" scope="col" onclick="activeRow('PRE00')">00</th>
      </tr>
      <tr id="PRE06">
        <th id="6p" class="py-0" scope="col" onclick="activeRow('PRE06')">06</th>
      </tr>
      <tr id="PRE12">
        <th id="12p" class="py-0" scope="col" onclick="activeRow('PRE12')">12</th>
      </tr>
      <tr id="PRE18">
        <th id="18p" class="py-0" scope="col" onclick="activeRow('PRE18')">18</th>
      </tr>
    `;

    // Define o conteúdo HTML da tabela dentro do elemento
    tabelaPre.innerHTML = tabelaHTML;
    

    let tabela = document.getElementById('tabelaRodadas');

    var tabelaHTML = `
      <tr id="00">
        <th id="0" class="py-0" scope="col" onclick="activeRow('00')">00</th>
      </tr>
      <tr id="06">
        <th id="6" class="py-0" scope="col" onclick="activeRow('06')">06</th>
      </tr>
      <tr id="12">
        <th id="12" class="py-0" scope="col" onclick="activeRow('12')">12</th>
      </tr>
      <tr id="18">
        <th id="18" class="py-0" scope="col" onclick="activeRow('18')">18</th>
      </tr>
    `;

    tabela.innerHTML = tabelaHTML;
}


function getModelosDisponiveis(dt_rodada){
  document.getElementById('tabelaRodadas').hidden = true
  document.getElementById('tabelaRodadasPreliminares').hidden = false
  $( "td" ).remove()
  $.ajax({
    method: "GET",
    url: "/middle/API/get/rodadas",
    data: {dt_rodada:dt_rodada},
  }).done(function (objeto) {
    
    for (let hr in objeto) {
      row = objeto[hr];
      for (let item in row){
        modelo = row[item].split("_")[0]
        id = row[item].split("_")[1]

        adicionaLinha(hr, id , modelo);
      }
    }
    adicionaCelulasNull("tabelaRodadas")
    adicionaCelulasNull("tabelaRodadasPreliminares")
    Objeto_ids=[]
    aux = {}
})
}


// CRIA A TABELA COM OS MODELOS
  function adicionaLinha(hr_rodada,id, modelo) {
    if (hr_rodada == 0){
      let r_0z = modelo + "00"
      if (modelo.includes(".PRE")){
        l_0 = document.getElementById('PRE00');
      }else
        l_0 = document.getElementById('00');
      html_0 = `<td class="py-0 " name = '${r_0z}' id = '${id}' onclick= "activeCell('${r_0z}','${id}')" style="font-size:11px" >${modelo}</td>`;
      
      l_0.innerHTML += html_0;

    }
    if (hr_rodada == 6){
      let r_6z = modelo + "06"
      if (modelo.includes(".PRE")){
        l_6 = document.getElementById('PRE06');
      }else
        l_6 = document.getElementById('06');
      html_6 =  `<td class="py-0 " name = '${r_6z}' id = '${id}' onclick= "activeCell('${r_6z}','${id}')" style="font-size:11px">${modelo}</td>`;
      
      l_6.innerHTML += html_6;
    }
    if (hr_rodada == 12){
      let r_12z = modelo + "12"
      if (modelo.includes(".PRE")){
        l_12 = document.getElementById('PRE12');
      }
      else
        l_12 = document.getElementById('12');
      html_12 = `<td class="py-0 " name = '${r_12z}' id = '${id}' onclick= "activeCell('${r_12z}','${id}')" style="font-size:11px">${modelo}</td>`;

      l_12.innerHTML += html_12;
    }
    if (hr_rodada == 18){
      let r_18z = modelo + "18"
      if (modelo.includes(".PRE")){
        l_18 = document.getElementById('PRE18');
      }else
        l_18 = document.getElementById('18');
      html_18 =`<td class="py-0 " name = '${r_18z}' id = '${id}' onclick= "activeCell('${r_18z}','${id}')" style="font-size:11px">${modelo}</td>`;
      
      l_18.innerHTML += html_18; 
    }
  }

  function adicionaCelulasNull(table){
    if (table.includes("Preliminares"))
        l_0 = document.getElementById('PRE00')
    else
      l_0 = document.getElementById('00')

    for(tr of document.getElementById(table).querySelectorAll('tr')){
      numTds = tr.querySelectorAll('td').length
      total_insert_celulas = parseInt(`${l_0.querySelectorAll('td').length}`) - parseInt(`${numTds}`)
      for (let i=0; i< total_insert_celulas; i++){
        html = `<td class="py-0" name = "-${tr.id}" id = "-" >-</td>`;
        tr.innerHTML += html;  
      }
  }

  }
  

  // ATIVA UMA CELULA DA LINHA
  function activeCell(name, id){
    
    if (name[0].indexOf("-") == -1){
      if (document.getElementById(id).classList.contains("table-success") == true){
        {
          document.getElementById(id).classList.remove("table-success");
          Reflect.deleteProperty(aux,id)
          Objeto_ids = aux
        }
      }
      else{
        document.getElementById(id).classList.toggle("table-success")
        
        aux[id] = name
        Objeto_ids = aux
          
      }
    }
  } 

  // ATIVA TODAS AS CELULAS DA LINHA
  function activeRow(coluna){
    if (coluna.includes("PRE")){
      array_td = document.getElementById('tabelaRodadasPreliminares').getElementsByTagName('td')
    }else{
      array_td = document.getElementById('tabelaRodadas').getElementsByTagName('td')
    }
    for (i = 0; i < array_td.length; i++) { 
      if (array_td[i].attributes.name.value.includes(coluna)){
        activeCell(array_td[i].attributes.name.value,array_td[i].id)
      }
    }
  }


  function activeTable(){
    navRodadas = document.getElementById('nav-rodadas')
    // if (navRodadas.classList.contains("active"))
      array_tr = document.getElementById('tabelaRodadas').getElementsByTagName('tr')
      for( tr of array_tr){
        activeRow(tr.id)
      }
    // else
      array_tr = document.getElementById('tabelaRodadasPreliminares').getElementsByTagName('tr')
    for( tr of array_tr){
      activeRow(tr.id)
    }
  }




// FUNÇÔES LISTENERS 

// ==============================================================================

btnChuva.addEventListener("click", function () {

  for(grupo in charts){

  if (btnChuva.checked == false){
    charts[grupo].data.datasets =  charts[grupo].data.datasets.filter(dataset => dataset.yAxisID != 'eixo_prec')

    charts[grupo].update()    
  }
  else if(btnChuva.checked){
    charts[grupo].data.datasets = datasetsChart[grupo]
    charts[grupo].update()   
    }
  }
  })

btnRev.addEventListener("click", function () {

  for(grupo in charts){

  if (btnRev.checked == false){
    charts[grupo].data.datasets =  charts[grupo].data.datasets.filter(dataset => !dataset.label.includes('RV'))

    charts[grupo].update()    
  }
  else if(btnRev.checked){
    charts[grupo].data.datasets = datasetsChart[grupo]
    charts[grupo].update()   
    }
  }
  })
    

GranularidadeSelect.addEventListener("click", function () {
     granularidade = GranularidadeSelect.value
})

inputDate.addEventListener("input", function () {
  dt_rodada = document.getElementById("inputDate").value;
  change_data_table()
  navRodadas = document.getElementById('nav-rodadas')
  navPreliminar = document.getElementById('nav-preliminar')
  navPreliminar.classList.add('active');
  navRodadas.classList.remove('active');


  getModelosDisponiveis(dt_rodada)

  })

$(window).on('load', function(){
if (inputDate.value == ""){
  btnChuva.checked = true
  btnRev.checked = true
  let today = new Date().toISOString().slice(0, 10)
  inputDate.value = today
  dt_rodada = document.getElementById("inputDate").value;
  create_tables()
  getModelosDisponiveis(dt_rodada) 
}
})

// ==============================================================================




// CONFIGURAÇÂO DOS GRAFICOS

// ==============================================================================
gradientChartOptionsConfiguration = {
    responsive: true,
    
    tooltips: {
      intersect: false,
      mode: 'nearest',
    },
    hover: {
        intersect: false,
        mode: 'nearest',
    },

    legend: {
      display:true,
      position: 'bottom',
      onClick: function(event, legendItem) {
        var chart = this.chart;
        chart.data.datasets.forEach(function(dataset, i) {
          if (dataset.label === legendItem.text) {
            dataset.hidden = !dataset.hidden;
          }
        })
        legendItem.hidden = !legendItem.hidden;
        chart.update();
      },
      labels: {
                generateLabels: function() {
                return labels.sort();
                }
      }
  },
scales:   {
      yAxes: [
                  {
                  id: 'eixo_ena',
                  type: 'linear',
                  position: 'left',
                  scaleLabel: {
                    display: true,
                    labelString: 'ENA',
                    fontSize: 20,
                  },
                  gridLines: {
                    drawOnChartArea: true
                  },

                  ticks: {
                      
                    display: true,
                    fontSize: 15          
                },
                },

                {
                  id: 'eixo_prec',
                  type: 'linear',
                  position: 'right',
                  scaleLabel: {
                    display: true,
                    labelString: 'Precipitação',
                    fontSize: 20
                  },
          
                  ticks: {
                      display: true,
                      beginAtZero: true,
                      reverse: true,
                      max: 100,
                      min: 0,
                      fontSize: 15          
                  },
                  gridLines: {
                    drawOnChartArea: false
                  },
                },
    ],
    xAxes: [{
              type: 'time',
              time: {
                  unit: 'week',
                  parser: "MMM DD, YYYY",
              },
              
  }]
  }
};
// ==============================================================================




// CRIAÇÂO DO GRAFICOS

// ==============================================================================
var charts = {}
var graphs = document.getElementById("graphs")
var keyOrder
var datasetsChart = {}

function create_charts(){
  if (graphs.children){
    while (graphs.firstChild) {
      graphs.removeChild(graphs.firstChild);
    }
  }
  if (granularidade == 'bacia'){
    keyOrder = Object.values(bacia_aux);
  }
  else{
    keyOrder = ['SUDESTE', 'SUL', 'NORDESTE', 'NORTE'];
  }
  for(let grup of keyOrder){

    var div_aux = document.createElement("div");
    div_aux.classList = "mt-5 card rounded"
    var canvas_aux = document.createElement("canvas");
    canvas_aux.id = "chart"+grup
    canvas_aux.setAttribute("height", 120);
    div_aux.appendChild(canvas_aux);
    graphs.appendChild(div_aux);

    var ctx = document.getElementById("chart"+grup).getContext("2d");
    ctx.canvas.width = $(window).width();
    ctx.canvas.height = $(window).height()*.8;
    
    gradientChartOptionsConfiguration['title'] = {
      display: true,
      text: grup,
      fontSize: 20
    }
    
    charts[grup] = new Chart(ctx ,{
      type: 'bar',
      options:gradientChartOptionsConfiguration,  

    })

    datasetsChart[grup] = []
    
  }
  
}
// ==============================================================================




// CHAMA TODAS AS REQUISIÇOES


// ==============================================================================

var dtInicioHistorico
var dataProximaRevisao
var inicioMesEletProxRev
var dtFimHistorico


let xhrMLT, xhrRevs, xhrRev, xhrAcomph,xhrChuva, xhrEna, xhrChuvaObs;;

btnRun.addEventListener("click", function () {

    labels = []
    create_charts()

    dtInicioHistorico = moment(dt_rodada).subtract(35, "days");
    dataProximaRevisao = dataProximaRev_moment(moment(dt_rodada))
    inicioMesEletProxRev = inicioMesEletrico_moment(dataProximaRevisao)
    dtFimHistorico = moment(dt_rodada).add(6*7 , 'days')

    if (granularidade == 'submercado'){
      getMLT()
      getRevs()
    }
    else{
      plotRev()
    }

    getAcomph()
    getChuva()
    getEna()
    getChuvaObs()
    
})
// ==============================================================================



// FUNÇÔES DE REQUISIÇÂO
// ==============================================================================

function getEna () {

  if (xhrEna) {
    xhrEna.abort();
  }



  document.getElementById('spin').classList = "fa fa-spinner fa-spin" 
  var dataTo = {"rodadas":Objeto_ids,"dt_rodada":dt_rodada,"granularidade":granularidade}
  
  dataToSend = JSON.stringify(dataTo);
   xhrEna =  $.ajax({
      contentType: "application/json; charset=utf-8",
      dataType: "json",
      method: "GET",
      url: "/middle/API/get/dados-rodadas-ena",
      data: {data:dataToSend},
      success: function(objeto) {
        objetoEna = objeto

        for (itens of objetoEna['PREVISAO_ENA']){

          ena = {}

          id = parseInt(itens['id_rodada'])
          modelo = itens['modelo']
          rodada = itens['horario_rodada']
          values = itens['valores']


          color = selectColor(rodada, modelo)


          if(labels.find(element => element.text == Objeto_ids[id]) == undefined){
            labels.push({text : Objeto_ids[id],fillStyle:color})
          }

          for (grup in values){

            if (granularidade == 'submercado'){
              grupo_renamed = submercado_aux
            }
            else{
              grupo_renamed = bacia_aux
            }

            if (grupo_renamed[grup] != undefined){

            ena[grup] = []

            for (data of Object.keys(values[grup])){

              date = moment(data +' 00:00','YYYY-MM-DD')
              ena[grup].push({t:new Date(date), y:values[grup][data]}) 
            }

            datasetConfig={
              type: "line",
              label: Objeto_ids[id],
              borderColor: color,
              backgroundColor: color,
              fill: false,
              yAxisID:'eixo_ena',
              data: ena[grup],
            }



            charts[grupo_renamed[grup]].data.datasets.push(datasetConfig)
            charts[grupo_renamed[grup]].update()
            datasetsChart[grupo_renamed[grup]].push(datasetConfig)
          }
        }
      }
      document.getElementById('spin').classList = ""

    },
    error: function() { 
      document.getElementById('spin').classList = ""
      exibirAlerta(`Não foi possivel mostrar informações de ENA!`, 'danger', 5000)
    }

  })
}


var objeto_acomph
var enaAcomph 
function getAcomph() {


  if (xhrAcomph) {
    xhrAcomph.abort();
  }
  
  dataInicioAcomph = dtInicioHistorico
  dataInicioAcomph = dataInicioAcomph.format("DD/MM/YYYY")
  
    xhrAcomph = $.ajax({
      contentType: "application/json; charset=utf-8",
      dataType: "json",
      method: "GET",
      url: `/middle/API/getEnaAcomph?dataInicial=${dataInicioAcomph}&divisao=${granularidade}`,

      success: function(objeto) {

        colorAcomph = getColor_byModelo('ACOMPH')

        objeto_acomph = objeto
        enaAcomph = {}

        labels.push({text:"ACOMPH",fillStyle: colorAcomph})

        if (granularidade == 'submercado'){
            grupo_renamed = submercado_aux
          }
          else{
            grupo_renamed = bacia_aux
          }

        for (let grup in objeto_acomph){ 
          if (grupo_renamed[grup] != undefined){
            
            enaAcomph[grup] = []

            for (data in objeto_acomph[grup]){

              date = moment(data +' 00:00','YYYY/MM/DD')
              enaAcomph[grup].push({t:new Date(date), y:objeto_acomph[grup][data]}) 
            }

            datasetConfig={
              type: "line",
              label: "ACOMPH",
              borderColor: 'rgba(6, 187, 199)',
              pointBackgroundColor: colorAcomph,
              backgroundColor: [],
              fill: false,
              data: enaAcomph[grup] ,
              yAxisID : 'eixo_ena',
              pointBackgroundColor : colorAcomph,
            }

            charts[grupo_renamed[grup]].data.datasets.push(datasetConfig)
            charts[grupo_renamed[grup]].update()
            datasetsChart[grupo_renamed[grup]].push(datasetConfig)
          }  
        }
      },
    error: function() { 
      exibirAlerta(`Não foi possivel mostrar informações do Acomph!`, 'danger', 5000)
      
    }

})
}


var objeto_chuva
var chuva
function getChuva() {

  if (xhrChuva) {
    xhrChuva.abort();
  }

  var dataTo = {"rodadas":Objeto_ids,"granularidade":granularidade,"dt_rodada": dt_rodada}
  
  dataToSend = JSON.stringify(dataTo);
    xhrChuva = $.ajax({
      contentType: "application/json; charset=utf-8",
      dataType: "json",
      method: "GET",
      url: "/middle/API/get/dados-rodadas-chuva",
      data: {data:dataToSend},
      success: function(objeto) {

        objeto_chuva = objeto

        for (itens of objeto_chuva['PREVISAO_CHUVA']){

          chuva = {}

          id = parseInt(itens['id_rodada'])
          modelo = itens['modelo']
          rodada = itens['horario_rodada']
          values = itens['valores']

          color = selectColor(rodada, modelo)


          if(labels.find(element => element.text == Objeto_ids[id]) == undefined){
            labels.push({text : Objeto_ids[id],fillStyle:color})
          }

          for (grup in values){

            if (granularidade == 'submercado'){
            grupo_renamed = submercado_aux
            }
            else{
              grupo_renamed = bacia_aux
            }

            if (grupo_renamed[grup] != undefined){

              chuva[grup] = []

              for (data of Object.keys(values[grup])){

                date = moment(data +' 00:00','YYYY-MM-DD').subtract(1,'days')
                chuva[grup].push({t:new Date(date), y:values[grup][data]}) 
              }

              datasetConfig={
                type: "bar",
                label: Objeto_ids[id],
                borderColor: color,
                backgroundColor: color,
                fill: false,
                yAxisID:'eixo_prec',
                data: chuva[grup],
              }

              charts[grupo_renamed[grup]].data.datasets.push(datasetConfig)
              charts[grupo_renamed[grup]].update()
              datasetsChart[grupo_renamed[grup]].push(datasetConfig)
            }
          }
        }
    },

    error: function() { 

      exibirAlerta(`Não foi possivel mostrar informações de Previsão de chuva!`, 'danger', 5000)

      
    }

})
}



var objeto_mlt
var enaMlt 

function getMLT() {

  if (xhrMLT) {
    xhrMLT.abort();
  }

    xhrMLT = $.ajax({
      contentType: "application/json; charset=utf-8",
      dataType: "json",
      method: "GET",
      url: "/middle/API/getmlt",
      success: function(objeto) {
        objeto_mlt = objeto

        labels.push({text:"MLT",fillStyle:colorMlt})
        enaMlt = {}

        let dataInicioMlt = new Date(dtInicioHistorico)
        let dataFimMlt = new Date(dtFimHistorico)

        for (submerc of Object.keys(objeto_mlt)){

          enaMlt[submerc] = []
          
          
          let dt_ref = new Date(dataInicioMlt)

           while (dt_ref <= dataFimMlt) {
              dt = new Date(dt_ref)
              let mesRef = dt_ref.getMonth()+1
              enaMlt[submerc].push({t:dt, y:objeto_mlt[submerc][mesRef].toFixed(2)})    
              dt_ref.setDate(dt_ref.getDate() + 1)
         }

          newDataset = {
              type: 'line',
              label: 'MLT',
              backgroundColor: colorMlt,
              borderColor: colorMlt,
              borderWidth: 1,
              fill: false,
              data: enaMlt[submerc],
              steppedLine: 'true',
              yAxisID: 'eixo_ena',
              pointRadius: 2,
          }

          charts[submerc].data.datasets.push(newDataset)
          charts[submerc].update()
          datasetsChart[submerc].push(newDataset)
          
        }
      },
    error: function() { 
      exibirAlerta(`Não foi possivel mostrar informações da MLT!`, 'danger', 5000)

      
    }

})
}



var objeto_merge
var chuvaMerge

function getChuvaObs(){

  if (xhrChuvaObs) {
    xhrChuvaObs.abort();
  }

  var dataInicioMerge = dtInicioHistorico.format("DD/MM/YYYY");
  xhrChuvaObs = $.ajax({
          url: "/middle/API/getchuvaObservada",
          type : 'get',
          data: {'data_inicial': dataInicioMerge,'granularidade':granularidade}
      })
  .done(function(resposta){
    objeto_merge = resposta
    chuvaMerge = {}
    labels.push({text : "MERGE",fillStyle:colorMerge})

      for(grup in objeto_merge){


        if (granularidade == 'submercado'){
            grupo_renamed = submercado_aux
        }
        else{
            grupo_renamed = bacia_aux
        }   


        if (grupo_renamed[grup] != undefined) {
                        
          chuvaMerge[grup] = []

          for (data of Object.keys(objeto_merge[grup])){

            chuvaMerge[grup].push({t:new Date(data + ' 00:00'), y:objeto_merge[grup][data].toFixed(2)}) 
          }
            newDataset = {
              type: 'bar',
                label: 'MERGE',
                backgroundColor: colorMerge,
                borderColor: colorMerge,
                borderWidth: 1,
                fill: false,
                yAxisID: 'eixo_prec',
                data: chuvaMerge[grup],
            }
            
            grupo_renamed[grup] = grupo_renamed[grup].toUpperCase()
            charts[grupo_renamed[grup]].data.datasets.push(newDataset)
            charts[grupo_renamed[grup]].update()
            datasetsChart[grupo_renamed[grup]].push(newDataset)
        }
      }
    
  })

}


var objeto_rev
var borderDashs = [[], [1,1], [1,2], [3,6], [10,5]]

function getRevs (){


  if (xhrRevs) {
    xhrRevs.abort();
  }

  xhrRevs = $.ajax({
      url:"/middle/API/getallrevs",
      type : 'get',
  })
  .done(function(resposta){
    objeto_rev = resposta
    rev = {}

    
     for (rv in objeto_rev){

         for (let submerc in objeto_rev[rv]){
         
             if (rev[submerc] == undefined){
                 rev[submerc] = {}
             }

             if (rev[submerc][rv] == undefined){
                 rev[submerc][rv] = []
             }

             for (data in objeto_rev[rv][submerc]){
                 dt = new Date(data)
                 rev[submerc][rv].push({t:dt, y:objeto_rev[rv][submerc][data].toFixed(2)})
             }


             newDataset = {
                 type: 'line',
                 label: 'RV'+rv,
                 backgroundColor: colorRV,
                 borderColor: colorRV,
                 fill: false,
                 data: rev[submerc][rv],
                 borderDash: borderDashs[Object.keys(objeto_rev).length-rv-1],  // ordem reversa, sempre a ultima rev sera uma linha continua
                 steppedLine: 'true',
                 yAxisID: 'eixo_ena',
                 pointRadius: 2,
                 borderWidth: 1
             }

             charts[submerc].data.datasets.push(newDataset);
             charts[submerc].update();
             datasetsChart[submerc].push(newDataset)
         }
     }
    })
  }


function plotRev(){

  if (xhrRev) {
    xhrRev.abort();
  }


  dataInicioAcomph = dtInicioHistorico.format("DD/MM/YYYY");
  xhrRev = $.ajax({
       url: "/middle/API/getrevbacias",
       type : 'get',
       data:  {'dataInicial':dataInicioAcomph}
  })
  .done(function(resposta){
    objeto_rev = resposta


    var rev = objeto_rev['revisao']

    labels.push({text:'RV'+rev,fillStyle:colorRV})
    enaRev = {}

    labels.push({text:"MLT",fillStyle:colorMlt})
    enaMlt = {} 

    for (let bac in bacia_aux){
      
      enaRev[bac] = []
      enaMlt[bac] = []


      for (data in objeto_rev['ena'][bac]){

        enaRev[bac].push({t:new Date(data + ' 00:00'), y:objeto_rev['ena'][bac][data].toFixed(2)})

      }

        newDataset = {
          type: 'line',
          label: 'RV'+rev,
          backgroundColor: colorRV,
          borderColor: colorRV,
          borderWidth: 1,
          borderDash: borderDashs[rev],
          fill: false,
          yAxisID: 'eixo_ena',
          steppedLine: 'true',
          pointRadius: 2,
          data: enaRev[bac],
        }

        charts[bacia_aux[bac]].data.datasets.push(newDataset)
        charts[bacia_aux[bac]].update()
        datasetsChart[bacia_aux[bac]].push(newDataset)
      

      // GERANDO MLT DAS BACIAS 

        for (data in objeto_rev['mlt'][bac]){

        enaMlt[bac].push({t:new Date(data + ' 00:00'), y:objeto_rev['mlt'][bac][data].toFixed(2)})

      }

          newDataset = {
            label: 'MLT',
            type: 'line',
            backgroundColor: colorMlt,
            borderColor: colorMlt,
            borderWidth: 1,
            fill: false,
            data: enaMlt[bac],
            steppedLine: 'true',
            yAxisID: 'eixo_ena',
            pointRadius: 2,
        }
        charts[bacia_aux[bac]].data.datasets.push(newDataset)
        charts[bacia_aux[bac]].update()
        datasetsChart[bacia_aux[bac]].push(newDataset)
      }
    })
}