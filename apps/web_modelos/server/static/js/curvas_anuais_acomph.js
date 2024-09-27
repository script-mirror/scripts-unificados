var submerc = {'SE':'SUDESTE', 'S': 'SUL','NE':'NORDESTE','N':'NORTE'}


function customTooltipHandler(tooltipModel) {
  if (!tooltipModel || !tooltipModel.body) return;

  // Agrupa os pontos de dados por data
  const dataGroups = {};
  tooltipModel.dataPoints.forEach(point => {
    const label = point.label;
    const value = point.value;
    if (!dataGroups[label]) {
      dataGroups[label] = [];
    }
    dataGroups[label].push(value);
  });

  // Obtém o rótulo da data e os valores para o ponto específico do tooltip
  const label = tooltipModel.body[0].lines[0].split(": ")[1]; // Extrai a data do rótulo
  const values = dataGroups[label].sort((a, b) => b - a);

  // Cria o novo rótulo do tooltip com os valores ranqueados
  let tooltipLabel = `Data: ${label}\n`;
  values.forEach((value, index) => {
    tooltipLabel += `${index + 1}º valor: ${value}\n`;
  });

  // Define o novo rótulo do tooltip
  tooltipModel.body[0].lines = [{ line: tooltipLabel }];
}


gradientChartOptionsConfiguration = {
    responsive: true,
    legend: {
      display:true,
      position: 'bottom',
  },

    hover: {
        intersect: false,
        mode: 'index',
    },
  tooltips: {
    intersect: false,
    mode: 'index',
    callbacks: {
      title: function (tooltipItems) {
        const label = moment(tooltipItems[0].xLabel,'MMM DD, YYYY, h:mm:ss a').format('DD MMM');
        return `Data: ${label}`;
      },
      label: function (tooltipItem, data) {

        var label = tooltipItem.xLabel;
        const datasetIndex = tooltipItem.datasetIndex;
        const values = data.datasets.flatMap(dataset =>dataset.data.filter(point => `${point.t}` === `${new Date(`${label}`)}`).map(point => point.y));
        var label_name = data.datasets[datasetIndex].label
        const valor = data.datasets[datasetIndex].data.filter(point => `${point.t}` === `${new Date(`${label}`)}`).map(point => point.y);
        const sortedValues = values.sort((a, b) => b - a);
        sortedValues.forEach((value, index) => {
          if(`${value}` === valor[0]){
          tooltipLabel = `${index + 1}º ${label_name}: ${value}`;
          }
        });

        return tooltipLabel;
      }
    },
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
    ],
    xAxes: [{
              type: 'time',
              time: {
                unit: 'month',
                displayFormats: {
                    month: 'MMM'
                }
              },
              
  }]
  }
};




var charts = {}
var chartEna = {}
var graphs = document.getElementById("graphs")
var keyOrder

function create_chart(submercado){

  for (submercado of Object.values(submerc)){

      var div_aux = document.createElement("div");
      div_aux.classList = "mt-5 card rounded"
      var canvas_aux = document.createElement("canvas");
      canvas_aux.id = "chart"+submercado
      canvas_aux.setAttribute("height", 120);
      div_aux.appendChild(canvas_aux);
      graphs.appendChild(div_aux);

      var ctx = document.getElementById("chart"+submercado).getContext("2d");
      ctx.canvas.width = $(window).width();
      ctx.canvas.height = $(window).height()*.8;
      
      gradientChartOptionsConfiguration['title'] = {
        display: true,
        text: submercado,
        fontSize: 20
      }
      
      charts[submercado] = new Chart(ctx ,{
        type: 'bar',
        options:gradientChartOptionsConfiguration,  

      })
      chartEna[submercado] = []
    }
  }

var objeto_acomph
var newDataset
function plotAcomph(anos) {
      modalLoading.style.display = "block";
      dataInicioAcomph = Math.min(...anos)
      

      $.ajax({
        contentType: "application/json; charset=utf-8",
        dataType: "json",
        method: "GET",
        url: `/middle/API/getEnaAcomph?dataInicial=01/01/${dataInicioAcomph}&divisao=submercado&flagNoCache=0`,
  
        success: function(objeto) {

            objeto_acomph = objeto
            console.log(objeto_acomph)
            
            for (let submercado of Object.keys(objeto_acomph).reverse()){ 
                
                for(ano of anos){
                  let dadosPorAno = {}
                  dadosPorAno[ano] = [];
                  

                  if (getColor_byYear(ano) == undefined) {
                      color = ramdomColor();
                    }
                  else{
                    color = getColor_byYear(ano)
                  }
                  

                  Object.keys(objeto_acomph[submercado]).filter((data) => data.slice(0,4) === ano).forEach((chave) => {
                    if(!(chave.slice(5) === "02/29")){ 
                    dadosPorAno[ano].push({t:new Date("2014/"+chave.slice(5)), y:objeto_acomph[submercado][chave].toFixed(2)})
                  }});

                newDataset = {
                    label: ano,
                    type: 'line',
                    backgroundColor: color[0],
                    borderColor: color[0],
                    fill: false,
                    data: dadosPorAno[ano],
                    yAxisID: 'eixo_ena',
                    pointRadius: false,
                    borderWidth: 3,
                    tension: 0.1
                }
                if(ano == moment().format("YYYY")){
                  newDataset['borderWidth'] = 8,
                  newDataset['borderDash'] = [0,0]
                }

                 charts[submerc[submercado]].data.datasets.push(newDataset);
                 chartEna[submerc[submercado]].push(newDataset);
                 charts[submerc[submercado]].update();
             }



            }
            modalLoading.style.display = "none";
          
        },
      error: function() { 
        alert("Algo deu errado: Não foi possivel plotar o acomph")
        modalLoading.style.display = "none";
      }
  
  })
  }

var objetoMlt 
function plotMlt(){
  $.ajax({
    contentType: "application/json; charset=utf-8",
    dataType: "json",
    method: "GET",
    url: "/middle/API/getmlt",
    success: function(objeto) {

      objetoMlt = objeto

      let dataInicioMlt = new Date("2014/01/01")
      let dataFimMlt = new Date("2014/12/31")

      mlt = {}
      for (submercado of Object.keys(objetoMlt)){

          
          mlt_aux = objetoMlt[submercado]

            if (mlt[submercado] == undefined){
                mlt[submercado] = []
            }

            let dt_ref = new Date(dataInicioMlt)
            while (dt_ref <= dataFimMlt) {
              dt = new Date(dt_ref)
                let mesRef = dt_ref.getMonth()+1
                mlt[submercado].push({t:dt, y:mlt_aux[mesRef].toFixed(2)})    
                dt_ref.setDate(dt_ref.getDate() + 1)
            }

            newDataset = {
                label: 'MLT',
                type: 'line',
                backgroundColor: 'rgba(14,0,0)',
                borderColor: 'rgba(14,0,0)',
                fill: false,
                data: mlt[submercado],
                steppedLine: 'true',
                yAxisID: 'eixo_ena',
                pointRadius: false,
                borderDash: [5, 5],
                borderWidth: 2,
                tension: 0.1
            }

            charts[submercado].data.datasets.push(newDataset);
            chartEna[submercado].push(newDataset);
            charts[submercado].update();
        }

    },
    error: function() { 
      alert("Algo deu errado: Não foi possivel plotar o acomph")
    }

  })
}

  function iniciarListeners(){
    $('#sidebarCollapse').on('click', function () {
        $('#sidebar').toggleClass('active');
    });


    $('[data-toggle="tooltip"]').tooltip()
}

const modalLoading = document.getElementById("modalLoading");
var botoes

$(window).on('load', function(){
    createSelect()
    var dtInicioHistorico = moment("2014",'YYYY').format('YYYY');
    var dtFinalHistorico = moment().format('YYYY')
    var dt_aux = dtInicioHistorico
    while(dt_aux <= dtFinalHistorico){
      anos.push(dt_aux)
      dt_aux = parseInt(dt_aux) + 1
      dt_aux = dt_aux.toString()
    }

    plotAcomph(anos)
    iniciarListeners()
    create_chart()
    plotMlt()
    createBtnAno()


    // dt_ontem = moment().subtract(1,"days").format("YYYY/MM/DD")
    // datasKeys = Object.keys(objeto_acomph['N'])


    botoes = document.querySelectorAll('.btn-anos');

    botoes.forEach(b => {
      if (anos.includes(b.textContent)) {
          b.classList.remove('btn-light')
          b.classList.add('btn-success');
          anos_aux.push(b.textContent)
      }
  });
    
})



btnAnosList = document.getElementById('Anos')

btnAnosList.addEventListener("click", function () {
  anos = []
  anos_aux = []

  var dt_inicial = btnAnosList.value
  var dt_final = parseInt(moment().format("YYYY"))
  
  var dt_aux = dt_inicial
  while(dt_aux<=dt_final){
    anos.push(dt_aux)
    dt_aux = parseInt(dt_aux) + 1
    dt_aux = dt_aux.toString()
  }

  removeAllSuccess()

  botoes.forEach(b => {
      if (anos.includes(b.textContent)) {
          b.classList.remove('btn-light')
          b.classList.add('btn-success');
          anos_aux.push(b.textContent)
      }
    })

})



btnAtualizar.addEventListener("click", function () {
      
  for (let submercado of Object.values(submerc)){

    charts[submercado].data.datasets = chartEna[submercado].filter((data) => anos.includes(data.label))
    charts[submercado].data.datasets.push(chartEna[submercado].filter((data) => data.label==='MLT')[0])

    charts[submercado].update()
    
  }
    
  })





const btnListDiv = document.getElementById("btnListAnos")
var selectYear = document.getElementById('Anos')
function createSelect(){

  dt_inicial = 2014// É o ultimo ano do histórico no banco de dados
  dt_final = parseInt(moment().format("YYYY"))
  
  var dt_aux = dt_final

  var div_aux = document.createElement("div");
  

  while(dt_aux >= dt_inicial){
    let option = document.createElement('option');
    option.value = dt_aux
    option.textContent = dt_aux;
    selectYear.appendChild(option);

    div_aux.appendChild(selectYear)
    dt_aux = dt_aux-1
  }
  btnListDiv.appendChild(div_aux)

}


var btnDiv = document.getElementById("btnAnos")

function createBtnAno(){

  dt_inicial = 2014
  dt_final = parseInt(moment().format("YYYY"))

  var dt_aux = dt_final
  while(dt_aux >= dt_inicial){

    var btn_aux = document.createElement("button");
    btn_aux.type = 'button';
    btn_aux.classList.add('btn', 'btn-light', 'btn-anos','mb-1','ml-1');
    btn_aux.textContent  = dt_aux

    btnDiv.appendChild(btn_aux)
    dt_aux = dt_aux-1
  }



}

var anos=[]
var anos_aux = []
btnDiv.addEventListener('click', (event) => {
  if (event.target.classList.contains('btn-anos')) {

    if(event.target.classList.contains('btn-light')){
    event.target.classList.remove('btn-light')
    event.target.classList.add('btn-success');
    anos_aux.push(event.target.textContent)
    }

    else if(event.target.classList.contains('btn-success')){
      event.target.classList.remove('btn-success')
      event.target.classList.add('btn-light');
      anos_aux = anos.filter(item => item !== event.target.textContent)
    }

    anos = anos_aux
  }
});


function removeAllSuccess(){


// Adiciona um ouvinte de evento de clique para cada botão

botoes.forEach(b => b.classList.remove('btn-success'));
botoes.forEach(b => b.classList.add('btn-light'));


}