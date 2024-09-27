var submerc = {'SE':'SUDESTE', 'S': 'SUL','NE':'NORDESTE','N':'NORTE', 'SIN':'SIN'}

function fechamento_mes_ano(dados) {

  const fechamentos = {};
  apenas_anos= anos.filter(ano=>  !['max','min'].includes(ano))
  
  for (ano of apenas_anos){

    datas = Object.keys(dados).filter(data=> data.slice(0,4) == ano)
    meses = datas.map(data => new Date(data+' 00:').getMonth() + 1);
    mesesUnicos = [...new Set(meses)];


    for (const mes of mesesUnicos) {

      if (!fechamentos[ano]) {
        fechamentos[ano] = {};
      }

      if(mes == 1){
        datas_do_ano_mes = datas.filter(data=> parseInt(data.slice(5,7)) == mes)
        ultima_data = datas_do_ano_mes[0]
      }

      else{
        datas_do_ano_mes = datas.filter(data=> parseInt(data.slice(5,7)) == mes-1)
        ultima_data = datas_do_ano_mes[datas_do_ano_mes.length-1]
      }

      var valor = dados[ultima_data];
      fechamentos[ano][mes] = valor;

    }
  }

  return fechamentos;
}


function valores_max_min(fechamentoPorMesPorAno) {
  const maximos = {};
    const minimos= {}
  apenas_anos= anos.filter(ano=> ano>=2013 && !['max','min'].includes(ano))

  for (let ano of apenas_anos) {
    ano = parseInt(ano)
    if(ano != moment().format("YYYY")){
                  
    for (const mes in fechamentoPorMesPorAno[ano]) {
      const valor = fechamentoPorMesPorAno[ano][mes];

      if (!maximos[mes]) {
        
        maximos[mes] = valor 
      } else if (valor > maximos[mes]) {
        maximos[mes] = valor 
      }
        if (!minimos[mes]) {
        minimos[mes] =  valor 
        
      } else if (valor < minimos[mes]) {
        minimos[mes] = valor 
      }
    }
  }
  }
    max_min = {}
    max_min['max'] = maximos
    max_min['min'] = minimos
  return max_min;
}




gradientChartOptionsConfiguration = {
    responsive: true,
    legend: {
      display:true,
      position: 'bottom',
  },

    animation: {
        duration: 1,
        onComplete: function () {
            if (btn_max_min == true){
 
            var chartInstance = this.chart,
                ctx = chartInstance.ctx;
            ctx.font = Chart.helpers.fontString(Chart.defaults.global.defaultFontSize, Chart.defaults.global.defaultFontStyle, Chart.defaults.global.defaultFontFamily);
            ctx.textAlign = 'center';
            ctx.textBaseline = 'bottom';
            ctx.fillStyle = 'black';

            this.data.datasets.forEach(function (dataset, i) {
                var meta = chartInstance.controller.getDatasetMeta(i);
                meta.data.forEach(function (bar, index) {
                    var data = dataset.data[index];
                    anos_plotar_dados = anos.filter(ano=> ano >= moment().format("YYYY") -1)
                    if (anos_plotar_dados.includes(dataset.label)){
                    ctx.fillText(parseFloat(data.y).toFixed() +'%', bar._model.x + 5, bar._model.y-5);
                  }
                });
            }); 

           } 
        }
    },
  
  tooltips: { 
    mode: 'x',
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
                    labelString: 'EAR (%)',
                    fontSize: 20,
                  },
                  gridLines: {
                    drawOnChartArea: true
                  },


                  ticks: {
                      
                    display: true,
                    fontSize: 15,
                    beginAtZero: true,
                    max: 100,
                    min: 0,         
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
var chartEnaMensal = {}
var graphs = document.getElementById("graphs")

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
      chartEnaMensal[submercado] = []
    }
  }


var newDataset
function plotAcomph(anos) {
      modalLoading.style.display = "block";
      dataInicioAcomph = Math.min(...anos)
      

      $.ajax({
        contentType: "application/json; charset=utf-8",
        dataType: "json",
        method: "GET",
        url: `/middle/API/GET/earm_submercado?dataInicial=01/01/${dataInicioAcomph}`,
  
        success: function(objeto) {

            objeto_armazenamnto = objeto

            anos.push('max','min')

            
            for (let submercado of Object.keys(objeto_armazenamnto).reverse()){ 
                
                fechamentos_mes_ano = fechamento_mes_ano(objeto_armazenamnto[submercado])
                max_min = valores_max_min(fechamentos_mes_ano)
                
                for(ano of anos){

                  if (['max','min'].includes(ano)){
                    teste = max_min

                  }
                  else(
                    teste = fechamentos_mes_ano
                    )

                  let dadosDiariosPorAno = {}
                  dadosDiariosPorAno[ano] = [];

                  let dadosMensaisPorAno = {}
                  dadosMensaisPorAno[ano] = [];
                  

                  if (getColor_byYear(ano) == undefined) {
                      color = ramdomColor();
                    }
                  else{
                    color = getColor_byYear(ano)
                  }

                  

                  Object.keys(objeto_armazenamnto[submercado]).filter((data) => data.slice(0,4) === ano).forEach((chave) => {

                    if(!(chave.slice(5) === "02/29")){ 
                    dadosDiariosPorAno[ano].push({t:new Date("2013/"+chave.slice(5)), y:objeto_armazenamnto[submercado][chave].toFixed(2)})
                  }
                  });
                    
                  Object.keys(teste).filter((data) => data === ano).forEach((chave) => {

                    for (mes in teste[chave]){
                    dadosMensaisPorAno[ano].push({t:new Date("2013/"+mes+"/01 00:"), y:teste[chave][mes]})
                  }
                });

                newDatasetDiario = {
                    label: ano,
                    type: 'line',
                    backgroundColor: color[0],
                    borderColor: color[0],
                    fill: false,
                    data: dadosDiariosPorAno[ano],
                    yAxisID: 'eixo_ena',
                    pointRadius: false,
                    borderWidth: 3,
                    tension: 0.1

                }

                newDatasetMensal = {
                    label: ano,
                    type: 'line',
                    backgroundColor: color[0],
                    borderColor: color[0],
                    fill: false,
                    data: dadosMensaisPorAno[ano],
                    yAxisID: 'eixo_ena',
                    pointRadius: false,
                    borderWidth: 3,
                    tension: 0.1

                }

                if(ano == moment().format("YYYY")){
                  newDatasetDiario['borderWidth'] = 8
                  newDatasetMensal['borderWidth'] = 8
                  
                }
                if(ano == 'min'){
                  newDatasetMensal['fill'] = '-1'

                }

                 charts[submercado].data.datasets.push(newDatasetDiario);
                 
                 chartEna[submercado].push(newDatasetDiario);
                 chartEnaMensal[submercado].push(newDatasetMensal);

                 charts[submercado].update();
             }
            }
            modalLoading.style.display = "none";
          
        },
      error: function() { 
        alert("Algo deu errado: Não foi possivel plotar a EAR")
        modalLoading.style.display = "none";
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
    var dtInicioHistorico = moment("2005",'YYYY').format('YYYY');
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
    createBtnAno()




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


var btnMaxMin = document.getElementById("btnMaxMin")
var btn_max_min = false
btnMaxMin.addEventListener("click", (event)=> {

  if (btn_max_min == true){
    btn_max_min = false
    btnMaxMin.innerText = 'Dados Diários'

  }
  else{
    btn_max_min = true
    btnMaxMin.innerText = 'Dados de Fechamento'
  }

  anos.push('max','min')
      
  for (let submercado of Object.values(submerc)){

    if (btn_max_min == true){
      charts[submercado].data.datasets = chartEnaMensal[submercado].filter((data) => anos.includes(data.label))

    }
    else{
      charts[submercado].data.datasets = chartEna[submercado].filter((data) => anos.includes(data.label))
    }
    
    charts[submercado].update()
    
  }

})


btnAtualizar.addEventListener("click", function () {

  anos.push('max','min')
      
  for (let submercado of Object.values(submerc)){

    if (btn_max_min == true){
      charts[submercado].data.datasets = chartEnaMensal[submercado].filter((data) => anos.includes(data.label))

    }
    else{
      charts[submercado].data.datasets = chartEna[submercado].filter((data) => anos.includes(data.label))
    }
    
    charts[submercado].update()
    
  }
    
  })





const btnListDiv = document.getElementById("btnListAnos")
var selectYear = document.getElementById('Anos')
function createSelect(){

  dt_inicial = 2005// É o ultimo ano do histórico no banco de dados
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

  dt_inicial = 2005
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