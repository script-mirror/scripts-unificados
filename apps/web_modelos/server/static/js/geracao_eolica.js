




var timeFormat = 'DD/MM/YYYY';
moment.locale('pt-br');
data = moment()





numSemanas = 0

dt_verificado = document.getElementById("semana-verificado")

dt_deck = document.getElementById("data-deck")

dia_semana = document.getElementById("dia-semana")


dt_verificado.value = data.format("YYYY-MM-DD")
dt_deck.value = data.add(-1, 'days').format("YYYY-MM-DD")


submercados = ['NORDESTE','SUL']


var options = {
                    title: {
                      display: true,
                    
                    },
                    legend: {
                      display: true,
                      position: 'bottom'
                    },
                    hover: {
                        intersect: false,
                        mode: 'nearest',
                    },
                    
                    tooltips: {
                        intersect: false,
                        mode: 'nearest',
                        callbacks: {
                          title: function (tooltipItems) {
                            const label = moment(tooltipItems[0].xLabel,'MMM DD, YYYY, h:mm:ss a').format('dddd HH:mm');
                            return `Data: ${label}`;
                          },
                        }
                    },

                    scales: {
                        xAxes: [{
                            type: 'time',

                            time: {

                                
                                unit: 'day',
                                displayFormats: {
                                    day: 'dddd HH:mm',
                                },

                            },

                            scaleLabel: {
                                display: true,
                                labelString: 'Data'
                            }
                        }],
                        yAxes: [
                        {
                            id: 'eixo_geracao',
                            type: 'linear',
                            position: 'left',
                            scaleLabel: {
                                display: true,
                                labelString: 'MW'
                            }
                        }
                        , 

                        ]
                    },
                    animation: {
                        duration: 0
                    },
                    maintainAspectRatio: false,
                    responsive: true
                }


function iniciar_listeners(){
    // Iniciar listeners
    $('#sidebarCollapse').on('click', function () {
        $('#sidebar').toggleClass('active');
    });

    $('#btn_atualizar').on('click', function () {
        data = moment(dt_verificado.value, "YYYY-MM-DD")
        diasPlot = 7
        // limpar_graficos()

        // get_previsao_eolica_decomp(data)
        // get_previsao_eolica_dessem(data,diasPlot);
        get_verificada_eolica(data)
        
    } );

    $('#btn_add_previsao_dessem').on('click', function () {
        data_deck = moment(dt_deck.value, "YYYY-MM-DD")

        data_semana = moment(dia_semana.value, "YYYY-MM-DD")

        // diasPlot = 7
        get_previsao_eolica_dessem(data_deck,data_semana)
        get_mlt_eolica_nw(data_semana)
        // get_verificada_eolica(data)
        numSemanas = numSemanas+1
    } );

    $('#btn_limpar').on('click', function () {
        limpar_graficos()
    } );
    
  }


$(window).on('load', function(){
        iniciar_listeners()
        create_chart()

        inicializa_label()

})


teste = {}

function inicializa_label(){

    dt_ini = moment('sábado 00:00', 'dddd HH:mm')
    teste[dt_ini.format('dddd HH:mm')] =  dt_ini.add(-7, 'days')
    
    dt_aux = moment('sábado 00:00', 'dddd HH:mm').add(1,'hours').format('dddd HH:mm')

    while(dt_aux != 'sábado 00:00'){ 

        dt_to_format = moment(dt_aux,'dddd HH:mm')

        if (dt_aux.includes('sábado')){ 

            dt_to_format = moment(dt_aux,'dddd HH:mm').add(-7,'days')
        }
        teste[dt_aux] =  dt_to_format

        dt_aux = moment(dt_aux,'dddd HH:mm').add(1,'hours').format('dddd HH:mm')
    }
}


charts={}
function create_chart(){

    for (sub of submercados){
            
        var ctx = document.getElementById("chart_"+sub).getContext("2d");
        ctx.canvas.width = $(window).width();
        ctx.canvas.height = $(window).height()*.8;
        options['title']['text'] = sub
        charts[sub] = new Chart(ctx, {
            // type: 'line',
            options : options

        })
    }
                

}

function limpar_graficos(){
    numSemanas = 0
    for (sub of submercados){
        charts[sub].data.datasets = [];
        charts[sub].update();
    }

  }



  
  var response_eol_nw
  function get_mlt_eolica_nw(data_semana){
    $.ajax({
        url: "/middle/API/get/mlt-eolica",
        type : 'GET',
        data: {
            'dtInicial': data_semana.format("YYYY-MM-DD"),
            'numSemanas': 1,
            "dataType":'diario'
        },
        success: function(resposta){

            response_eol_nw = resposta

            for(submercado in response_eol_nw){ 

                mlt_eol = []

                //diaria
                for (date in response_eol_nw[submercado]['vl_geracao_eol']){

                        dt_to_format = moment(date, 'YYYY-MM-DD HH:mm:ss')
                        dt_formated = dt_to_format.format('dddd HH:mm')

                        mlt_eol.push({t:teste[dt_formated], y:response_eol_nw[submercado]['vl_geracao_eol'][date]})
                }

                color_base = getColorSemana(numSemanas)

                newDataset = {
                            label: `NW -> Semana:(${data_semana.format("DD/MM")})`,
                            type: 'line',
                            backgroundColor: color_base,
                            borderColor: color_base,
                            fill: false,
                            data: mlt_eol,
                            pointRadius: 0.5,
                            steppedLine: 'true',
                            // borderDash: [10,5],
                            // hidden: true,
                            borderWidth: 1,
                            tension: 0.1
                        }
                charts[submercado].data.datasets.push(newDataset);

                charts[submercado].update();

            }

            
        },
        error: function() { 
            alert(`Não foi possivel plotar o previsao do dessem para a semana do dia (${data.format("DD/MM")})`)
      
        }
    })
}





var response_ds
function get_previsao_eolica_dessem(data_deck,data_semana){
    $.ajax({
        url: "/middle/API/get/geracao_eolica_prevista_ds",
        type : 'GET',
        data: {
            'data_referente': data_semana.format("YYYY-MM-DD"),
            'data_deck': data_deck.format("YYYY-MM-DD")
        },
        success: function(resposta){

            response_ds = resposta

            medias = response_ds['media']
            previsao = response_ds['prev']

            for(submercado in previsao){ 

                media_horaria_prev_ds = []
                media_diaria_prev_ds = []
                // media_semanal_verif = []


                //horaria
                for (date in medias[submercado]['horaria']){

                 
                        dt_to_format = moment(date, 'YYYY-MM-DD HH:mm:ss')
                        dt_formated = dt_to_format.format('dddd HH:mm')

                        media_horaria_prev_ds.push({t:teste[dt_formated], y:medias[submercado]['horaria'][date]})

                }

                //diaria
                for (date in medias[submercado]['diaria']){


                        dt_to_format = moment(date, 'YYYY-MM-DD HH:mm:ss')
                        dt_formated = dt_to_format.format('dddd HH:mm')

                        media_diaria_prev_ds.push({t:teste[dt_formated], y:medias[submercado]['diaria'][date]})

                }

                color_base = getColorSemana(numSemanas)

                newDataset = {
                            label: `DS-h -> Deck:(${data_deck.format("DD/MM")}) Semana:(${data_semana.format("DD/MM")})`,
                            type: 'line',
                            backgroundColor: color_base,
                            borderColor: color_base,
                            fill: false,
                            data: media_horaria_prev_ds,
                            // steppedLine: 'true',
                            // borderDash: [10,5],
                            // hidden: true,
                            borderWidth: 1,
                            pointRadius: 0.5,
                            tension: 0.1
                        }
                charts[submercado].data.datasets.push(newDataset);

                newDataset = {
                            label: `DS-d -> Deck:(${data_deck.format("DD/MM")}) Semana:(${data_semana.format("DD/MM")})`,
                            type: 'line',
                            backgroundColor: color_base,
                            borderColor: color_base,
                            fill: false,
                            data: media_diaria_prev_ds,
                            steppedLine: 'true',
                            borderDash: [10,5],
                            hidden: true,
                            borderWidth: 1,
                            tension: 0.1
                        }
                charts[submercado].data.datasets.push(newDataset);

                // newDataset = {
                //             label: `DS-s (${data.format("DD/MM")})`,
                //             type: 'line',
                //             backgroundColor: cor_nova_curva_dessem,
                //             borderColor: cor_nova_curva_dessem,
                //             fill: false,
                //             data: eolica_semanal_ds,
                //             steppedLine: 'true',
                //             borderDash: [10,5],
                //             // hidden: true,
                //             borderWidth: 1,
                //             pointRadius: 0.5,
                //         }
                // charts[sub].data.datasets.push(newDataset);

                charts[submercado].update();

            }

            
        },
        error: function() { 
            alert(`Não foi possivel plotar o previsao do dessem para a semana do dia (${data.format("DD/MM")})`)
      
        }
    })
}


var response_ver
function get_verificada_eolica(data){
    $.ajax({
        url: "/middle/API/get/geracao_eolica_verificada",
        type : 'GET',
        data: {
            'data_referente': data.format("YYYY-MM-DD"),
        },
        success: function(resposta){
            response_ver = resposta

            medias = response_ver['media']

            for(submercado in medias){ 

                media_horaria_verif = []
                media_diaria_verif = []
                // media_semanal_verif = []

                ultimaData = Object.keys(response_ver['media'][submercado]['horaria']).pop()
                // ultimaData_sul = Object.keys(response_ver['media']['SUL']['horaria']).pop()

                if (moment(ultimaData,'YYYY-MM-DD HH:mm:ss').format('dddd') == 'sábado'){

                    delete medias[submercado]['horaria'][ultimaData]
                    delete medias[submercado]['diaria'][ultimaData]
                }


                //horaria
                for (date in medias[submercado]['horaria']){

                        dt_to_format = moment(date, 'YYYY-MM-DD HH:mm')
                        dt_formated = dt_to_format.format('dddd HH:mm')

                        media_horaria_verif.push({t:teste[dt_formated], y:medias[submercado]['horaria'][date]})

                }

                //diaria
                for (date in medias[submercado]['diaria']){

                    dt_to_format = moment(date, 'YYYY-MM-DD HH:mm')
                    dt_formated = dt_to_format.format('dddd HH:mm')

                    media_diaria_verif.push({t:teste[dt_formated], y:medias[submercado]['diaria'][date]})

                }

                color_base = getColorSemana(numSemanas)

                // Verificada
                newDataset = {
                            label: `Ver-h (${data.format("DD/MM")})`,
                            type: 'line',
                            backgroundColor: color_base,
                            borderColor: color_base,
                            fill: false,
                            data: media_horaria_verif,
                            // steppedLine: 'true',
                            pointRadius: 0.5,
                            borderWidth: 2,
                            tension: 0.1
                        }
                charts[submercado].data.datasets.push(newDataset);

                newDataset = {
                            label: `Ver-d (${data.format("DD/MM")})`,
                            type: 'line',
                            backgroundColor: color_base,
                            borderColor: color_base,
                            fill: false,
                            data: media_diaria_verif,
                            steppedLine: 'true',
                            borderDash: [10,5],
                            hidden: true,
                            borderWidth: 1,
                            tension: 0.1
                        }
                charts[submercado].data.datasets.push(newDataset);

                charts[submercado].update();


                }
        },
        error: function() { 
            alert(`Não foi possivel plotar o verificado para a semana do dia (${data.format("DD/MM")})`)
      
        }
    })
}

function comutar_coff(){
    var checkbox_coff = document.getElementById('checkbox_coff');

    let dataRef = $('#semana-verificado').val()
    let ultimoSabado = moment(dataRef).day(-1);
    let proximoSabado = ultimoSabado.clone().add(7,'days');

    if(checkbox_coff.checked) {
      get_constrained_off(ultimoSabado,proximoSabado)
    } else {
        for (sub of submercados){
            charts[sub].data.datasets = charts[sub].data.datasets.filter(obj => obj.label != 'contrained-off');
            charts[sub].update();
        }
    }
}

var response_coff
function get_constrained_off(dataInicial,dataFinal){
    $.ajax({
        url: "/middle/API/get/ons-dados-abertos/constrained-off/geracao-limitada/eolica",
        type : 'GET',
        data: {
            'dtInicial': dataInicial.format("YYYY-MM-DD"),
            'dtFinal': dataFinal.format("YYYY-MM-DD"),
            'pivot':true
        },
        success: function(resposta){
            response_coff = resposta
            let c_off = resposta

            for(submercado in c_off){

                let c_off_submercado = []
                for (date in c_off[submercado]){
                    c_off_submercado.push({t:dt_to_format = teste[moment(date, 'YYYY-MM-DD HH:mm').format('dddd HH:mm')], y:c_off[submercado][date]})
                }

                let = newDataset = {
                    label: `contrained-off`,
                    type: 'line',
                    backgroundColor: getColorSemana(1)[0],
                    borderColor: getColorSemana(1)[0],
                    fill: false,
                    data: c_off_submercado,
                    steppedLine: 'true',
                    pointRadius: 0.5,
                    borderWidth: 2,
                    tension: 0.1
                }
                charts[submercado].data.datasets.push(newDataset);
                charts[submercado].update();
            }
        },
        error: function() { 
            alert(`Não foi possivel constrained-off para o período ${dataInicial.format("DD/MM")} - ${dataFinal.format("DD/MM")}`)
      
        }
    })
}


var response_dc
function get_previsao_eolica_decomp(data){
    $.ajax({
        url: "/middle/API/get/geracao_eolica_prevista_dc",
        type : 'GET',
        data: {
            'data_referente': data.format("YYYY-MM-DD"),
        },
        success: function(resposta){
            response_dc = resposta

            medias = response_dc['media']
            previsao = response_dc['prev']


                for(submercado in previsao){ 

                    prev_eolica_horaria_dc = []
                    media_horaria_dc = []
                    media_diaria_dc = []
                    media_semanal_dc = []

                    for (date in medias[submercado]['horaria']){

                        media_horaria_dc.push({t:new Date(date), y:medias[submercado]['horaria'][date]})
                    }

                    for (date in medias[submercado]['diaria']){

                        media_diaria_dc.push({t:new Date(date), y:medias[submercado]['diaria'][date]})
                    }

                    for (date in medias[submercado]['semanal']){

                        media_semanal_dc.push({t:new Date(date), y:medias[submercado]['semanal'][date]})
                    }


                color_base = getColorSemana(numSemanas)
                // Decomp
                newDataset = {
                            label: `DC-s (${data.format("DD/MM")})`,
                            type: 'line',
                            backgroundColor: color_base[4],
                            borderColor: color_base[4],
                            fill: false,
                            data: media_semanal_dc,
                            steppedLine: 'true',
                            pointRadius: 0.5,
                            borderWidth: 1,
                            borderDash: [10,5],
                            tension: 0.1
                        }
                charts[submercado].data.datasets.push(newDataset);

                newDataset = {
                            label: `DC-h (${data.format("DD/MM")})`,
                            type: 'line',
                            backgroundColor: color_base[2],
                            borderColor: color_base[2],
                            fill: false,
                            data: media_horaria_dc,
                            steppedLine: 'true',
                            pointRadius: 0.5,
                            borderWidth: 1,
                            hidden: true,
                            tension: 0.1
                        }
                charts[submercado].data.datasets.push(newDataset);

                
                newDataset = {
                            label: `DC-d (${data.format("DD/MM")})`,
                            type: 'line',
                            backgroundColor: color_base[3],
                            borderColor: color_base[3],
                            fill: false,
                            data: media_diaria_dc,
                            steppedLine: 'true',
                            borderDash: [10,5],
                            hidden: true,
                            borderWidth: 1,
                            tension: 0.1
                        }
                charts[submercado].data.datasets.push(newDataset);


                charts[submercado].update();

                }

        },
        error: function() { 
            alert(`Não foi possivel plotar a previsao do decomp para a semana do dia (${data.format("DD/MM")})`)
      
        }
    })
}

