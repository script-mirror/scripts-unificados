let timeFormat = 'DD/MM/YYYY';
moment.locale('pt-br');
data = moment()


numSemanas = 0

dt_verificado = document.getElementById("semana-verificado")

dt_deck = document.getElementById("semana-verificado")

dia_semana = document.getElementById("semana-verificado")

dt_verificado.value = data.format("YYYY-MM-DD")


submercadosGeracaoEolica = ['NORDESTE', 'SUL']


let options = {
    title: {
        display: true,

    },
    legend: {
        display: true,
        position: 'bottom'
    },
    tooltips: {
        mode: 'x',
        callbacks: {
            title: function (tooltipItems) {
                const label = moment(tooltipItems[0].xLabel, 'MMM DD, YYYY, h:mm:ss a').format('dddd HH:mm');
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
    resposive: true,
    maintainAspectRatio: false,
    animation: {
        duration: 0
    }
}




let diasSemanaHr = {}

function inicializa_label() {
    dt_ini = moment('sábado 00:00', 'dddd HH:mm')
    diasSemanaHr[dt_ini.format('dddd HH:mm')] = dt_ini.add(-7, 'days')

    dt_aux = moment('sábado 00:00', 'dddd HH:mm').add(1, 'hours').format('dddd HH:mm')

    while (dt_aux != 'sábado 00:00') {
        dt_to_format = moment(dt_aux, 'dddd HH:mm')

        if (dt_aux.includes('sábado')) {

            dt_to_format = moment(dt_aux, 'dddd HH:mm').add(-7, 'days')
        }
        diasSemanaHr[dt_aux] = dt_to_format

        dt_aux = moment(dt_aux, 'dddd HH:mm').add(1, 'hours').format('dddd HH:mm')
    }
}


charts_eolica = {}
function create_chart() {

    for (sub of submercadosGeracaoEolica) {

        let ctx = document.getElementById("chart_eolica_" + sub).getContext("2d");
        ctx.canvas.width = $(window).width();
        ctx.canvas.height = $(window).height() * .8;
        options['title']['text'] = sub
        charts_eolica[sub] = new Chart(ctx, {
            // type: 'line',
            options: options

        })
    }


}

function limpar_graficos() {

    numSemanas = 0
    for (sub of submercadosGeracaoEolica) {
        charts_eolica[sub].data.datasets = [];
        charts_eolica[sub].update();
    }

}





let response_ds
function get_previsao_eolica_dessem(data_deck, data_semana) {

    $.ajax({
        url: "/middle/API/get/geracao_eolica_prevista_ds",
        type: 'GET',
        data: {
            'data_referente': data_semana.format("YYYY-MM-DD"),
            'data_deck': data_deck.format("YYYY-MM-DD")
        },
        success: function (resposta) {

            response_ds = resposta

            medias = response_ds['media']
            previsao = response_ds['prev']

            numSemanas++;
            for (submercado in previsao) {

                media_horaria_prev_ds = []
                media_diaria_prev_ds = []
                // media_semanal_verif = []


                //horaria
                for (date in medias[submercado]['horaria']) {


                    dt_to_format = moment(date, 'YYYY-MM-DD HH:mm:ss')
                    dt_formated = dt_to_format.format('dddd HH:mm')

                    media_horaria_prev_ds.push({ t: diasSemanaHr[dt_formated], y: medias[submercado]['horaria'][date] })

                }

                //diaria
                for (date in medias[submercado]['diaria']) {


                    dt_to_format = moment(date, 'YYYY-MM-DD HH:mm:ss')
                    dt_formated = dt_to_format.format('dddd HH:mm')

                    media_diaria_prev_ds.push({ t: diasSemanaHr[dt_formated], y: medias[submercado]['diaria'][date] })

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
                }
                charts_eolica[submercado].data.datasets.push(newDataset);

                newDataset = {
                    label: `DS-d -> Deck:(${data_deck.format("DD/MM")}) Semana:(${data_semana.format("DD/MM")})`,
                    type: 'line',
                    backgroundColor: color_base,
                    borderColor: color_base,
                    fill: false,
                    data: media_diaria_prev_ds,
                    steppedLine: 'true',
                    borderDash: [10, 5],
                    hidden: true,
                    borderWidth: 1,
                }
                charts_eolica[submercado].data.datasets.push(newDataset);

                charts_eolica[submercado].update();

            }


        },
        error: function () {
            console.error(`Não foi possivel plotar o previsao do dessem para a semana do dia (${data.format("DD/MM")})`)

        }
    })
}


let response_ver
function get_verificada_eolica(data, qtdDiaPrevisao) {

    $.ajax({
        url: "/middle/API/get/geracao_eolica_verificada",
        type: 'GET',
        data: {
            'data_referente': data.format("YYYY-MM-DD"),
        },
        success: function (resposta) {
            response_ver = resposta

            medias = response_ver['media']

            for (submercado in medias) {

                media_horaria_verif = []
                media_diaria_verif = []
                // media_semanal_verif = []

                ultimaData = Object.keys(response_ver['media'][submercado]['horaria']).pop()
                // ultimaData_sul = Object.keys(response_ver['media']['SUL']['horaria']).pop()

                if (moment(ultimaData, 'YYYY-MM-DD HH:mm:ss').format('dddd') == 'sábado') {

                    delete medias[submercado]['horaria'][ultimaData]
                    delete medias[submercado]['diaria'][ultimaData]
                }


                //horaria
                for (date in medias[submercado]['horaria']) {

                    dt_to_format = moment(date, 'YYYY-MM-DD HH:mm')
                    dt_formated = dt_to_format.format('dddd HH:mm')

                    media_horaria_verif.push({ t: diasSemanaHr[dt_formated], y: medias[submercado]['horaria'][date] })

                }

                //diaria
                for (date in medias[submercado]['diaria']) {

                    dt_to_format = moment(date, 'YYYY-MM-DD HH:mm')
                    dt_formated = dt_to_format.format('dddd HH:mm')

                    media_diaria_verif.push({ t: diasSemanaHr[dt_formated], y: medias[submercado]['diaria'][date] })

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
                }
                charts_eolica[submercado].data.datasets.push(newDataset);

                newDataset = {
                    label: `Ver-d (${data.format("DD/MM")})`,
                    type: 'line',
                    backgroundColor: color_base,
                    borderColor: color_base,
                    fill: false,
                    data: media_diaria_verif,
                    steppedLine: 'true',
                    borderDash: [10, 5],
                    hidden: true,
                    borderWidth: 1,
                }
                charts_eolica[submercado].data.datasets.push(newDataset);

                charts_eolica[submercado].update();


            }

            data_semana = moment(dia_semana.value, "YYYY-MM-DD")
            for (let i = 0; i < qtdDiaPrevisao; i++) {
                data_deck = moment(dt_deck.value, "YYYY-MM-DD").subtract(i, "days")
                get_previsao_eolica_dessem(data_deck, data_semana, diasPlot)
            }
        },
        error: function () {
            console.error(`Não foi possivel plotar o verificado para a semana do dia (${data.format("DD/MM")})`)
        }
    })
}



function listenersGeracaoEolica() {

    $('#buscar-prev-eolica-n-dias').on('click', function () {
        limpar_graficos()
        data = moment(dt_verificado.value, "YYYY-MM-DD")
        diasPlot = 7
    
        get_verificada_eolica(data, $("#qtd-dias-prev-eolica").val())
    });
}