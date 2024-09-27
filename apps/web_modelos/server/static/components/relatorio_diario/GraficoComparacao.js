async function GraficoComparacao(dataRodada, modelo) {
    let chart;
    const { createApp, ref } = Vue
    createApp({
        delimiters: ['[[', ']]'],
        template: 
        `
        <div class="form-group">
            <input class="form-control" style="width:auto" type="date" id="datepicker-${modelo}" value=${dataRodada}>
        </div>

            <canvas id=chart-${modelo} > </canvas>
        
        `
    }).mount(`#card-diferenca-${modelo}`);
    const divModelo = document.getElementById(`chart-${modelo}`)

    let data = await getChartDataComparacao(dataRodada, modelo);
    if(data == '204'){
        // alert('204')
        divModelo.style.border = '1px solid red';
    }else{

    const ctx = document.getElementById(`chart-${modelo}`).getContext('2d');
    const options = {
        scales: {
            responsive: true,
            maintainAspectRatio: false,
            yAxes: [{

                ticks: {

                    beginAtZero: true,
                    fontSize:15,
                    display: true
                },
                gridLines:{
                    display: true
                }

            }],
            xAxes:[{
                scaleLabel: {
                    display: true,
                    labelString: `Observado X ${modelo}`,
                    fontSize:20
                  },
                gridLines:{
                    display: true
                }
            }]
        },
        interaction: {
            intersect: false,
            mode: 'x',
          },
        tooltips: {
            mode:'x'
        },
        hover:{
            intersect:false,
            mode:'x',
            axis:'x',
        },
        plugins: {
            title: {
                font: {
                    size: 20
                },
                display: true,
                text: `Observado X ${modelo}`
            }
        },
        elements: {
            borderWidth: 20
        }
    };
    chart = new Chart(ctx, {
        type: 'bar',
        data: data,
        options: options
    });
    let datePicker = document.getElementById(`datepicker-${modelo}`);

    datePicker.addEventListener('change',
    ()=>{
        updateChartComparacao(chart, datePicker.value, modelo)
    }
)
}

    // let dadosObservado = await getChuvaObservada(dataRodada)
    // let dadosPrevisto = await getChuvaPrevisao(modelo, dataRodada)
}