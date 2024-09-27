async function GraficoMlt(granularidade, tipo, indexDasetSelecionado) {
    const { createApp, ref } = Vue;
    let data = await getDataMlt(granularidade)
    let selectOptions = ""
    for(let regiao in data.datasets){
        if(regiao == 0){
            selectOptions += (` <option value='${regiao}' selected data-tokens=''>${data.datasets[regiao].label}</option>`)

        }else{
        selectOptions += (` <option value='${regiao}' data-tokens=''>${data.datasets[regiao].label}</option>`)
        }
    }

    createApp({
        delimiters: ['[[', ']]'],
        template: `
            <div class="row">
                <form>
                    <div class="col">
                        <select class="selectpicker" multiple data-live-search="true" data-actions-box="true" title="Nada selecionado"
                            id=options-${granularidade}>
                            
                            ${selectOptions}
                        </select>

                    </div>
                    <div class="col">
                        <div>
                            <button type="button" class="btn btn-dark" style="margin:2px" id="btn-trocar-${granularidade}">Alterar tipo de grafico</button>
                            <button type="button" class="btn btn-dark" style="margin:2px" id="btn-${granularidade}">Buscar</button>
                        </div>
                    </div>
                </form>
            </div>
            <div class="chart-container row">
                <canvas id=chart-${granularidade} height="80vh"> </canvas>
            </div>
        `
    }).mount(`#card-${granularidade}`)

    const ctx = document.getElementById(`chart-${granularidade}`).getContext('2d');
    const options = {
        responsive: true,
        scales: {
            y: {
                scaleLabel: {
                    display: true,
                    labelString: 'MLT',
                    fontSize: 20
                },
                ticks: {
                    callback: function (value, index, values) {
                        return value;
                    },
                    beginAtZero: true,
                    fontSize: 15,
                    display: true
                },
                // gridLines:{
                //     lineWidth: 1,
                //     color: "#000"
                // }

            },
            x: {
                // type: 'time',
                // position:'bottom',
                // time:{
                //     displaFormats: {'day': 'MM'},
                //     tooltipFormat:'MM/YY',
                //     unit:'month'
                // },
                gridLines: {
                    display: false
                }
            }
        },
        interaction: {
            intersect: false,
            mode: 'index',
        },
        tooltips: {
            mode: 'x'
        },
        hover: {
            intersect: false,
            mode: 'x',
            axis: 'x',
        },

        plugins:
        {
            title: {
                font: {
                    size: 20
                },
                display: true,
                text: `MLT ${granularidade}`
            },
            legend: {
                display: false,
                position: "right"
            },
            tooltip: {
                titleFont: { size: 20 },
                // caretSize:50
            }
        },
        elements: {
            borderWidth: 20
        },
        maintainAspectRatio: false,
        // onHover: function(event, activeElements) {
        //     if (activeElements.length) {
        //       let mouseX = event.x;
        //       drawVerticalLine(event.chart, mouseX);
        //     }
        //   }
    };
    
    let chart = new Chart(ctx, {
        type: tipo,
        data: await filtrarDatasets(granularidade, false),
        options: options
    });
    document.getElementById(`btn-trocar-${granularidade}`).addEventListener('click', () => { trocarTipo(chart) })
    document.getElementById(`btn-${granularidade}`).addEventListener('click', () => { filtrarDatasets(granularidade, true) })

}
