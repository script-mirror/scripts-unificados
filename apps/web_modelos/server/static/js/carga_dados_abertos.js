moment.locale('pt-br');

async function fetchData(path) {

    const loading = document.getElementById('loading');
    const buscar = document.getElementById('buscar');
    loading.style.display = 'block';
    buscar.setAttribute('disabled', '');
    const promise = await fetch(`API/get/${path}`)
    loading.style.display = 'none';
    buscar.removeAttribute('disabled');
    return await promise.json()
}


function plotarGrafico(ctx, tipoDado) {
    const config = {
        options: new Options('Data/Hora', tipoDado, ctx.replace('grafico-', ''), true)
    };
    return new Chart(ctx, config);
}


function filtrarGrafico(ctx, dados, tipo) {

    const chart = Chart.getChart(ctx);
    const dataset = new Dataset(tipo, dados, false, utils.cores[tipo], tipo.includes('geracao') && tipo != 'geracao-total' ? 'bar' : 'line');
    chart['config']['data']['datasets'].push(dataset);

    chart.update();
    // navMain.style.display = 'flex';
}

const filtrarTodos = () => {
    const dataInicio = $('#data-inicio').val();
    const dataFim = $('#data-fim').val();
    let graficos = ['grafico-sin',
        'grafico-sudeste',
        'grafico-sul',
        'grafico-norte',
        'grafico-nordeste']
    graficos.forEach((ctx) => {
        const chart = Chart.getChart(ctx);
        chart['config']['data']['datasets'] = [];
        chart.update();
    })
    $('.ckb-filtro').each((i, e) => {
        if (e.checked) {
            fetchData(`${utils.endpoints[$(e).val()]}?dtInicial=${dataInicio}&dtFinal=${dataFim}`).then((response) => {
                filtrarGrafico('grafico-sin', response.filter((object) => {
                    return object.id_submercado == 'SIN'
                }), $(e).val())
                filtrarGrafico('grafico-sudeste', response.filter((object) => {
                    return object.id_submercado == 'SUDESTE'
                }), $(e).val())
                filtrarGrafico('grafico-sul', response.filter((object) => {
                    return object.id_submercado == 'SUL'
                }), $(e).val())
                filtrarGrafico('grafico-norte', response.filter((object) => {
                    return object.id_submercado == 'NORTE'
                }), $(e).val())
                filtrarGrafico('grafico-nordeste', response.filter((object) => {
                    return object.id_submercado == 'NORDESTE'
                }), $(e).val())
            })
        }
    })

}

$(document).ready(() => {
    fetchData('ons-dados-abertos/datas').then((response) => {
        new Slider(response['datas'], document.getElementById('slider'), document.getElementById('data-inicio'), document.getElementById('data-fim'), filtrarTodos, 5);
    })

    plotarGrafico('grafico-sin', 'MW');
    plotarGrafico('grafico-sudeste', 'MW');
    plotarGrafico('grafico-sul', 'MW');
    plotarGrafico('grafico-norte', 'MW');
    plotarGrafico('grafico-nordeste', 'MW');
})

$('#buscar').click(() => {
    filtrarTodos();
})

$('#ckb-todos-geracao').click(() => {
    $('.ckb-filtro-geracao').each((i, e) => {
        e.checked = $('#ckb-todos-geracao')[0].checked && !e.disabled;
    })
})

$('#ckb-todos-eolica').click(() => {
    $('.ckb-filtro-eolica').each((i, e) => {
        e.checked = $('#ckb-todos-eolica')[0].checked && !e.disabled;
    })
})

$('#ckb-todos-solar').click(() => {
    $('.ckb-filtro-solar').each((i, e) => {
        e.checked = $('#ckb-todos-solar')[0].checked && !e.disabled;
    })
})


class Options {
    constructor(xTitle, yTitle, mainTitle, stacked) {
        xTitle = utils.formatar(xTitle);
        yTitle = utils.formatar(yTitle);
        mainTitle = utils.formatar(mainTitle);
        this.spanGaps = 1000 * 60 * 60 * 24 * 2
        this.animation = true;
        this.plugins = {
            tooltip: true,
            legend: { display: true },
            hover: { mode: null },
            title: {display: true, text: mainTitle, font: {size: 16}}
        }

        this.scales = {
            x: { title: { display: true, text: 'Data/Hora' }, scaleLabel: { display: true, labelString: 'Data' }, ticks: { maxTicksLimit: 30 }, type: 'time', time: { unit:'hour',displayFormats: { quarter: 'MMM YYYY', hour: 'HH', minute: 'MM', second: 'SS' } }, title: { display: true, text: xTitle }, scaleLabel: { display: true, labelString: 'Data' }, stacked: stacked, categoryPercentage: 1.0, barPercentage: 1.0
            },
            y: { title: { display: true, text: yTitle }, stacked: stacked,
            }
        }
        this.interaction = {
            intersect: false,
            mode: 'x',
        },
            this.tooltips = {
                mode: 'x',
                intersect: false
            },
            this.hover = {
                intersect: false,
                mode: 'x',
                axis: 'x',
            },
            this.parsing = {
                xAxisKey: 'dt_data_hora',
                yAxisKey: 'valor'
            }
        this.responsive = true;
        this.maintainAspectRatio = false;
    }
}
class Dataset {
    constructor(label, data, fill, color, type) {
        this.label = utils.formatar(label);
        this.data = data;
        this.pointStyle = false;
        this.borderColor = color;
        this.backgroundColor = color;
        this.fill = fill;
        this.type = type;
        this.borderWidth = 1.5;
        this.stack = label.includes('geracao') ? 'geracao' && label != 'geracao-total' : label.includes('constrained-off-eolica') ? 'coff-eolica' : label.includes('constrained-off-solar') ? 'coff-solar' : label
        this.order = utils.orders[label];
    }
}

const utils = {
    toObject: (keys, values) => {
        let result = {};
        keys.forEach((key, i) => result[key] = values[i]);
        return result;
    },
    formatar: (texto) => {
        texto = texto.replaceAll('-', ' ')
        texto = texto.replaceAll('none', 'N/A')
        texto = texto.split(' ')
        for (let i = 0; i < texto.length; i++) {
            texto[i] = texto[i][0].toUpperCase() + texto[i].substr(1);
        }
        texto = texto.join(' ');
        return texto
    },
    getLocalStorage: (key) => {
        return JSON.parse(localStorage.getItem(key))
    },
    cores: {
        'carga-global': '#000',
        'geracao-total': '#782777',
        'geracao-hidraulica': '#0C00AD',
        'geracao-termica': '#b80d00',
        'geracao-eolica': '#00ABB8',
        'geracao-nuclear': '#80D660',
        'geracao-solar': '#DC6B19',
        'constrained-off-eolica-cnf': '#FF3333',
        'constrained-off-eolica-ene': '#3333FF',
        'constrained-off-eolica-rel': '#33FF33',
        'constrained-off-eolica-none':'#FFFF33',
        'constrained-off-solar-cnf': '#FF3333',
        'constrained-off-solar-ene': '#3333FF',
        'constrained-off-solar-rel': '#33FF33',
        'constrained-off-solar-none':'#FFFF33',
    },
    
    orders: {
        'carga-global': 10,
        'geracao-total': 10,
        'geracao-hidraulica': 5,
        'geracao-termica': 2,
        'geracao-eolica': 3,
        'geracao-nuclear': 1,
        'geracao-solar': 4,
        'constrained-off-eolica': 1,
        'constrained-off-solar': 1
    },
    
    endpoints: {
        'carga-global': 'ons-dados-abertos/carga/global',
        'geracao-total': 'ons-dados-abertos/geracao-total',
        'geracao-hidraulica': 'ons-dados-abertos/geracao-usina/geracao/hidraulica',
        'geracao-termica': 'ons-dados-abertos/geracao-usina/geracao/termica',
        'geracao-eolica': 'ons-dados-abertos/geracao-usina/geracao/eolica',
        'geracao-nuclear': 'ons-dados-abertos/geracao-usina/geracao/nuclear',
        'geracao-solar': 'ons-dados-abertos/geracao-usina/geracao/solar',
        'constrained-off-eolica-cnf': 'ons-dados-abertos/constrained-off/geracao-limitada/eolica/cnf',
        'constrained-off-solar-cnf': 'ons-dados-abertos/constrained-off/geracao-limitada/solar/cnf',
        'constrained-off-eolica-ene': 'ons-dados-abertos/constrained-off/geracao-limitada/eolica/ene',
        'constrained-off-solar-ene': 'ons-dados-abertos/constrained-off/geracao-limitada/solar/ene',
        'constrained-off-eolica-rel': 'ons-dados-abertos/constrained-off/geracao-limitada/eolica/rel',
        'constrained-off-solar-rel': 'ons-dados-abertos/constrained-off/geracao-limitada/solar/rel',
        'constrained-off-eolica-none': 'ons-dados-abertos/constrained-off/geracao-limitada/eolica/none',
        'constrained-off-solar-none': 'ons-dados-abertos/constrained-off/geracao-limitada/solar/none'
    },
}


$('select').selectpicker();