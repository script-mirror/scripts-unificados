function plotarDemandaGeracaoIntercambioTermica(ctx, dados) {
    const labels = ['N', 'NE', 'S', 'SE', 'geracao_termica', 'gtmax'];

    let datasets = [];

    for (let tipo in labels) {
        datasets[tipo] = new Dataset(labels[tipo] == 'geracao_termica' ? labels[tipo].replace('_', ' ') : labels[tipo], dados[labels[tipo]], false, labels[tipo] =='geracao_termica' ? '#000000' : coresDessem[labels[tipo]], labels[tipo] == "gtmax" || labels[tipo] == "geracao_termica" ? "line" : "bar");
    }
    const data = {
        datasets: datasets
    };

    const config = {
        data: data, options: new Options("Data/Hora", "Geracao", true)
    };
    return new Chart(ctx, config);
}

async function loadGeracaoTermica() {
    let navMain = document.getElementById('nav-main');
    // navMain.style.display = 'none';
    const dadosGeracao = await getDadosDessem('geracao-termica');
    let chartDiv = document.getElementById('line-gt')
    charts.push(plotarDemandaGeracaoIntercambioTermica(chartDiv, dadosGeracao))
    const datas = await getDatas()

    // navMain.style.display = 'flex';
}

async function filtrarGeracaoTermica(inicio, fim) {
    let navMain = document.getElementById('nav-main');
    const labels = ['N', 'NE', 'S', 'SE', 'geracao_termica', 'gtmax'];
    // navMain.style.display = 'none';
    const dados = await getDadosDessem('geracao-termica', inicio, fim);

    const lineGeracaoTermica = Chart.getChart('line-gt');

    let lineGeracaoDatasets = lineGeracaoTermica['config']['_config']['data']['datasets'];
    for (let tipo in labels) {
        console.log(labels[tipo])
        lineGeracaoDatasets[tipo] = new Dataset(labels[tipo] == 'geracao_termica' ? labels[tipo].replace('_', ' ') : labels[tipo], dados[labels[tipo]], false, labels[tipo] =='geracao_termica' ? '#000000' : coresDessem[labels[tipo]], labels[tipo] == "gtmax" || labels[tipo] == "geracao_termica" ? "line" : "bar");
    }

    lineGeracaoTermica.update();
    // navMain.style.display = 'flex';
}
