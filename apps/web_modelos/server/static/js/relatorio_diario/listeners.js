$(window).on('load', function () {
    // ena ndidas
    qtdDeDiasASelecionar(5)
    document.querySelector('input[name="radioRegiao"]').click()
    document.getElementById("buttonBuscar").click()
 
    document.getElementById("buttonBuscar").addEventListener("click", () => {
        buscarEna()
    });
    // ena ndidas
 
 
    // carga horaria
    plotarCargaHoraria()
    document.getElementById('selectTipoPlot').addEventListener("change", atualizar)
    document.getElementById('selectTipoPlot').addEventListener("change", tipoPlot)
    // carga horaria
 
    //geracao eolica
    listenersGeracaoEolica()
    create_chart()
    inicializa_label()
    data = moment(dt_verificado.value, "YYYY-MM-DD")
    diasPlot = 7
    get_verificada_eolica(data, 3)
    // geracao eolica
 
    GraficoComparacao(moment().subtract(1, 'days').format('YYYY-MM-DD'), 'pconjunto')
    GraficoComparacao(moment().subtract(1, 'days').format('YYYY-MM-DD'), 'gefs')
    GraficoComparacao(moment().subtract(1, 'days').format('YYYY-MM-DD'), 'gfs')
    GraficoComparacao(moment().subtract(1, 'days').format('YYYY-MM-DD'), 'eta40')

})

let mapaObs;

$(window).on('DOMContentLoaded', function(){
    mapaObs = new Mapa("observado", esquemaCoresObs, 3.5)
    for (let i in nomesModelos) {
        CardMapa(nomesModelos[i])
        mapasPrevisao.push(new Mapa(nomesModelos[i], esquemaCoresPrevisao))
    }
    mapaObs.getChuvaObservadaPorDia(15)

    let dataAtual = new Date();
    let dataRodada = dataAtual.setDate(dataAtual.getDate() - 1)
    dataRodada = formatarData(dataAtual);

    for (let i in nomesModelos) {
        mapasPrevisao[i].getChuvaPrevisao(dataRodada)
        let inputData = document.getElementById(`data-inicial-${nomesModelos[i]}`)
        inputData.value = dataRodada;

        inputData.addEventListener('change', () => { procurarData(inputData.name, inputData.value) }
        )

    }
    criarLabelMapaObs()
})
