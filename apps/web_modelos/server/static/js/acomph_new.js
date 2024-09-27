var debugVar = null;

function formatarData(data){
    const [ano, mes, dia] = data.split('-');

    return `${dia}/${mes}/${ano}`;
}



function loading(){
    let loading = document.getElementById('loading');

    loading.style.opacity = '1';
    loading.style.visibility = 'visible';

    setTimeout(() => {
        loading.style.opacity = '0';
        loading.style.visibility = 'hidden';    
    }, 3000);
}



function atualizarGraficos(){
    let produto = document.getElementById('selectProduct').value;
    let dataInicial = document.getElementById('startDate').value;
    let dataFinal = document.getElementById('endDate').value;

    let dataInicialFormatada = formatarData(dataInicial);
    let dataFinalFormatada = formatarData(dataFinal);
    console.log(dataInicialFormatada)

    console.log(`Conferindo os valores recebidos dos inputs ${produto}, ${dataInicial} e ${dataFinal}`)

    if(dataInicial !== '' && dataFinal !== ''){
        if(dataInicial < dataFinal){
            console.log('Iniciando a requisição...');
            loading();
            $.ajax({
                method: "GET",
                url: `/middle/API/new_getEnaAcomph?dataInicial=${dataInicialFormatada}&dataFinal=${dataFinalFormatada}&divisao=submercado&flagNoCache=0`, 
                // Na chave 'url' está sendo passado os parâmetros (dataInicial, dataFinal, divisao e flagCache) 
                // com os respectivos valores (dataInicialFormatada, dataFinalFormatada, submercado, 1)
                dataType: 'json',
                success: function(dados){
                    debugVar = dados;
                    console.log(debugVar)
                    setTimeout(() => {
                        plotarGraficos(dados);    
                    }, 3000);
                },
                error: function(status, error){
                    console.error('Erro na requisição:', status, error);
                    setTimeout(() => {
                        mostrarAviso('Erro ao obter os dados!');    
                    }, 2800);
                    
                },
            })
   
        } else{
            mostrarAviso('A data inicial não pode ser anterior à data final!');
        }
    } else{
        console.log('Valores não recebidos.');
        mostrarAviso('Preencha todos os campos.');
    }
}



function plotarGraficos(dados){
    
    const boxGraphs = document.querySelector('.boxGraphs');
    boxGraphs.innerHTML = ''; 

    let jsonTemDados = false; // Verificação feita, caso o intervalo selecionado não tenha informações.

    for (let submercado in dados) {
        if (Object.keys(dados[submercado]).length > 0) {
            jsonTemDados = true; 

            // Cria um container div para cada gráfico de submercado
            const chartContainer = document.createElement('div');
            chartContainer.style.width = '100%';
            chartContainer.style.height = '400px';
            chartContainer.id = `chart-${submercado}`;
            boxGraphs.appendChild(chartContainer);

            // Preparar os dados para o gráfico
            const datas = Object.keys(dados[submercado]); // As chaves dentro de cada submercado se referem as datas que serão colocadas no eixo x
            const valores = Object.values(dados[submercado]); // Os valores dentro dessas chaves, mostram o valor de energia em MWmed.

            // Inicializa o gráfico com echarts
            const chart = echarts.init(document.getElementById(`chart-${submercado}`));

            // Configuração do gráfico
            const option = {
                title: {
                    text: `${submercado}`,
                    left: 'center'
                },
                tooltip: {
                    trigger: 'axis',
                    formatter: (params) => {
                        let tooltipText = `${params[0].axisValue}<br/>`;
                        params.forEach((param) => {
                            tooltipText += `${param.seriesName}: ${param.data}<br/>`;
                        });
                        return tooltipText;
                    }
                },
                xAxis: {
                    type: 'category',
                    data: datas, // Recebe as chaves de submercado
                    axisLabel: {
                        formatter: function (value) {
                            return value.split('/').reverse().join('/'); // Formatar datas
                        }
                    }
                },
                yAxis: {
                    type: 'value', 
                    name: 'ENA (MWmed)',
                    axisLabel: {
                        formatter: '{value}'
                    }
                },
                series: [
                    {
                        name: 'Consumo (MWmed)',
                        type: 'line',
                        data: valores,
                        smooth: true, // Curva suave no gráfico
                        lineStyle: {
                            color: 'rgba(156, 51, 155, 1)'
                        },
                        itemStyle: {
                            color: 'rgba(156, 51, 155, 1)'
                        }
                    }
                ]
            };

            // Define as opções no gráfico
            chart.setOption(option);
        }
    }
    if(!jsonTemDados){
        mostrarAviso('Não há registros nesse período')
    }

}

let modelosDebug;

function atualizarTabelas(){
    let modelsDate = document.getElementById('modelsDate').value;

    
    console.log('Iniciando a requisição...');

    if(modelsDate !== ''){
        loading();
        $.ajax({
            method: 'GET',
            dataType: 'text', // Foi necessário receber a resposta da requisição em formato de txt, pelo fato de haver 'NaN'.
            url: `API/get/previsao/modelos-disponiveis?dt_rodada=${modelsDate}`,
            success: function(dados){
                dados = JSON.parse(dados.replace(/NaN/g, "null")); // Para só depois, tratar, tirando valores NaN, e convertendo para JSON
                modelosDebug = dados;
                console.log(modelosDebug)
                setTimeout(function() {
                    plotarTabelas(dados);
                }, 1000);
            },
            error: function(status, error){
                console.error('Erro na requisição:', status, error);
                setTimeout(() => {
                    mostrarAviso('Erro ao obter os dados!');    
                }, 2800);
            },
        })
    } else {
        mostrarAviso('Insira uma data válida')
    }
}


function plotarTabelas(dados){
    const boxTables = document.getElementById('boxTables');
    const corpoTabela = document.getElementById('bodyTable');
    corpoTabela.innerHTML = '';

    if(dados.length > 0){
        boxTables.style.display = 'flex';

        let modelosRodada0 = dados.filter((modelo)=>{
            return modelo.hr_rodada == 0
        })

        let modelosRodada6 = dados.filter((modelo)=>{
            return modelo.hr_rodada == 6
        })

        let modelosRodada12 = dados.filter((modelo)=>{
            return modelo.hr_rodada == 12
        })

        let modelosRodada18 = dados.filter((modelo)=>{
            return modelo.hr_rodada == 18
        })

        console.log(modelosRodada0);
        console.log(modelosRodada6);
        console.log(modelosRodada12);
        console.log(modelosRodada18);

        let qntdLinhasModelosRodada0 = modelosRodada0.length;
        let qntdLinhasModelosRodada6 = modelosRodada6.length;
        let qntdLinhasModelosRodada12 = modelosRodada12.length;
        let qntdLinhasModelosRodada18 = modelosRodada18.length;
        let qntdLinhasGeral = 0;

        if(qntdLinhasModelosRodada0 > qntdLinhasGeral){
            qntdLinhasGeral = qntdLinhasModelosRodada0;
        }
        if(qntdLinhasModelosRodada6 > qntdLinhasGeral){
            qntdLinhasGeral = qntdLinhasModelosRodada6;
        }
        if(qntdLinhasModelosRodada12 > qntdLinhasGeral){
            qntdLinhasGeral = qntdLinhasModelosRodada12;
        }
        if(qntdLinhasModelosRodada18 > qntdLinhasGeral  ){
            qntdLinhasGeral = qntdLinhasModelosRodada18;
        }

        console.log(qntdLinhasGeral);

        for(let linha = 1; linha <= qntdLinhasGeral; linha++){
            const corpoTabela = document.getElementById('bodyTable');
            corpoTabela.innerHTML += `<tr id='row${linha}'></tr>`;
            const linhaTabela = document.getElementById(`row${linha}`);
            linhaTabela.innerHTML += `<td></td>`;
            
            // Adiciona a coluna para modelosRodada0
            if(modelosRodada0[linha-1] == undefined){
                linhaTabela.innerHTML += `<td></td>`;
            } else{
                linhaTabela.innerHTML += `<td>${modelosRodada0[linha-1].str_modelo}</td>`;
            }
        
            // Adiciona a coluna para modelosRodada6
            if(modelosRodada6[linha-1] == undefined){
                linhaTabela.innerHTML += `<td></td>`;
            } else{
                linhaTabela.innerHTML += `<td>${modelosRodada6[linha-1].str_modelo}</td>`;
            }
        
            // Adiciona a coluna para modelosRodada12
            if(modelosRodada12[linha-1] == undefined){
                linhaTabela.innerHTML += `<td></td>`;
            } else{
                linhaTabela.innerHTML += `<td>${modelosRodada12[linha-1].str_modelo}</td>`;
            }
        
            // Adiciona a coluna para modelosRodada18
            if(modelosRodada18[linha-1] == undefined){
                linhaTabela.innerHTML += `<td></td>`;
            } else{
                linhaTabela.innerHTML += `<td>${modelosRodada18[linha-1].str_modelo}</td>`;
            }
        }
        
    } else{
        boxTables.style.display = 'none';

        mostrarAviso('Sem modelos disponíveis');
    }
}



let boxAviso = document.getElementById('boxWarning');

function mostrarAviso(newText) {
    let warningText = document.getElementById('warningText');

    boxAviso.style.opacity = '1';
    boxAviso.style.visibility = 'visible';
    warningText.innerHTML = `${newText}`;
}

function fecharAviso() {
    boxAviso.style.opacity = '0';
    boxAviso.style.visibility = 'hidden'; 
}



function escolherProduto(){
    expandirMenu();

    const boxParametros1 = document.getElementById('parameters1');
    const boxParametros2 = document.getElementById('parameters2');
    const btnPlotar = document.getElementById('btnPlotar');
    const product = document.getElementById('selectProduct').value;
    const boxGraphs = document.getElementById('boxGraphs');
    const boxTables = document.getElementById('boxTables');

    if(product == 'acomph'){
        boxParametros1.style.display = 'flex';
        boxParametros2.style.display = 'none';

        btnPlotar.style.display = 'flex';
        btnPlotar.addEventListener('click',atualizarGraficos);
        btnPlotar.removeEventListener('click',atualizarTabelas);

        boxGraphs.style.display = 'flex';

    } else if(product == 'previsoes'){
        boxParametros1.style.display = 'none';
        boxParametros2.style.display = 'flex';

        btnPlotar.style.display = 'flex';
        btnPlotar.addEventListener('click',atualizarTabelas);
        btnPlotar.removeEventListener('click',atualizarGraficos);

        boxGraphs.style.display = 'none';
        
    } else if(product == ''){
        boxParametros1.style.display = 'none';
        boxParametros2.style.display = 'none';

        btnPlotar.style.display = 'none';

        boxGraphs.style.display = 'none';

        fecharMenu();
    }
}


function expandirMenu(){
    const boxBtns = document.getElementById('boxBtns');

    boxBtns.style.width = '90vw';
}

function fecharMenu(){
    const boxBtns = document.getElementById('boxBtns');

    boxBtns.style.width = '20vw';
}