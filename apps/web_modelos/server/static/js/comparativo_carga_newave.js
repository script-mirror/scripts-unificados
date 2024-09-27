// Obtém o ano atual
const ano_Atual = new Date().getFullYear();
const mes_Atual = new Date().getMonth() + 1;

// valores deixado como default
var valorSelecionadoCarga = 'carga_global';
var valoresSelecionadosGeracoes  = [];
var valorSelecionadoSubmercado = 'SE';
var valorSelecionadoDeck = `${mes_Atual}/${ano_Atual}`;
var listaDeDatasDecksEscolhidos = []
let valorSelecionadoAno = [ano_Atual];

// funcao para dar atualizar a pagina
const AtualizarPagina = document.getElementById("Atualizar");
AtualizarPagina.addEventListener('click', function () {
    location.reload();
});

// Obtém o elemento onde os botões serão adicionados
const btnGroupAnos = document.querySelector('.btn-group-anos');
// Loop para criar os botões para os próximos 4 anos
for (let i = -1; i < 5; i++) {
    // Cria o elemento de label para o botão
    const label = document.createElement('label');
    label.classList.add('btn-sm', 'bg-wx-original-escuro','botaoAnoReferencia','ml-3');
    // Cria o elemento de input para o botão
    const input = document.createElement('input');
    input.type = 'radio';
    input.name = 'options';
    input.dataset.value = ano_Atual + i;
    input.id = 'ano' + (ano_Atual + i);
    // Cria o texto para o botão
    const textoBotao = document.createTextNode(ano_Atual + i);
    // Adiciona o input e o texto ao label
    label.appendChild(input);
    label.appendChild(textoBotao);

    // Adiciona a classe "active" se o ano for igual ao ano atual, pois ja começa como default ele ativo
    if (ano_Atual + i === ano_Atual) {
        label.classList.add('active');
    }
    // Adiciona o label ao grupo de botões
    btnGroupAnos.appendChild(label);
}

var dataAtual = new Date();
// Obtém o ano e o mês atual
var anoAtual = dataAtual.getFullYear();
var mesAtual = dataAtual.getMonth() + 1; // Os meses em JavaScript são baseados em zero, então adicionamos 1
// Cria uma referência ao elemento da dropdown-menu
var dropdownMenu = document.getElementById("dropdownMenu");
// Inicializa window.myChart como null
var myChart = null;

function pegaValorDeckClicado() {
    // Loop pelos meses e anos anteriores até o mês atual
    for (var ano = anoAtual - 1; ano <= anoAtual; ano++) {
        for (var mes = 1; mes <= 12; mes++) {
            // Se o ano for o ano atual e o mês for maior que o mês atual, pare o loop
            if (ano == anoAtual && mes > mesAtual) {
                break;
            }
            // Formata o mês e o ano para o formato desejado
            var mesFormatado = mes.toString().padStart(2, "0");
            var anoFormatado = ano.toString();

            // Cria o elemento de lista
            var listItem = document.createElement("li");

            // Cria a label e o input checkbox
            var label = document.createElement("label");
            var checkbox = document.createElement("input");
            checkbox.type = "checkbox";
            checkbox.value = mesFormatado + "/" + anoFormatado;

            // Texto da label
            var labelText = document.createTextNode(mesFormatado + "/" + anoFormatado);

            // Adiciona o checkbox e o texto à label
            label.appendChild(checkbox);
            label.appendChild(labelText);

            // Adiciona a label ao item da lista
            listItem.appendChild(label);

            // Adiciona um evento de clique ao checkbox
            checkbox.addEventListener("click", function(event) {
                // Verifica se o checkbox está marcado
                if (event.target.checked) {
                    // Adiciona o valor à lista
                    listaDeDatasDecksEscolhidos.push(event.target.value);
                } else {
                    // Remove o valor da lista, se estiver presente
                    var index = listaDeDatasDecksEscolhidos.indexOf(event.target.value);
                    if (index !== -1) {
                        listaDeDatasDecksEscolhidos.splice(index, 1);
                    }
                }
                // Impede a propagação do evento de clique para o elemento pai
                event.stopPropagation();
            });
            // Adiciona o item da lista ao dropdown-menu
            dropdownMenu.appendChild(listItem);

            // adiciona o item checked ao checkbox do ano e mes atual
            if (ano === anoAtual && mes === mesAtual) {
                checkbox.checked = true;
                listaDeDatasDecksEscolhidos.push(checkbox.value); 
            }
        }      
    }
}

// Seleciona os botões de cargas e adiciona um event listener a cada um deles
document.querySelectorAll('.botaoDecks').forEach(button => {
    button.addEventListener('click', function(event) {
        event.stopPropagation();
        pegaValorDeckClicado();
    });
});
// Adiciona um event listener para o dropdown-menu para evitar que ele feche ao clicar em um item
dropdownMenu.addEventListener("click", function(event) {
    event.stopPropagation();
});

// Chama a função pegaValorDeckClicado inicialmente
pegaValorDeckClicado();

function pegaValorCargaClicado(event) {
    // Recupera o valor do atributo data-value do botão clicado
    valorSelecionadoCarga = event.currentTarget.getAttribute('data-value');
    // Remove a classe 'active' de todos os botões de carga
    document.querySelectorAll('#navbarNav .botaoCarga').forEach(btn => btn.classList.remove('active'));
    // Adiciona a classe 'active' apenas ao botão clicado
    event.currentTarget.classList.add('active');

    // Verifica se o valor selecionado é 'nao_Simulada' ou 'carga_liquida'
    if (valorSelecionadoCarga === 'nao_Simulada' || valorSelecionadoCarga === 'carga_liquida') {
        // Desativa a opção de clicar nos botões de geração
        document.querySelectorAll('.botaoGeracoes').forEach(container => {
            container.classList.remove('active');
            container.style.pointerEvents = 'none';
        });
        // Limpa a lista de valores selecionados de geração
        valoresSelecionadosGeracoes = [];
    } else {
        // Ativa a opção de clicar nos botões de geração
        document.querySelectorAll('.botaoGeracoes').forEach(container => {
            container.style.pointerEvents = 'auto';
        });
    }
}

// Seleciona os botões de carga e adiciona um event listener a cada um deles
document.querySelectorAll('#navbarNav .botaoCarga').forEach(button => {
    button.addEventListener('click', pegaValorCargaClicado);
});

// Função para lidar com o clique nos botões de geracoes
function pegaValorGeracoesClicado(event) {
    const valorSelecionado = event.currentTarget.getAttribute('data-value');
    // Verifica se o valor já está na lista
    const index = valoresSelecionadosGeracoes.indexOf(valorSelecionado);
    // Se o valor já estiver na lista, remove-o
    if (index !== -1) {
        valoresSelecionadosGeracoes.splice(index, 1);
    } else { // Se não estiver na lista, adiciona-o
        valoresSelecionadosGeracoes.push(valorSelecionado);
    }
    // Remove a classe 'btn-selected' apenas se o botão clicado não tiver essa classe
    if (!event.currentTarget.classList.contains('active')) {
        // Remove a classe 'btn-selected' de todos os botões de cargas
        document.querySelectorAll('.btn-sm .botaoGeracoes').forEach(btn => btn.classList.remove('active'));
    }
    // Adiciona a classe 'btn-selected' apenas ao botão clicado
    event.currentTarget.classList.toggle('active');
}
// Seleciona os botões de cargas e adiciona um event listener a cada um deles
document.querySelectorAll('.btn-group-geracoes .botaoGeracoes').forEach(button => {
    button.addEventListener('click', pegaValorGeracoesClicado);
});

// Função para lidar com o clique nos botões de cargas
function pegaValorSubmercadoClicado(event) {
    valorSelecionadoSubmercado = event.currentTarget.getAttribute('data-value');
    // Remove a classe 'btn-selected' apenas se o botão clicado não tiver essa classe
    document.querySelectorAll('#navbarNav .botaoSubmercado').forEach(btn => btn.classList.remove('active'));
    event.currentTarget.classList.add('active');
}
// Seleciona os botões de cargas e adiciona um event listener a cada um deles
document.querySelectorAll('#navbarNav .botaoSubmercado').forEach(button => {
    button.addEventListener('click', pegaValorSubmercadoClicado);
});

// Função para lidar com o clique nos botões dos anos de referência
function pegaValorAnoReferenciaClicado(event) {
    // Recupera o elemento <input> filho do botão
    const input = event.currentTarget.querySelector('input[type="radio"], input[type="checkbox"]');
    // Verifica se o elemento <input> foi encontrado e recupera o valor do atributo data-value
    const valorSelecionado = input ? input.dataset.value : null;

    // Remove a classe 'btn-selected' apenas se o botão clicado não tiver essa classe
    if (!event.currentTarget.classList.contains('active')) {
        // Remove a classe 'btn-selected' de todos os botões de cargas
        document.querySelectorAll('.btn-sm .botaoAnoReferencia').forEach(btn => btn.classList.remove('active'));
    }
    // Adiciona a classe 'btn-selected' apenas ao botão clicado
    event.currentTarget.classList.toggle('active');

    // Converte os valores para números antes de verificar a presença no array
    const valorNumerico = parseInt(valorSelecionado);

    const index = valorSelecionadoAno.indexOf(valorNumerico);
    if (index === -1) {
        valorSelecionadoAno.push(valorNumerico); // Adiciona o ano se não estiver na lista
    } else {
        valorSelecionadoAno.splice(index, 1); // Remove o ano se já estiver na lista
    }
}

// Seleciona os botões de cargas e adiciona um event listener a cada um deles
document.querySelectorAll('#navbarNav .botaoAnoReferencia').forEach(button => {
    button.addEventListener('click', pegaValorAnoReferenciaClicado);
});

// Função para ordenar a lista em ordem crescente
function ordenarLista(lista) {
    return lista.sort(function(a, b) {
        // Converte as datas para o formato 'MM/YYYY' e compara
        return new Date(a.split('/').reverse().join('/')) - new Date(b.split('/').reverse().join('/'));
    });
}

function removerDuplicados(lista) {
    return lista.filter(function(item, index, self) {
        // Retorna true apenas para o primeiro índice do item na lista
        return self.indexOf(item) === index;
    });
}

function pegaDeck(){

    apagaMyChart();
    datasOrdenadasDecks = ordenarLista(listaDeDatasDecksEscolhidos);
    var listaSemDuplicados = removerDuplicados(datasOrdenadasDecks);
    var listaDeckFormatada = [];   
    for (var i = 0; i < listaSemDuplicados.length; i++) {
        var partes = listaSemDuplicados[i].split("/");
        var mes = partes[0];
        var ano = partes[1];
        // Adiciona um zero à esquerda se o mês tiver apenas um dígito
        if (mes.length < 2) {
            mes = '0' + mes;
        }
        listaDeckFormatada.push(ano + '-' + mes + '-' + '01');    
    }
    for (const data of listaDeckFormatada) {
        selecaoValores(data)  
    }
}

var dadosNewaveOrdenadosAnoMes;
var myChart;

// Função para capturar valores selecionados e fazer a requisição
function selecaoValores(data) {
    // Ativação do modal para efeito loading do gráfico
    modalLoading.style.display = "block";

    // Fazendo esse if para pegar o valor do deck default ou selecionado pelo usuário
    if (typeof data === 'undefined') {
        // Se a data for indefinida, execute este bloco de código
        var partes = valorSelecionadoDeck.split("/");
        var mes = partes[0];
        var ano = partes[1];
        var deckSelecionado = ano + '-' + mes.padStart(2, "0") + '-' + '01';
        var deck = ano + mes.padStart(2, "0");
    } else {
        // Se a data não for indefinida, execute este bloco de código
        deckSelecionado = data
        var partes = deckSelecionado.split("-");
        var ano = partes[0];
        var mes = partes[1];
        var deck = ano + mes.padStart(2, "0");
    }

    $.ajax({
        method: "GET",
        url: "/middle/API/get/comparativo_carga_newave",
        data: { data_referente: deckSelecionado },
    }).done(function (data) {

            if (data && typeof data === 'object') {
        
                var lista_N_Simuladas = []
                var lista_vl_cargaGlobal = [valorSelecionadoCarga]
                var lista_vl_Geracoes = [valoresSelecionadosGeracoes]
    
                if (valorSelecionadoCarga == 'nao_Simulada'){
                    lista_N_Simuladas = ['vl_geracao_eol','vl_geracao_eol_mmgd',
                                'vl_geracao_pch','vl_geracao_pch_mmgd',
                                'vl_geracao_pct','vl_geracao_pct_mmgd',
                                'vl_geracao_ufv','vl_geracao_ufv_mmgd']
                    lista_vl_cargaGlobal = []
                    lista_vl_Geracoes[0] = []
                }
                else if (valorSelecionadoCarga == 'carga_liquida'){
                    lista_N_Simuladas = ['vl_geracao_eol','vl_geracao_eol_mmgd',
                    'vl_geracao_pch','vl_geracao_pch_mmgd',
                    'vl_geracao_pct','vl_geracao_pct_mmgd',
                    'vl_geracao_ufv','vl_geracao_ufv_mmgd']
                    lista_vl_cargaGlobal = ['carga_global']
                    lista_vl_Geracoes[0] = []
                }else if(valorSelecionadoCarga == 'carga_global' && valoresSelecionadosGeracoes.length < 0){
                    lista_vl_cargaGlobal = ['carga_global']
                    lista_vl_Geracoes[0] = []
                }else{
                    lista_vl_cargaGlobal = ['carga_global']
                    lista_vl_Geracoes = [valoresSelecionadosGeracoes]
                }
    
                dadosNewaveOrdenadosAnoMes = {};
                let dadosSubmercado = {};
    
                let anosEscolhidos = valorSelecionadoAno;
                for (var deckChave in data) {
                    if (data.hasOwnProperty(deckChave)) {
                        var dadosPorSubmercado = data[deckChave];
                        // Criar um objeto para armazenar os dados do deck atual
                        let dadosDeckAtual = {};
                        for (var submercadoChave in dadosPorSubmercado) {
                            if (dadosPorSubmercado.hasOwnProperty(submercadoChave)) {
    
                                var objetoSubmercadoEscolhido = dadosPorSubmercado[valorSelecionadoSubmercado];
                                
                                // Iterar sobre os anos escolhidos
                                for (let ano of anosEscolhidos) {
                                    // Adicionar o ano como chave
                                    let dadosAno = {};
                                    for (let mes = 1; mes <= 12; mes++) {
                                        let chave = ano * 100 + mes;
    
                                        if (objetoSubmercadoEscolhido.hasOwnProperty(chave)) {
                                            // Adicionar os dados do mês ao objeto do submercado
                                            dadosAno[mes] = objetoSubmercadoEscolhido[chave];
                                        }
                                    }
                                    // Adicionar os dados do ano ao objeto do submercado
                                    dadosSubmercado[ano] = dadosAno;
                                }
                                // Adicionar os dados do submercado ao objeto do deck atual
                                dadosDeckAtual[valorSelecionadoSubmercado] = dadosSubmercado;
                            }
                        }
                        // Adicionar os dados do deck atual ao objeto principal usando o valor do deck como chave
                        dadosNewaveOrdenadosAnoMes[deckChave] = dadosDeckAtual;
                    }
                }
        
        // mapeamento para trocar sigla por nome do submercado 
        const mapeamentoSubmercado = {
            'NE': 'NORDESTE',
            'N': 'NORTE',
            'SE': 'SUDESTE',
            'S': 'SUL'
        };
        // funcao que faz troca de sigla por noome completo do submercado
        function trocarSiglaNomeSubmercado(valorSelecionadoSubmercado) {
            
            return mapeamentoSubmercado[valorSelecionadoSubmercado] || valorSelecionadoSubmercado; 
            // Retorna o valor mapeado se existir, caso contrário, retorna o valor original
        }

        // funcao para deixar valores null sempre no comeco da lista, sempre acontece com o ano vigente. Pois os valores que sao mostrados sao sempre do mes atual para frente
        function moveNullToFront(lista) {
            lista.sort((a, b) => {
                if (a === null) return -1; // Se 'a' for null, colocamos ele antes
                if (b === null) return 1;  // Se 'b' for null, colocamos ele antes
                return 0; // Se nenhum for null, a ordem não é alterada
            });
            return lista;
        }
        
        let labels = {}; // Objeto para armazenar os labels por ano
        let values = {}; // Objeto para armazenar os valores por ano

        for (let deck in dadosNewaveOrdenadosAnoMes){
            let objetoDeck = dadosNewaveOrdenadosAnoMes[deck];

            for (let submercado in objetoDeck){
                let objetoSubmercado = objetoDeck[submercado];
                
                // Loop pelos anos
                for (let ano in objetoSubmercado) {
                    let valoresObjetoAno = objetoSubmercado[ano];
                    let chave = `${deck}_${submercado}_${ano}`; // Forma a chave única para o objeto de valores

                    if (!values[chave]) {
                        // Se o objeto de valores correspondente a esta chave não existe, crie-o
                        values[chave] = new Array(12).fill(null); // Inicializa o array de valores para o ano com 12 valores null
                    }

                    for (let mes in valoresObjetoAno) {
                        let valoresMensais = valoresObjetoAno[mes];
                        let cargaGlobalMes = 0;
                        let cargaNaoSimuladaMes = 0;
                        let cargaLiquidadaMes = 0;
                        let cargaFinal = 0;
                        let cargaGeracoesSelecionadas = 0;
                    
                        // Inicializar a soma de carga global para este mês
                        cargaGlobalMes = valoresMensais['carga_global'];
                        
                        if (lista_vl_Geracoes[0].length > 0) {
                            // Loop para somar os valores das gerações selecionadas
                            for (let i = 0; i < lista_vl_Geracoes[0].length; i++) {
                                let tipoGeracao_N_Simuladas = lista_vl_Geracoes[0][i];
                                cargaGeracoesSelecionadas += valoresMensais[tipoGeracao_N_Simuladas];
                            }
                            cargaFinal = cargaGlobalMes - cargaGeracoesSelecionadas;
                        } else {
                            // Lógica para calcular a carga não simulada e liquida
                            cargaNaoSimuladaMes += valoresMensais["vl_geracao_eol"];
                            cargaNaoSimuladaMes += valoresMensais["vl_geracao_eol_mmgd"];
                            cargaNaoSimuladaMes += valoresMensais["vl_geracao_pch"];
                            cargaNaoSimuladaMes += valoresMensais["vl_geracao_pch_mmgd"];
                            cargaNaoSimuladaMes += valoresMensais["vl_geracao_pct"];
                            cargaNaoSimuladaMes += valoresMensais["vl_geracao_pct_mmgd"];
                            cargaNaoSimuladaMes += valoresMensais["vl_geracao_ufv"];
                            cargaNaoSimuladaMes += valoresMensais["vl_geracao_ufv_mmgd"];
                            cargaLiquidadaMes = cargaGlobalMes - cargaNaoSimuladaMes;
                        }

                        // Aplicar a lógica para adicionar valores ao array de valores correspondente ao ano
                        if (lista_vl_cargaGlobal.length > 0 && lista_N_Simuladas.length === 0 && valoresSelecionadosGeracoes.length === 0) {
                            // Calcula a soma dos valores para lista_vl_cargaGlobal
                            values[chave].splice(parseInt(mes) - 1, 1, parseFloat(cargaGlobalMes.toFixed(2)));

                        } else if (lista_N_Simuladas.length > 0 && lista_vl_cargaGlobal.length === 0) {
                            // Calcula a soma dos valores para lista_N_Simuladas
                            values[chave].splice(parseInt(mes) - 1, 1, parseFloat(cargaNaoSimuladaMes.toFixed(2)));
                            
                        } else if (lista_N_Simuladas.length > 0 && lista_vl_cargaGlobal.length > 0) {
                            // Calcula a soma dos valores para lista_N_Simuladas e lista_vl_cargaGlobal
                            values[chave].splice(parseInt(mes) - 1, 1, parseFloat(cargaLiquidadaMes.toFixed(2)));

                        } else if (lista_vl_cargaGlobal.length > 0 && valoresSelecionadosGeracoes.length > 0) {
                            // Calcula a soma dos valores para lista_vl_cargaGlobal excluindo os valores das geracoes selecionadas
                            values[chave].splice(parseInt(mes) - 1, 1, parseFloat(cargaFinal.toFixed(2)));
                        } else {
                            // Lógica para outros casos, se necessário
                        }
                    }
                }
            }
        }
        
        if (!myChart) {
            
            var ctx = document.getElementById('myChart').getContext('2d');
            myChart = new Chart(ctx, {
                type: 'line',
                data: {
                    // legenda com todos os meses de um ano, para adicionar no eixo X do grafico
                    labels: ['Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez'], 
                    // Adiciona os datasets aqui
                    datasets: []
                },
                options: {
                    legend: {
                        display:true,
                        position: 'bottom',
                    },
                    responsive: true,
                    maintainAspectRatio: false,
                    scales: {
                        xAxes: [{
                            // scaleLabel: {
                            //     display: true,
                            //     labelString: caso queira adicionar uma legenda a mais na parte inferior do grafico,
                            //     fontSize: 20,                        
                            // }
                        }],
                        yAxes: [{
                            scaleLabel: {
                                display: true,
                                labelString: 'MW',
                            },
                            ticks: {
                                beginAtZero: false,
                            }
                        }]
                    },
                    tooltips: {
                        mode: 'nearest', // Define o modo para "nearest" para mostrar o valor mais próximo
                        intersect: false // Define para não interseccionar os pontos do gráfico
                    },
                    // deixei o titulo de forma automatica no HTML
                    // title: {
                    //     display: true,
                    //     // text: `${trocarSiglaNomeSubmercado(valorSelecionadoSubmercado)}`,
                    //     fontSize: 20,
                    //     fontFamily: 'Arial',
                    //     fontStyle: 'bold',
                    // }
                }
            });
        }          
        // Itera sobre os decks e anos para adicionar os datasets ao gráfico
        for (let chave in values) {
            let deckAnoSubmercado = chave.split('_');
            let deck = deckAnoSubmercado[0];
            let ano = deckAnoSubmercado[2];
    
            // Move os valores null para o início do array
            moveNullToFront(values[chave]);
    
            // Verifica se já existe um conjunto de dados para o ano e deck
            if (values[chave].some(value => value !== null) && !myChart.data.datasets.some(dataset => dataset.label === `${ano}_Deck:${deck}`)) {
                let modelo = 'coresNewave';
                let corBackground = getColor_byModeloNewave(modelo)[Object.keys(myChart.data.datasets).length % getColor_byModeloNewave(modelo).length];
                let corBorder = corBackground.replace(', 0.2)', ', 1)'); // Ajusta a opacidade para a borda
    
                myChart.data.datasets.push({
                    label: `${ano}_Deck:${deck}`,
                    data: values[chave], // Aqui definimos os dados do dataset
                    backgroundColor: corBackground,
                    borderColor: corBorder,
                    fill: false,
                    borderWidth: 1,
                    pointRadius: 3,
                    tension: 0.1 // aqui colocamos o parametro para deixar as linhas mais retas
                });
            }
        }
        // adicionando titulo ao grafico com o submercado
        document.getElementById("tituloSubmercado").innerText = trocarSiglaNomeSubmercado(valorSelecionadoSubmercado);

        // Depois de adicionar todos os datasets necessários, atualize o gráfico
        myChart.update();
}
else {
        console.error("Os dados recebidos não estão no formato esperado.");
    }
}).fail(function(jqXHR, textStatus, errorThrown) {
    console.error("Erro na requisição AJAX:", textStatus, errorThrown);
    if (jqXHR.status === 0) {
        // Erro de conexão
        alert("Ocorreu um erro na conexão. Por favor, clique em recarregar página. Se o problema persistir, verifique a conexão de rede.");
    } else if (jqXHR.status === 404 || jqXHR.status === 500) {
        // Dados não encontrados (erro 404 ou 500)
        alert(`Os dados para deck ${deckSelecionado} não foram encontrados no banco de dados.Tente novamente com outros dados`);
    } else {
        // Outro tipo de erro
        alert("Ocorreu um erro na conexão.");
    }
}).always(function() {
    // Este bloco de código será executado sempre, independentemente do resultado da requisição
    // Ocultar o modal de loading aqui, se necessário
    modalLoading.style.display = "none";
});

}

function apagaMyChart() {
    if (myChart) {
        myChart.data.datasets = [];
        cargaGlobalMes = 0;
        cargaNaoSimuladaMes = 0;
        cargaLiquidadaMes = 0;
        cargaFinal = 0;
        cargaGeracoesSelecionadas = 0;
        myChart.update();
    }
}
selecaoValores();









