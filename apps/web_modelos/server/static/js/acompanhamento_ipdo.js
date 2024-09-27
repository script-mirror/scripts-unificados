
const dataInput = document.getElementById('inputDataRef');
dataInput.value = obterDataInicialMes();

var limparBtn = document.getElementById('btn-2');
var btnAtualizar = document.getElementById('btn-1');
var resultTable4 = document.getElementById('resultTable4');
var resultTable5 = document.getElementById('resultTable5');
var resultTable6 = document.getElementById('resultTable6');
var botaoPrevisao = document.getElementById('botaoPrevisao');
var botaoNewave = document.getElementById('botaoNewave');

function obterDataInicialMes() {
  var dataAtual = new Date();
  var mesAtual = dataAtual.getMonth();
  var anoAtual = dataAtual.getFullYear();
  var primeiroDiaMesAtual = new Date(anoAtual, mesAtual, 1);
  var primeiroDiaSemana = primeiroDiaMesAtual.getDay(); // Dia da semana do primeiro dia do mês
  var primeiroSabado;

  if (primeiroDiaSemana === 6 && primeiroDiaMesAtual.getDate() === 1) {
    // Se o primeiro dia do mês é sábado e dia 1, mantemos esse dia selecionado
    primeiroSabado = primeiroDiaMesAtual;
  } else {
    // Caso contrário, voltamos 7 dias para encontrar o último sábado do mês anterior
    primeiroSabado = new Date(anoAtual, mesAtual, 1);
    while (primeiroSabado.getDay() !== 6) {
      primeiroSabado.setDate(primeiroSabado.getDate() - 1);
    }
    
  }

  const anodataStart = primeiroSabado.getFullYear();
  const mesdataStart = String(primeiroSabado.getMonth() + 1).padStart(2, '0');
  const diadataStart = String(primeiroSabado.getDate()).padStart(2, '0');
  const dataStartString = `${anodataStart}-${mesdataStart}-${diadataStart}`;

  return dataStartString;
}



document.getElementById('botaoNewave').addEventListener('click', function() {
  relocate_processos()
});

function relocate_processos()
{
  location.href = "/middle/acompanhamento_newave";
} 


// Função para verificar se a data está no formato "dd/mm/yyyy"
function isDateValid(dateString) {
  var regex = /^\d{2}\/\d{2}\/\d{4}$/;
  return regex.test(dateString);
}
// Função para verificar se a data é igual à data de hoje
function isToday(dateString) {
  var today = new Date();
  var dateParts = dateString.split('/');
  var date = new Date(dateParts[2], dateParts[1] - 1, dateParts[0]);
  return today.toDateString() === date.toDateString();
}
// Função para buscar a célula com a data de hoje e rolar para ela
function scrollToTodayCell() {
  var tableCells = document.getElementsByTagName('td');
  // Procurando a célula com a data de hoje
  for (var i = 0; i < tableCells.length; i++) {
    var cellContent = tableCells[i].innerText;
    if (isDateValid(cellContent) && isToday(cellContent)) {
      var positionOptions = {
        behavior: 'smooth', // Para animação suave
        block: 'center',    // Rola para posicionar a célula no centro da tela
      };
      tableCells[i].scrollIntoView(positionOptions);
      break;
    }
  }
}
// Adicionando um evento de clique ao botão "previsao"
var previsaoButton = document.getElementById('botaoPrevisao');
previsaoButton.addEventListener('click', scrollToTodayCell);


document.getElementById('botao-flutuante').addEventListener('click', function() {
  window.scrollTo({
    top: 0,
    behavior: 'smooth'
  });
});

var botaoFlutuante = document.getElementById('botao-flutuante');
botaoFlutuante.style.display = 'none';

  window.addEventListener('scroll', function() {
    if (window.scrollY > 2) {
      botaoFlutuante.style.display = 'block';
    } else {
      botaoFlutuante.style.display = 'none';
    }
  });

document.getElementById("sidebarCollapse").addEventListener("click", function() {
  var sidebar = document.getElementById("sidebar");
  sidebar.classList.toggle("active");
});

function toggleMenu() {
  var menuContent = document.getElementById("menu-content");
  var menuButton = document.getElementById("menu-button");
  if (menuContent.style.display === "none") {
    menuContent.style.display = "block";
    menuButton.innerHTML = "Fechar Menu";
  } else {
    menuContent.style.display = "none";
    menuButton.innerHTML = "Menu";
  }
}

window.onload = function() {
  atualizaTabela();
  
};

function atualizaTabela() {
  var [ano, mes, dia] = dataInput.value.split('-');
  var dataEscolhida = `${ano}${mes}${dia}`;
  getPrevisao(dataEscolhida);
}

var datasIpdo = [];
var objetoIpdo
var objetoIpdoPrevisao
var objetoDecomp
var objetoNewave
var objetoipdo_media_valores
var objetopercentual_arredondado
var objetodiferenca_valores

function getPrevisao(data, grupo) {
  let rod = `${data} - ${grupo}`;


  function isSaturday(dateString) {
    // Extrai o ano, o mês e o dia da data fornecida
    const year = parseInt(dateString.substr(0, 4));
    const month = parseInt(dateString.substr(4, 2)) - 1; // Os meses em JavaScript são baseados em zero (janeiro é 0)
    const day = parseInt(dateString.substr(6, 2));
  
    // Cria um objeto Date com os valores extraídos
    const date = new Date(year, month, day);
  
    // Verifica se o dia da semana é um sábado (6)
    return date.getDay() === 6;
  }
  
  // Exemplo de uso
  const dateString = data;
  if (isSaturday(dateString)) {
    // console.log("A data é um sábado!");
  } else {
    toggleDiv()
    // alert("A data foi trocada para um sabado anterior, pois é o início da semana elétrica.");
  }

  function toggleDiv() {
    const div = document.getElementById('myDiv');
    if (div.style.display === 'none') {
      div.style.display = 'block'; // Exibe a div
      setTimeout(function() {
        div.style.display = 'none'; // Esconde a div após 2 segundos
      }, 6000);
    } else {
      div.style.display = 'none'; // Esconde a div
    }
  }



  setTimeout(function() {
    
    $.ajax({
      url: '/middle/API/get/acompanhamento_ipdo',
      method: 'GET',
      dataType: 'json',
      data: { dtref: `${data}`, grupo: grupo },
      success: function(response) {
        objetoIpdo = response;
        var grupoEscolhido = response.ipdo_verificado;
        // montaTabela(
        //   grupoEscolhido,
        //   grupo,
        //   'resultTable1',
        //   'titleTable1',
        //   'IPDO'
        // );
      },
      error: function(xhr, status, error) {
        console.error('An error occurred:', error);
      }
    });
  }, 0); // Atraso de 0 segundo para a primeira requisição

  setTimeout(function() {
    $.ajax({
      url: '/middle/API/get/acompanhamento_ipdo_previsao',
      method: 'GET',
      dataType: 'json',
      data: { dtref: `${data}`, grupo: grupo },
      success: function(response) {
        objetoIpdoPrevisao = response;
        var grupoEscolhido = response.ipdo_Previsao;
        // montaTabela(
        //   grupoEscolhido,
        //   grupo,
        //   'resultTable2',
        //   'titleTable2',
        //   '' 
        // );
      },
      error: function(xhr, status, error) {
        console.error('An error occurred:', error);
      }
    });
  }, 0); // Atraso de 0 segundos para a segunda requisição

  setTimeout(function() {
    $.ajax({
      url: '/middle/API/get/acompanhamento_decomp',
      method: 'GET',
      dataType: 'json',
      data: { dtref: `${data}`, grupo: grupo },
      success: function(response) {
        objetoDecomp = response;
        decomp = response.decomp;
        // Função para fazer todo cálculo de gerar os dados para inserir na tabela.
        var grupoEscolhido = processarDados(decomp);
        montaTabela(
          grupoEscolhido,
          grupo,
          'resultTable4',
          'titleTable4',
          'DECOMP'
        );
      },
      error: function(xhr, status, error) {
        console.error('An error occurred:', error);
      }
    });
  }, 0); // Atraso de 0 segundos para a terceira requisição

  setTimeout(function() {
    
    $.ajax({
      url: '/middle/API/get/acompanhamento_ipdo_verificado_e_previsao',
      method: 'GET',
      dataType: 'json',
      data: { dtref: `${data}`, grupo: grupo },
      success: function(response) {
        objetoipdoVerificadoPrevisao = response;
        grupoEscolhido = response.ipdo_verificado_previsao;
        montaTabela(
          grupoEscolhido,
          grupo,
          'resultTable6',
          'titleTable6',
          'IPDO'
        );
      },
      error: function(xhr, status, error) {
        console.error('An error occurred:', error);
      }
    });
  }, 0); // Atraso de 0 segundo para a primeira requisição


  setTimeout(function() {
    $.ajax({
      url: '/middle/API/get/expectativa_realizado',
      method: 'GET',
      dataType: 'json',
      data: { dtref: `${data}`, grupo: grupo },
      success: function(response) {
        var ipdo_media_valores;
        var percentual_arredondado;
        objetoipdo_media_valores = response;
        ipdo_media_valores = objetoipdo_media_valores[0];
        objetopercentual_arredondado = response;
        percentual_arredondado = objetopercentual_arredondado[1];
        objetodiferenca_valores = response;
        diferenca_valores = objetodiferenca_valores[2];

        // Função para fazer todo cálculo de gerar os dados para inserir na tabela.
        var grupoEscolhido = processarDados(objetoipdo_media_valores[0].ipdo_media_valores);
        var grupoEscolhido1 = processarDados(objetopercentual_arredondado[1].percentual_arredondado);
        var grupoEscolhido2 = processarDados(objetodiferenca_valores[2].diferenca_valores);
        montaTabelaResultTable5(grupoEscolhido, grupoEscolhido1, grupoEscolhido2,
          grupo,
          'resultTable5',
          'titleTable5',
          'EXPECTATIVA/REALIZADO'
        );
      },
      error: function(xhr, status, error) {
        console.error('An error occurred:', error);
      }
    });
  }, 0); // Atraso de 0 segundos para a terceira requisição


// Função para processar os dados obtidos com sucesso no Ajax
function processarDados(response) {
  
  function calcularDatas(dataString) {
    // Extrair o ano, mês e dia da dataString
    const ano = parseInt(dataString.substr(0, 4));
    const mes = parseInt(dataString.substr(4, 2));
    const dia = parseInt(dataString.substr(6, 2));

    // Obter o número de dias no mês
    const diasNoMes = new Date(ano, mes, 0).getDate();

    // Definir a data inicial
    var dataInicial = new Date(ano, mes - 1, dia);
    var diaSemana = dataInicial.getDay(); // Dia da semana (0 = domingo, 1 = segunda, etc.)

    // Retroceder para o sábado anterior, se necessário
    while (diaSemana !== 6) {
      dataInicial.setDate(dataInicial.getDate() - 1);
      diaSemana = dataInicial.getDay();
    }

    // Retroceder mais quatro sábados
    for (var i = 0; i < 3; i++) {
      dataInicial.setDate(dataInicial.getDate() - 7);
    }

    // Definir a data final
    var dataFinal = new Date(ano, mes, dia);
    diaSemana = dataFinal.getDay();

    // Avançar para o próximo sábado, se necessário
    while (diaSemana !== 6) {
      dataFinal.setDate(dataFinal.getDate() + 1);
      diaSemana = dataFinal.getDay();
    }

    // Avançar até o último dia do mês ou o próximo sábado
    while (dataFinal.getDate() < diasNoMes && dataFinal.getDay() !== 6) {
      dataFinal.setDate(dataFinal.getDate() + 1);
    }
    
    // Retornar as datas inicial e final
    return {
      dataInicial: dataInicial.toISOString().substr(0, 10),
      dataFinal: dataFinal.toISOString().substr(0, 10)
    };
  }

  // Exemplo de uso
  var dataString = data;
  var datas = calcularDatas(dataString);
  
  const chaveSubmercados = ['N', 'NE', 'SE', 'S'];
  const dicionarioResultados = {};
  const ultimoDia = new Date(datas.dataFinal);
  const json = response;
  for (let chave of chaveSubmercados) {
    const dadosDiarios = {};
    const dadosDiarios_newave = {};
    
    const lista_dias = Object.keys(json[chave]);
    const dataInicial = lista_dias[0];
    var dataInicialFiltrada = datas.dataInicial.replace(/-/g, "");
    var dia_inicial = parseInt(dataInicialFiltrada.slice(6, 8));
    var mes_inicial = parseInt(dataInicialFiltrada.slice(4, 6));
    var ano_inicial = parseInt(dataInicialFiltrada.slice(0, 4));
   
    const primeiroDiaIpdo = Object.keys(objetoIpdo.ipdo_verificado.N);
    const dateString = primeiroDiaIpdo[0];
    var year = dateString.substr(0, 4);
    var month = dateString.substr(4, 2) - 1; // Subtraindo 1 do mês, pois em JavaScript os meses são indexados de 0 a 11
    var day = dateString.substr(6, 2);
    var dt = new Date(year, month, day);
    var mes_atual = mes_inicial - 1;
    var ano_atual = ano_inicial;
    var valor_anterior = json[chave][dataInicial];
    
    while (
      (dt <= ultimoDia && dt.getMonth() === mes_atual) ||
      (dt.getMonth() === mes_atual + 1 && mes_atual - mes_inicial < 5)
    ) {
      const d_dt = dt.getDate().toString().padStart(2, '0');
      const m_dt = (dt.getMonth() + 1).toString().padStart(2, '0');
      const a_dt = dt.getFullYear().toString();

      const dtString = `${a_dt}${m_dt}${d_dt}`;
      if (dtString in json[chave]) {
        valor_anterior = json[chave][dtString];
      }
      dadosDiarios[dtString] = valor_anterior;
      dadosDiarios_newave[dtString] = valor_anterior;

      dt.setDate(dt.getDate() + 1);

      if (dt.getMonth() !== mes_atual && dt.getMonth() !== mes_atual + 1) {
        // Mudar para o próximo mês
        mes_atual++;
        if (mes_atual > 11) {
          mes_atual = 0;
          ano_atual++;
        }
      }
    }
    
    dicionarioResultados[chave] = dadosDiarios;
  }

  function excluirChavesPosteriores(dicionario, chaveReferencia) {
    const chaves = Object.keys(dicionario);
    const indiceReferencia = chaves.indexOf(chaveReferencia);
    if (indiceReferencia !== -1) {
      const chavesExcluir = chaves.slice(indiceReferencia + 1);
      chavesExcluir.forEach(chave => delete dicionario[chave]);
    }
  }

  const totalMeses = 4; // Número total de meses desejados
  const mesesCompletos = Math.floor(totalMeses / 12); // Quantidade de anos completos
  
  var chaves = Object.keys(objetoDecomp.decomp.N);
  var ultimaChave = chaves[chaves.length - 1];
  
  // Converter a última chave para um objeto Date
  var ano = parseInt(ultimaChave.substring(0, 4), 10);
  var mes = parseInt(ultimaChave.substring(4, 6), 10) - 1;
  var dia = parseInt(ultimaChave.substring(6, 8), 10);
  var ultimaData = new Date(ano, mes, dia);
  
  // Somar 6 dias
  ultimaData.setDate(ultimaData.getDate() + 6);
  
  // Formatar a nova data como "yyyyMMdd"
  var novoAno = ultimaData.getFullYear();
  var novoMes = (ultimaData.getMonth() + 1).toString().padStart(2, '0');
  var novoDia = ultimaData.getDate().toString().padStart(2, '0');
  var ultimaChave = `${novoAno}${novoMes}${novoDia}`;
  
  // Excluir as chaves posteriores à chave de referência em cada grupo do dicionário
  for (let grupo in dicionarioResultados) {
    excluirChavesPosteriores(dicionarioResultados[grupo], ultimaChave);
  }

  // Adicionar meses anteriores com valores nulos ou padrão, se necessário
  const primeiroDia = new Date(ano_inicial, mes_inicial - 1, dia_inicial);
  for (let grupo in dicionarioResultados) {
    const dadosDiarios = dicionarioResultados[grupo];
    var dataAtual = new Date(primeiroDia.getTime());
    for (let i = 0; i < mesesCompletos; i++) {
      dataAtual.setMonth(dataAtual.getMonth() - 1);
      const ultimoDiaMesAnterior = new Date(dataAtual.getFullYear(), dataAtual.getMonth() + 1, 0).getDate();
      for (let dia = 1; dia <= ultimoDiaMesAnterior; dia++) {
        const dtString = formatDate(dataAtual, dia);
        dadosDiarios[dtString] = null; // Adicionar null ou algum valor padrão para os dias sem dados
      }
    }
  }
  return dicionarioResultados
}
}


function montaTabelaResultTable5(grupoEscolhido, grupoEscolhido1, grupoEscolhido2, grupo, tableId, titleId, titulo) {
  var ordemSubmercados = ['SE', 'S', 'NE', 'N'];
  var table = document.getElementById(tableId);
  var title = document.getElementById(titleId);

  // Verifica se o grupoEscolhido é vazio ou se algum grupo está faltando
  if (
    !grupoEscolhido ||
    !grupoEscolhido1 ||
    !grupoEscolhido2 ||
    Object.keys(grupoEscolhido).length === 0 ||
    Object.keys(grupoEscolhido1).length === 0 ||
    Object.keys(grupoEscolhido2).length === 0
  ) {
    // Oculta a tabela
    table.style.display = 'none';
    return;
  }

  // Remove todas as linhas existentes da tabela
  while (table.tBodies[0].firstChild) {
    table.tBodies[0].removeChild(table.tBodies[0].firstChild);
  }

  // Define o título da tabela
  title.textContent = titulo;

  // Cria o tbody se não existir
  if (!table.tBodies.length) {
    table.createTBody();
  }

  // Obtém todas as datas disponíveis
  var datas = Object.keys(grupoEscolhido[ordemSubmercados[0]]);

  // Função para converter a data no formato do índice para "dd/mm/yyyy"
  function formatarData(data) {
    var dia = data.substring(6, 8);
    var mes = data.substring(4, 6);
    var ano = data.substring(0, 4);
    return dia + '/' + mes + '/' + ano;
  }

  // Cria a primeira linha com os nomes dos submercados
  var submercadosRow = document.createElement('tr');
  var emptyCell = document.createElement('th');
  submercadosRow.appendChild(emptyCell);

  ordemSubmercados.forEach(function (submercadoNome) {
    var submercadoCell = document.createElement('th');
    submercadoCell.textContent = submercadoNome;
    submercadosRow.appendChild(submercadoCell);
  });

  // Adiciona a linha com os nomes dos submercados
  table.tBodies[0].appendChild(submercadosRow);
  
  // Cria as linhas para cada intervalo de datas
for (var i = 0; i < datas.length; i += 7) {
  // Verifique se há menos de sete datas restantes
  if (i + 7 > datas.length) {
    rowData = datas.slice(i); // Use todas as datas restantes
  } else {
    rowData = datas.slice(i, i + 7); // Use o intervalo normal de sete datas
  }

  // Cria uma linha para o intervalo de datas
  var row = document.createElement('tr');
  row.setAttribute('data-id', i / 7);
  // adicionando classe para deixar linhas da tabela 5 vermelha
  row.classList.add('vermelho');
  // Cria a célula do índice
  var indexCell = document.createElement('td');
  indexCell.textContent = i / 7 + 1;
  indexCell.classList.add('hidden');
  row.appendChild(indexCell);

  // Cria a célula do intervalo de datas
  var intervaloCell = document.createElement('td');
  intervaloCell.innerHTML = ' <br><br>' + formatarData(rowData[0]) + '<br><br>⇕<br><br>' + formatarData(rowData[rowData.length - 1]);
  // ↕
  row.appendChild(intervaloCell);

  // Cria as células de valor para cada submercado
  ordemSubmercados.forEach(function (submercadoNome) {
    var valorCell = document.createElement('td');
    var valor = '';
    rowData.forEach(function (data, index) {
      if (index > 0) valor += '<br>';
      if (index === 0 && grupoEscolhido[submercadoNome]) valor += '<br><br>' + grupoEscolhido[submercadoNome][data];
      else if (index === 1 && grupoEscolhido1[submercadoNome]) valor += '<br>' + grupoEscolhido1[submercadoNome][data] + '%';
      else if (index === 2 && grupoEscolhido2[submercadoNome]) valor += '<br>' + grupoEscolhido2[submercadoNome][data];
    });
    if (rowData.length === 1) {
      // Adiciona o valor de cada objeto, mesmo que sejam nulos
      if (grupoEscolhido1[submercadoNome]) valor += (valor ? '<br><br>' : '') + grupoEscolhido1[submercadoNome][rowData[0]] + '%';
      if (grupoEscolhido2[submercadoNome]) valor += (valor ? '<br><br>' : '') + grupoEscolhido2[submercadoNome][rowData[0]];
    }

    valorCell.innerHTML = valor;

    // Adicione a classe CSS para alinhamento centralizado
    valorCell.classList.add('texto-centralizado');
    row.appendChild(valorCell);
  });

  //// Verifica se a tabela é a resultTable5 e adiciona a classe para ficar vermelha
  //if (tableId === 'resultTable5') {
  //  table.classList.add('vermelho');
  //}


  // Adiciona a linha ao tbody
  table.tBodies[0].appendChild(row);
}}


function montaTabela(grupoEscolhido, grupo, tableId, titleId, titulo) {

  var ordemSubmercados = ['SE', 'S', 'NE', 'N'];
  var table = document.getElementById(tableId);
  var title = document.getElementById(titleId);


  // Verifica se o grupoEscolhido é vazio
  if (!grupoEscolhido || Object.keys(grupoEscolhido).length === 0) {
    // Oculta a tabela
    table.style.display = 'none';
    return;
  }


  // Remove todas as linhas existentes da tabela
  while (table.tBodies[0].firstChild) {
    table.tBodies[0].removeChild(table.tBodies[0].firstChild);
  }

  // Define o título da tabela, se estiver sem titulo ele não insere o titulo.
  if (titulo) {
    title.textContent = titulo;
  }

  // Obtendo todas as linhas da tabela
  var linhas = table.getElementsByTagName("tr");

  // Percorrendo as linhas da tabela em ordem inversa
  for (var i = linhas.length - 1; i >= 0; i--) {
    var linha = linhas[i];
    
    // Verificando se a linha contém uma tag <th>
    if (linha.getElementsByTagName("th").length > 0) {
      tabela.deleteRow(i); // Removendo a linha
    }
  }

  // Cria o tbody se não existir
  if (!table.tBodies.length) {
    table.createTBody();

    var headerRow = document.createElement('tr');
    var emptyHeaderCell = document.createElement('th');
    headerRow.appendChild(emptyHeaderCell);

    var titleCell = document.createElement('th');
    titleCell.textContent = titulo;
    titleCell.setAttribute('colspan', ordemSubmercados.length);
    headerRow.appendChild(titleCell);

    // Adiciona as linhas de cabeçalho ao tbody
    table.tBodies[0].appendChild(headerRow);
  }

  var row1 = document.createElement('tr');
  // Célula vazia para o título dos submercados
  var emptyCell = document.createElement('th');
  row1.appendChild(emptyCell);
  // Preenche as células com os valores dos submercados
  ordemSubmercados.forEach(function(submercadoNome) {
    var submercadoCell = document.createElement('th');
    submercadoCell.textContent = submercadoNome;
    row1.appendChild(submercadoCell);
  });

   // Adiciona a linha ao tbody
  if (table.id !== "resultTable2"){
    table.tBodies[0].appendChild(row1);
  }

  // Obtém todas as datas disponíveis
  var datas = Object.keys(grupoEscolhido[ordemSubmercados[0]]);
  // Função para converter a data no formato do índice para "dd/mm/yyyy"
  function converteData(data) {
    var dia = data.substring(6, 8);
    var mes = data.substring(4, 6);
    var ano = data.substring(0, 4);
    return dia + '/' + mes + '/' + ano;
  }

  // Itera sobre cada data e cria uma linha para cada dia e valor
  datas.forEach(function(data, index) {
    var row = document.createElement('tr');
    // Cria a célula do índice
    var indexCell = document.createElement('td');
    indexCell.textContent = index + 1;
    indexCell.classList.add('hidden');
    row.appendChild(indexCell);

    // Cria a célula da data
    var dataCell = document.createElement('td');
    var dataConvertida = converteData(data);
    dataCell.textContent = dataConvertida;
    row.appendChild(dataCell);

    // Preenche as células com os valores de cada submercado na data correspondente
    ordemSubmercados.forEach(function(submercadoNome) {
      var valorCell = document.createElement('td');
      valorCell.textContent = grupoEscolhido[submercadoNome][data];


    // Verifica se a tabela é a resultTable2 e adiciona a classe CSS "vermelho"
    if (tableId === 'resultTable2') {
        valorCell.classList.add('vermelho');
      }

      // Função para converter string de data no formato "DD/MM/AAAA" para objeto Date
    function convertStringToDate(dateString) {
      const [dia, mes, ano] = dateString.split('/');
      return new Date(`${ano}-${mes}-${dia}`);
    }

    // Função para verificar se a data é hoje ou posterior
    function isDateTodayOrFuture(dateString) {
      const dataLinha = convertStringToDate(dateString);
      const hoje = new Date();
      const ontem = new Date(hoje);
      ontem.setDate(hoje.getDate() -1);
      ontem.setHours(0, 0, 0, 0);  
       // Define o horário para 00:00:00:00 para comparar apenas a data
      // hoje.setHours(0, 0, 0, 0);  
      return dataLinha >= ontem;
    }

    // Itere pelas linhas da tabela
    const tabela = document.getElementById('resultTable6');
    const linhas = tabela.getElementsByTagName('tr');
    for (let i = 1; i < linhas.length ; i++) { // Começa em 1 para pular o cabeçalho da tabela
      const linha = linhas[i];
      const dataCell = linha.querySelector('td:nth-child(2)'); // Segunda célula contém a data
      if (dataCell) {
        const dataString = dataCell.textContent.trim();
        if (isDateTodayOrFuture(dataString)) {
          
          const tdList = linha.getElementsByTagName('td');
          for (const td of tdList) {
            td.classList.add('vermelho');
          }
        }
      }
    }

      row.appendChild(valorCell);
    });

    // Adiciona o atributo data-id com o valor do índice na linha
    row.setAttribute('data-id', index);
    // Adiciona a linha ao tbody
    table.tBodies[0].appendChild(row);
  });
  
  // Verifica se a tabela é a resultTable6 e remove a última linha
  if (tableId === 'resultTable6') {
    var ultimaLinha = table.tBodies[0].lastElementChild;
    table.tBodies[0].removeChild(ultimaLinha);
  }
}



// Adicione um evento de clique ao botão de limpar
function erase() {
  limparBtn.addEventListener('click', function() {
    var tbody = document.querySelector('#resultTable6 tbody');
    tbody.innerHTML = '';
  });
}

function dataAtual() {
  var hoje = new Date();
  var dd = String(hoje.getDate()).padStart(2, '0');
  var mm = String(hoje.getMonth() + 1).padStart(2, '0');
  var yyyy = hoje.getFullYear();
  return yyyy + '-' + mm + '-' + dd;
}

function ajustarAlturaTabela() {
  var tabela4 = document.getElementById('resultTable4');
  var tabela5 = document.getElementById('resultTable5');
  var tabela6 = document.getElementById('resultTable6');

  var altura4 = tabela4.getElementsByTagName('tr').length;
  var altura5 = tabela5.getElementsByTagName('tr').length;
  var altura6 = tabela6.getElementsByTagName('tr').length;

  tabela4.style.height = altura4 * 40 + 'px'; // Ajuste a altura conforme necessário
  tabela5.style.height = altura5 * 40 + 'px'; // Ajuste a altura conforme necessário
  tabela6.style.height = altura6 * 40 + 'px'; // Ajuste a altura conforme necessário
}

// Chamada da função para ajustar a altura das tabelas ao carregar a página
window.addEventListener('load', ajustarAlturaTabela);