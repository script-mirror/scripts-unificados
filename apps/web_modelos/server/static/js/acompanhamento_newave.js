
const dataInput = document.getElementById('inputDataRef');
dataInput.value = ultimoSabadoMesAnterior();
var limparBtn = document.getElementById('btn-2');
var btnAtualizar = document.getElementById('btn-1');
var resultTable3 = document.getElementById('resultTable3');
var botaoNewave = document.getElementById('botaIPDO');

function ultimoSabadoMesAnterior() {
  var dataAtual = new Date();
  var mesAtual = dataAtual.getMonth();
  var anoAtual = dataAtual.getFullYear();

  // Subtrai um mês da data atual
  var dataMesAnterior = new Date(anoAtual, mesAtual - 1, 1);

  // Define a data para o último dia do mês anterior
  dataMesAnterior.setMonth(dataMesAnterior.getMonth() + 1);
  dataMesAnterior.setDate(0);

  // Obtém o dia da semana do último dia do mês anterior
  var diaSemana = dataMesAnterior.getDay();

  // Calcula a diferença entre o último dia do mês anterior e o último sábado
  var diferencaDias = (diaSemana + 1) % 7;

  // Define a data para o último sábado do mês anterior
  var ultimoSabado = new Date(dataMesAnterior);
  ultimoSabado.setDate(dataMesAnterior.getDate() - diferencaDias);

  // Formata a data para 'yyyy-mm-dd'
  var dataStartString = `${ultimoSabado.getFullYear()}-${String(ultimoSabado.getMonth() + 1).padStart(2, '0')}-${String(ultimoSabado.getDate()).padStart(2, '0')}`;

  return dataStartString;
}


document.getElementById('botaoIPDO').addEventListener('click', function() {
  relocate_processos()
});

function relocate_processos()
{
  location.href = "/middle/acompanhamento_ipdo";
} 


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
      url: '/middle/API/get/acompanhamento_newave',
      method: 'GET',
      dataType: 'json',
      data: { dtref: `${data}`, grupo: grupo },
      success: function(response) {
        objetoNewave = response;
        newave = response.newave;

         // Função para fazer todo cálculo de gerar os dados para inserir na tabela.
         var dataFinal = new Date();
         var grupoEscolhido = filtrarValoresPorData(objetoNewave,data,dataFinal);

        montaTabela(
          grupoEscolhido,
          grupo,
          'resultTable3',
          'titleTable3',
          'NEWAVE'
        );
        },
      
      error: function(xhr,status, error) {
        console.error('An error occurred:', error);
      }
    });
  }, 0); // Atraso de 0 segundos para a quarta requisição

  
function filtrarValoresPorData(json, dataStart, dataFinal) {
  const jsonNovo = json;
  // diminuindo 1 dia do dia atual, para que pegue ate o dia de ontem.
  dataFinal.setDate(dataFinal.getDate() - 1);

  const anodataFinal = dataFinal.getFullYear();
  const mesdataFinal = String(dataFinal.getMonth() + 1).padStart(2, '0');
  const diadataFinal = String(dataFinal.getDate()).padStart(2, '0');
  const dataFinalString = `${anodataFinal}${mesdataFinal}${diadataFinal}`;

  const anodataStart = dataStart.slice(0, 4);
  const mesdataStart = dataStart.slice(4, 6) - 1;
  const diadataStart = dataStart.slice(6, 8);
  const datastartString = new Date(anodataStart, mesdataStart, diadataStart);
  
  var diaSemanaNumerico = datastartString.getDay(); // Dia da semana (0 = domingo, 1 = segunda, etc.)
  
  // Retroceder para o sábado anterior, se necessário
  while (diaSemanaNumerico !== 6) {
    datastartString.setDate(datastartString.getDate() - 1);
    diaSemanaNumerico = datastartString.getDay();
  }
  
  // Retroceder mais quatro sábados
  for (var i = 0; i < 3; i++) {
    datastartString.setDate(datastartString.getDate() - 7);
  }
  
  const anoFinal = datastartString.getFullYear();
  const mesFinal = String(datastartString.getMonth() + 1).padStart(2, '0');
  const diaFinal = String(datastartString.getDate()).padStart(2, '0');
  const dataInicioString = `${anoFinal}${mesFinal}${diaFinal}`;
 
  var valoresFiltrados = {};
  
  for (var regiao in jsonNovo.newave) {
    valoresFiltrados[regiao] = {};
    var valorAnterior = null;
    
    for (var chave in json.newave[regiao]) {
      var dataChave = chave.substring(0, 8);
      var valorChave = json.newave[regiao][chave];
  
      if (dataChave >= dataInicioString && dataChave <= dataFinalString) {
        valoresFiltrados[regiao][dataChave] = valorChave;
        valorAnterior = valorChave;
      }
      
      // Replicar o valor para datas subsequentes até a próxima chave
      if (valorAnterior !== null && dataChave < dataFinalString) {
        var dataAtual = new Date(parseInt(dataChave.slice(0, 4)), parseInt(dataChave.slice(4, 6)) - 1, parseInt(dataChave.slice(6, 8)));
        dataAtual.setDate(dataAtual.getDate() + 1);
        
        while (dataAtual <= dataFinal && !(dataAtual.toISOString().slice(0, 10) in json.newave[regiao])) {
          valoresFiltrados[regiao][dataAtual.toISOString().slice(0, 10).replace(/-/g, "")] = valorAnterior;
          dataAtual.setDate(dataAtual.getDate() + 1);
        }
      }
    }
  }

  return valoresFiltrados;
}
}


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

      row.appendChild(valorCell);
    });

    // Adiciona o atributo data-id com o valor do índice na linha
    row.setAttribute('data-id', index);
    // Adiciona a linha ao tbody
    table.tBodies[0].appendChild(row);
  });
}


// Adicione um evento de clique ao botão de limpar
function erase() {
  limparBtn.addEventListener('click', function() {
    var tbody = document.querySelector('#resultTable3 tbody');
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

  var tabela3 = document.getElementById('resultTable3');
  var altura3 = tabela3.getElementsByTagName('tr').length;
  tabela3.style.height = altura3 * 40 + 'px'; // Ajuste a altura conforme necessário


}

// Chamada da função para ajustar a altura das tabelas ao carregar a página
window.addEventListener('load', ajustarAlturaTabela);