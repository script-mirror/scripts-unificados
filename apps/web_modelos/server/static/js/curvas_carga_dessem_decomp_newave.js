var submerc = {'SE':'SUDESTE', 'S': 'SUL','NE':'NORDESTE','N':'NORTE'}
const modalLoading = document.getElementById("modalLoading");
var listaDeDatasDecomp = []
var listaDeDatasDessem = []
var listaDeDatasNewave = []
var datasOrdenadasDecomp = []
var datasOrdenadasDessem = []
var datasOrdenadasNewave = []
var listaSabados = [];
var listaTodosSabados  = [];
var listaUltimosSabados  = [];
var divModelos = document.getElementById("modelos");
const dataInput = document.getElementById('inputDataRef');
var dadosDoAjax = {}


// Evento de clique para o botão "Plotar Tudo"
$("#btnPlotarTudo").click(function () {
  $("#btnPlotarDecomp").click();
  $("#btnPlotarDessem").click();
});


$(window).on('load', function(){
  iniciarListeners()
  create_chart()
  criarListaDinamicaDecomp()
  criarListaDinamicaNewave()
})

// funcao para esconder e aparecer menu.
function iniciarListeners(){
  $('#sidebarCollapse').on('click', function () {
      $('#sidebar').toggleClass('active');
  });
  $('[data-toggle="tooltip"]').tooltip()
}


// Obtendo de forma automatica o conteudo do botao ano, com 1 ano anterior
const selectAno = document.getElementById("selectAno");
const anoAtual = new Date().getFullYear()
const numeroDeAnos = 2; // Incluindo o ano atual e + 1 ano anterior
for (let i = 0; i < numeroDeAnos; i++) {
    const option = document.createElement("option");
    const ano = anoAtual - i;
    option.value = ano;
    option.textContent = ano;
    selectAno.appendChild(option);
}

// funcção para recarregar pagina, zerada
btnLimpar.addEventListener("click", function(){
  location.reload();
});


// Função de comparação para ordenar as datas
function compararDatas(data1, data2) {
  return data1.localeCompare(data2);
}

function converterListaDeDatas(lista) {
  const dates = lista[0].split(', '); // Divide as datas separadas por vírgula
  const dateParts = dates.map(function(dateStr) {
    const parts = dateStr.split("/");
    const ano = parts[2];
    const mes = parts[1];
    const dia = parts[0];
    return ano + mes + dia; // Converte para o formato yyyyMMdd
  });
  return dateParts.join(",");
}


// Evento de clique para o botão "Plotar Decomp"
$("#btnPlotarDecomp").click(function() {
  for (const submercado of Object.values(submerc)) {
    charts[submercado].data.datasets = [];
    charts[submercado].update();
  }
  listaDeDatasDecomp = [];

  const selectedOptions = Array.from(document.querySelectorAll(".options-list.decomp li.selected"));
  selectedOptions.forEach(option => {
    listaDeDatasDecomp.push(option.getAttribute("data-value"));
  });

  listaDeDatasDecomp.push(selectedOptionsDivDecomp.textContent);
  var dataDeckParamDecomp = converterListaDeDatas(listaDeDatasDecomp);
  datasOrdenadasDecomp = listaDeDatasDecomp.sort(compararDatas);
  const datasFormatadasDecomp = datasOrdenadasDecomp.map(data => {
    const [dia, mes, ano] = data.split('/');
    return `${ano}${mes}${dia}`;
  });

// Função para remover datas duplicadas
function removerDatasDuplicadas(lista) {
  const setDeDatas = new Set(); // Usando um conjunto SET para manter datas únicas
  lista.forEach(item => {
      const datas = item.split(',').map(dateStr => dateStr.trim()); // Dividir as datas separadas por vírgula
      datas.forEach(dateStr => {
          setDeDatas.add(dateStr);
      });
  });
  return Array.from(setDeDatas);
}
const datasUnicas = removerDatasDuplicadas(datasOrdenadasDecomp);

// Função para formatar e ordenar as datas para fazer requisição
function formatarEOrdenarDatas(lista) {
  const datasFormatadas = lista.map(data => {
      const [dia, mes, ano] = data.split('/');
      return `${ano}${mes}${dia}`;
  });
  datasFormatadas.sort(); // Ordena as datas
  return datasFormatadas;
}
const datasOrdenadas = formatarEOrdenarDatas(datasUnicas);

var indexColor = 0
for (const data of datasOrdenadas) {
  fazerRequisicao(data);
}

function fazerRequisicao(datasOrdenadas) {
  $.ajax({
    contentType: "application/json; charset=utf-8",
    dataType: "json",
    method: "GET",
    url: `/middle/API/GET/carga_decompPatamar?dataDeck=${datasOrdenadas}`,
    success: function(objeto) {  
        objeto_armazenamento = objeto;
        const objeto_ = objeto_armazenamento;
        var dataEscolhida = {}
        if (objeto && objeto.length > 0) {
          document.getElementById("graphs").style.display = "block";
        } 
        for (const objeto_ of objeto_armazenamento) {
          for (let submercado of Object.keys(objeto_)) {
            let decomp = [];
            for (let chave of Object.keys(objeto_[submercado])) {
              const dateString = chave;   
              const year = parseInt(dateString.substr(0, 4));
              const month = parseInt(dateString.substr(4, 2)) - 1;
              const day = parseInt(dateString.substr(6, 2));
              const hours = parseInt(dateString.substr(8, 2));
              const minutes = parseInt(dateString.substr(11, 2));
              const date = new Date(year, month, day, hours, minutes);
              decomp.push({ t: date, y: objeto_[submercado][chave] });
            } 
          var datasArray = datasOrdenadas.split(",");
          var datasOrdenadasDecomp = datasArray.sort(compararDatas);
          const labelDataFormatada = datasOrdenadasDecomp.map(data => {
            const year = data.substr(0, 4);
            const month = data.substr(4, 2);
            const day = data.substr(6, 2);
            return `${day}/${month}/${year}`;
          });
            let labelDecomp = labelDataFormatada[indexColor];
            let newDataset = {
              label: `DC - ínicio deck: ${labelDataFormatada}`,
              type: 'line',
              backgroundColor: getColor_byModeloCargas("DECOMP")[indexColor],
              borderColor: getColor_byModeloCargas("DECOMP")[indexColor],
              fill: false,
              data: decomp,
              yAxisID: 'eixo_carga',
              steppedLine: 'true',
              pointRadius: 1,
              borderWidth: 1,
            };
            charts[submerc[submercado]].options['scales']['xAxes'] = [{
              type: 'time',
              time: {
                unit: 'day',
                displayFormats: {
                  day: 'DD MMM YYYY',
              },
              tooltipFormat: "DD MMM YYYY HH:mm",
              },
              scaleLabel: {
                display: true,
            }
  }];
            charts[submerc[submercado]].data.datasets.push(newDataset);
            charts[submerc[submercado]].update();
          }
          indexColor = indexColor + 1
        }
    },
    error: function() {
      const ano = datasOrdenadas.substr(0, 4);
      const mes = datasOrdenadas.substr(4, 2);
      const dia = datasOrdenadas.substr(6, 2);
      const dataErroFormatada = `${dia}/${mes}/${ano}`;
      alert("A data: " + dataErroFormatada + " para carga DECOMP, não está disponivel no banco de dados");
      modalLoading.style.display = "none";
  }
  });
}
});


// Evento de clique para o botão "Plotar Newave"
$("#btnPlotarNewave").click(function() {
  // limparEAtualizarListaSabados()
  for (const submercado of Object.values(submerc)) {
    charts[submercado].data.datasets = [];
    charts[submercado].update();
  }
  listaDeDatasNewave = [];
  const selectedOptions = Array.from(document.querySelectorAll(".options-list.newave li.selected"));
  selectedOptions.forEach(option => {
    listaDeDatasNewave.push(option.getAttribute("data-value"));
  });
  listaDeDatasNewave.push(selectedOptionsDivNewave.textContent);
  var dataDeckParamNewave = converterListaDeDatas(listaDeDatasNewave);
  datasOrdenadasNewave = listaDeDatasNewave.sort(compararDatas);
  const datasFormatadasNewave = datasOrdenadasNewave.map(data => {
    const [dia, mes, ano] = data.split('/');
    return `${ano}${mes}${dia}`;
  });
// Função para remover datas duplicadas
function removerDatasDuplicadas(lista) {
  const setDeDatas = new Set(); // Usando um conjunto para manter datas únicas
  lista.forEach(item => {
      const datas = item.split(',').map(dateStr => dateStr.trim()); // Dividir as datas separadas por vírgula
      datas.forEach(dateStr => {
          setDeDatas.add(dateStr);
      });
  });
  return Array.from(setDeDatas);
}
const datasUnicas = removerDatasDuplicadas(datasOrdenadasNewave);

// Função para formatar e ordenar as datas para fazer requisição
function formatarEOrdenarDatas(lista) {
  const datasFormatadas = lista.map(data => {
      const [dia, mes, ano] = data.split('/');
      return `${ano}${mes}${dia}`;
  });
  datasFormatadas.sort(); // Ordena as datas
  return datasFormatadas;
}
const datasOrdenadas = formatarEOrdenarDatas(datasUnicas);

var indexColor = 0
for (const data of datasOrdenadas) {
  fazerRequisicao(data);
}


function fazerRequisicao(datasOrdenadas) {
  $.ajax({
    contentType: "application/json; charset=utf-8",
    dataType: "json",
    method: "GET",
    url: `/middle/API/GET/carga_newavePatamar?dataDeck=${datasOrdenadas}`,
    success: function(objeto) {  
        objeto_armazenamento = objeto;
        const objeto_ = objeto_armazenamento;
        var dataEscolhida = {}
        if (objeto && objeto.length > 0) {
          document.getElementById("graphs").style.display = "block";
        } 
        for (const objeto_ of objeto_armazenamento) {
          for (let submercado of Object.keys(objeto_)) {
            let newave = [];
            for (let chave of Object.keys(objeto_[submercado])) {
              const dateString = chave;   
              const year = parseInt(dateString.substr(0, 4));
              const month = parseInt(dateString.substr(4, 2)) - 1;
              const day = parseInt(dateString.substr(6, 2));
              const date = new Date(year, month, day);
              newave.push({ t: date, y: objeto_[submercado][chave] });
            }
          var datasArray = datasOrdenadas.split(",");
          var datasOrdenadasNewave = datasArray.sort(compararDatas);
          const labelDataFormatada = datasOrdenadasNewave.map(data => {
            const year = data.substr(0, 4);
            const month = data.substr(4, 2);
            const day = data.substr(6, 2);
            return `${day}/${month}/${year}`;
          });
            let labelNewave = labelDataFormatada[indexColor];
            let newDataset = {
              label: `NW - ínicio deck: ${labelDataFormatada}`,
              type: 'line',
              backgroundColor: getColor_byModeloCargas("NEWAVE")[indexColor],
              borderColor: getColor_byModeloCargas("NEWAVE")[indexColor],
              fill: false,
              data: newave,
              yAxisID: 'eixo_carga',
              steppedLine: 'true',
              pointRadius: 1,
              borderWidth: 1,
            };
            charts[submerc[submercado]].options['scales']['xAxes'] = [{
              type: 'time',
              time: {
                unit: 'month',
                displayFormats: {
                  day: 'DD MMM',
              },
              tooltipFormat: "DD MMM",
              },
              scaleLabel: {
                display: true,           
            }
  }];
            charts[submerc[submercado]].data.datasets.push(newDataset);
            charts[submerc[submercado]].update();
          }
          indexColor = indexColor + 1
        }
    },
    error: function() {
      const ano = datasOrdenadas.substr(0, 4);
      const mes = datasOrdenadas.substr(4, 2);
      const dia = datasOrdenadas.substr(6, 2);
      const dataErroFormatada = `${dia}/${mes}/${ano}`;
      alert("A data: " + dataErroFormatada + " para carga NEWAVE, não está disponivel no banco de dados");
      modalLoading.style.display = "none";
  }
  });
}
});

const listaDeDatasSelecionadasDS = [];
let datasOrdenadasDs = [];

// Adicione um ouvinte de eventos para detectar mudanças no campo de entrada de data
dataInput.addEventListener('change', () => {
  const dataEscolhidaDS = dataInput.value;
  if (!listaDeDatasDessem.includes(dataEscolhidaDS)) {
    listaDeDatasDessem.push(dataEscolhidaDS);
  }
  // Verifique se a data já está na lista
  if (!listaDeDatasSelecionadasDS.includes(dataEscolhidaDS)) {
    listaDeDatasSelecionadasDS.push(dataEscolhidaDS);
  }

  
// Função para remover datas duplicadas
function removerDatasDuplicadas(listaDS) {
  const setDeDatas = new Set(); // Usando um conjunto para manter datas únicas
  listaDS.forEach(item => {
      const datas = item.split(',').map(dateStr => dateStr.trim()); // Dividir as datas separadas por vírgula
      datas.forEach(dateStr => {
          setDeDatas.add(dateStr);
      });
  });
  return Array.from(setDeDatas);
}
const datasUnicasDS = removerDatasDuplicadas(listaDeDatasDessem);
// Função para formatar e ordenar as datas para fazer requisição
function formatarEOrdenarDatas(listaDS) {
  const datasFormatadas = listaDS.map(data => {
      const [ano, mes, dia] = data.split('-');
      return `${ano}${mes}${dia}`;
  });
  datasFormatadas.sort(); // Ordena as datas
  return datasFormatadas;
}
datasOrdenadasDs = formatarEOrdenarDatas(datasUnicasDS);

const datasFormatadasDs = datasUnicasDS.map(data => {
  const [ano, mes, dia] = data.split('-');
  return `${dia}/${mes}/${ano}`; });

function atualizarSelectedOptionsDessem() {
  selectedOptionsDessem.innerHTML = datasFormatadasDs.join(', ');
  // Exibe a div se houver datas selecionadas, caso contrário, a mantém oculta
  selectedOptionsDessem.style.display = datasOrdenadasDs.length > 0 ? 'block' : 'none';
}
atualizarSelectedOptionsDessem(); // Atualiza a exibição das datas selecionadas
});

// Evento de clique para o botão "Plotar Dessem"
$("#btnPlotarDessem").click(function() {
  for (const submercado of Object.values(submerc)) {
    charts[submercado].data.datasets = [];
    charts[submercado].update();
  }
var indexColor = 0
for (const data of datasOrdenadasDs) {
  fazerRequisicao(data);
}

function fazerRequisicao(datasOrdenadasDs) {
  $.ajax({
    contentType: "application/json; charset=utf-8",
    dataType: "json",
    method: "GET",
    url: `/middle/API/GET/carga_dessemPatamar?dataDeck=${datasOrdenadasDs}`,
    success: function(objeto) {  
        objeto_armazenamento = objeto;
        const objeto_ = objeto_armazenamento;
        var dataEscolhida = {}
        if (objeto && objeto.length > 0) {
          document.getElementById("graphs").style.display = "block";
        } 
        for (const objeto_ of objeto_armazenamento) {
          for (let submercado of Object.keys(objeto_)) {
            let dessem = [];
            for (let chave of Object.keys(objeto_[submercado])) {
              const dateString = chave;   
              const year = parseInt(dateString.substr(0, 4));
              const month = parseInt(dateString.substr(4, 2)) - 1;
              const day = parseInt(dateString.substr(6, 2));
              const hours = parseInt(dateString.substr(8, 2));
              const minutes = parseInt(dateString.substr(11, 2));
              const date = new Date(year, month, day, hours, minutes);
              dessem.push({ t: date, y: objeto_[submercado][chave] });
            } 
          var datasArray = datasOrdenadasDs.split(",");
          var datasOrdenadasDessem = datasArray.sort(compararDatas);
          const labelDataFormatada = datasOrdenadasDessem.map(data => {
            const year = data.substr(0, 4);
            const month = data.substr(4, 2);
            const day = data.substr(6, 2);
            return `${day}/${month}/${year}`;
          });
            let labelDecomp = labelDataFormatada[indexColor];
            let newDataset = {
              label: `DS - ínicio deck: ${labelDataFormatada}`,
              type: 'line',
              backgroundColor: getColor_byModeloCargas("DESSEM")[indexColor],
              borderColor: getColor_byModeloCargas("DESSEM")[indexColor],
              fill: false,
              data: dessem,
              yAxisID: 'eixo_carga',
              steppedLine: 'true',
              pointRadius: 1,
              borderWidth: 1,
            };
            charts[submerc[submercado]].options['scales']['xAxes'] = [{
              type: 'time',
              time: {
                unit: 'day',
                displayFormats: {
                  day: 'DD MMM YYYY',
              },
              tooltipFormat: "DD MMM YYYY HH:mm",
              },
              scaleLabel: {
                display: true,        
            }
  }];
            charts[submerc[submercado]].data.datasets.push(newDataset);
            charts[submerc[submercado]].update();
          }
          indexColor = indexColor + 1
        }
    },
    error: function() {
      const ano = datasOrdenadasDs.substr(0, 4);
      const mes = datasOrdenadasDs.substr(4, 2);
      const dia = datasOrdenadasDs.substr(6, 2);
      const dataErroFormatada = `${dia}/${mes}/${ano}`;
      alert("A data: " + dataErroFormatada + " para carga DESSEM, não está disponivel no banco de dados");
      modalLoading.style.display = "none";
  }
  });
}
});


const optionsContainerDessem = document.querySelector(".options-container.dessem");
const optionsContainerDecomp = document.querySelector(".options-container.decomp");
const optionsContainerNewave = document.querySelector(".options-container.newave");
const myInputLabelDessem = document.querySelector(".my-input-label.dessem");
const myInputLabelDecomp = document.querySelector(".my-input-label.decomp");
const myInputLabelNewave = document.querySelector(".my-input-label.newave");
const selectedOptionsDessem = document.querySelector('.selected-options.dessem');
const selectedOptionsDivDecomp = document.querySelector(".selected-options.decomp");
const selectedOptionsDivNewave = document.querySelector(".selected-options.newave");

// Evento de clique para os itens da lista
const optionsListDecomp = document.querySelector(".options-list.decomp").getElementsByTagName("li");
const optionsListNewave = document.querySelector(".options-list.newave").getElementsByTagName("li");

// Variável para rastrear se o myInput foi clicado
let myInputClicked = false;
// Array para armazenar os itens selecionados
const selectedItemsDessem = [];
const selectedItemsDecomp = [];
const selectedItemsNewave = [];

function toggleOption(event) {
  const option = event.target;
  if (option.classList.contains("selected")) {
      option.classList.remove("selected");
      removeSelectedOption(option.getAttribute("data-value"));
  } else {
      option.classList.add("selected");
      addSelectedOption(option.textContent, option.getAttribute("data-value"));
  }
}

let btnDessemClicked = false;
let btnDecompClicked = false;
let btnNewaveClicked = false;

// Evento de clique para o botão limpar dados dessem
const btnLimparDessem = document.getElementById("btnLimparDessem");
btnLimparDessem.addEventListener("click", () => {
    selectedOptionsDessem.innerHTML = "lista de data vazia, adicione novas datas";
    listaDeDatasDessem = []
});

// Evento de clique para o botão btnDessem
const btnDessem = document.getElementById("btnDessem");
btnDessem.addEventListener("click", () => {
    myInputLabelNewave.style.display = "none"; // Ocultar o rótulo e a caixa de entrada
    optionsContainerNewave.style.display = "none"; // Ocultar o container de opções
    selectedOptionsDivNewave.style.display = "none";// Ocultar a selected-options
    toggleButtonBehavior(optionsContainerDessem);
});

// Evento de clique para o botão btnDecomp
const btnDecomp = document.getElementById("btnDecomp");
btnDecomp.addEventListener("click", () => {
    myInputLabelNewave.style.display = "none"; // Ocultar o rótulo e a caixa de entrada
    optionsContainerNewave.style.display = "none"; // Ocultar o container de opções
    selectedOptionsDivNewave.style.display = "none";// Ocultar a selected-options
    toggleButtonBehavior(optionsContainerDecomp);
});

btnDecomp.addEventListener("click", () => {
  if (!btnDecompClicked) {
      myInputLabelDecomp.style.display = "block"; // Mostrar o rótulo e a caixa de entrada
      optionsContainerDecomp.style.display = "block"; // Mostrar o container de opções

      if (selectedOptionsDivDecomp.innerHTML.trim() !== "") {
        // O elemento tem conteúdo interno
        // Mostrar a selected-options
        selectedOptionsDivDecomp.style.display = "block";
    } else {
        // O elemento está vazio
        // Ocultar a selected-options
        selectedOptionsDivDecomp.style.display = "none";
    }
      // Atualizar a variável para indicar que o botão foi clicado
      btnDecompClicked = true;
  } else {
      myInputLabelDecomp.style.display = "none"; // Ocultar o rótulo e a caixa de entrada
      optionsContainerDecomp.style.display = "none"; // Ocultar o container de opções
      // Ocultar a selected-options
      selectedOptionsDivDecomp.style.display = "none";
      // Redefinir a variável para indicar que o botão não foi clicado
      btnDecompClicked = false;
  }
});



// Evento de clique para o botão btnNewave
const btnNewave = document.getElementById("btnNewave");
btnNewave.addEventListener("click", () => {
    // limparEAtualizarListaSabados()
    myInputLabelDecomp.style.display = "none"; // Ocultar o rótulo e a caixa de entrada
    optionsContainerDecomp.style.display = "none"; // Ocultar o container de opções
    selectedOptionsDivDecomp.style.display = "none";// Ocultar a selected-options
    myInputLabelDessem.style.display = "none"; // Ocultar o rótulo e a caixa de entrada
    optionsContainerDessem.style.display = "none"; // Ocultar o container de opções
    selectedOptionsDessem.style.display = "none";// Ocultar a selected-options
    toggleButtonBehavior(optionsContainerNewave);
});
btnNewave.addEventListener("click", () => {
    if (!btnNewaveClicked) {
      myInputLabelNewave.style.display = "block"; // Mostrar o rótulo e a caixa de entrada
        optionsContainerNewave.style.display = "block"; // Mostrar o container de opções
      
        if (selectedOptionsDivNewave.innerHTML.trim() !== "") {
          // O elemento tem conteúdo interno
          // Mostrar a selected-options
          selectedOptionsDivNewave.style.display = "block";
      } else {
          // O elemento está vazio
          // Ocultar a selected-options
          selectedOptionsDivNewave.style.display = "none";
      }
        // Atualizar a variável para indicar que o botão foi clicado
        btnNewaveClicked = true;
    } else {
      myInputLabelNewave.style.display = "none"; // Ocultar o rótulo e a caixa de entrada
        optionsContainerNewave.style.display = "none"; // Ocultar o container de opções
        // Ocultar a selected-options
        selectedOptionsDivNewave.style.display = "none";
        // Redefinir a variável para indicar que o botão não foi clicado
        btnNewaveClicked = false;
    }
});



// Função para alternar o comportamento do botão
function toggleButtonBehavior(optionsContainer) {
  const myInputLabelDecomp = document.querySelector(".my-input-label.decomp");
  const myInputLabelNewave = document.querySelector(".my-input-label.newave");
  if (!optionsContainer.classList.contains("active")) {
      myInputLabelDecomp.style.display = "block"; // Mostrar o rótulo e a caixa de entrada
      myInputLabelNewave.style.display = "block"; // Mostrar o rótulo e a caixa de entrada
      optionsContainer.style.display = "block"; // Mostrar o container de opções
      optionsContainer.classList.add("active"); // Adicionar classe para indicar que o botão foi clicado
  } else {
      myInputLabelDecomp.style.display = "none"; // Ocultar o rótulo e a caixa de entrada
      myInputLabelNewave.style.display = "none"; // Ocultar o rótulo e a caixa de entrada
      optionsContainer.style.display = "none"; // Ocultar o container de opções
      optionsContainer.classList.remove("active"); // Remover classe para indicar que o botão não foi clicado
  }
}


// Funcão que cria uma lista com as datas dos ultimos sabados de cada mes do ano.
function primeiroDiaMesSeguinte(data) {
  const dataMesSeguinte = new Date(data);
  dataMesSeguinte.setMonth(data.getMonth() + 1);
  dataMesSeguinte.setDate(1);
  return dataMesSeguinte;
}

function encontrarSabadoAnterior(data) {
  const dataAnterior = new Date(data);
  while (dataAnterior.getDay() !== 6) { // 6 representa sábado (0 é domingo, 1 é segunda-feira, ..., 6 é sábado)
    dataAnterior.setDate(dataAnterior.getDate() - 1);
  }
  return dataAnterior;
}

function obterUltimoSabadoDoMes(data) {
  const primeiroDiaSeguinte = primeiroDiaMesSeguinte(data);
  if (primeiroDiaSeguinte.getDay() === 6) { // Verifica se o primeiro dia do mês seguinte é um sábado
    return primeiroDiaSeguinte;
  } else {
    return encontrarSabadoAnterior(primeiroDiaSeguinte);
  }
}

// Evento de alteração no selectAno
selectAno.addEventListener("change", function(){
  if (selectAno.value !== "") {
    divModelos.style.display = "block";
    var anoEscolhido = selectAno.value;
    listaTodosSabados = listarTodosSabadosDoMesDoAnoDesejado(anoEscolhido);
    listaUltimosSabados = listarUltimosSabadosDoAno(anoEscolhido);
    limparEAtualizarListaSabados();
} else {
    divModelos.style.display = "none";
}
});


function limparEAtualizarListaSabados() {
  const listaContainerDecomp = document.querySelector(".options-list.decomp");
  const listaContainerNewave = document.querySelector(".options-list.newave");
  listaContainerDecomp.innerHTML = "";
  listaContainerNewave.innerHTML = "";
  criarListaDinamicaDecomp();
  criarListaDinamicaNewave();
}


function listarUltimosSabadosDoAno(ano) {
  const datasUltimosSabados = [];
  const dataOntem = new Date(); // Obtém a data atual
  dataOntem.setDate(dataOntem.getDate() - 1);
  
  for (let mes = 0; mes < 12; mes++) {
    const dataInicial = new Date(ano, mes, 1);
    const ultimoSabado = obterUltimoSabadoDoMes(dataInicial);
    // Verifica se o último sábado do mês é maior ou igual a dataOntem
    if (ultimoSabado <= dataOntem) {
      const dia = ultimoSabado.getDate().toString().padStart(2, '0');
      const mesFormatado = (mes + 1).toString().padStart(2, '0');
      const dataFormatada = `${dia}/${mesFormatado}/${ano}`;
      datasUltimosSabados.push(dataFormatada);
    }
  }
  return datasUltimosSabados;
}


// Função para gerar uma lista de todos os sabados existentes no mes de um ano especifico
function listarTodosSabadosDoMesDoAnoDesejado(ano) {
  const datasTodosSabados = [];
  const dataAtual = new Date(); // Obtém a data atual
  const dataOntem = new Date(dataAtual); // Cria uma cópia da data atual
  dataOntem.setDate(dataAtual.getDate() - 1);
  for (let mes = 0; mes < 12; mes++) {
    const dataInicial = new Date(ano, mes, 1);
    const ultimoDiaDoMes = new Date(ano, mes + 1, 0).getDate(); // Último dia do mês
    for (let dia = 1; dia <= ultimoDiaDoMes; dia++) {
      const data = new Date(ano, mes, dia);
      if (data.getDay() === 6) { // 6 representa sábado (0 é domingo, 1 é segunda-feira, ..., 6 é sábado)
        if (data >= dataOntem) {
          continue; // Se for anterior, pule para a próxima iteração do loop
        }
        const diaFormatado = dia.toString().padStart(2, '0');
        const mesFormatado = (mes + 1).toString().padStart(2, '0'); // O mês é base 0, então adicionamos 1 e formatamos com dois dígitos
        const dataFormatada = `${diaFormatado}/${mesFormatado}/${ano}`;
        datasTodosSabados.push(dataFormatada);
      }
    }
  }
  return datasTodosSabados;
}


// Função para criar a lista dinâmica decomp
function criarListaDinamicaDecomp() {
  // Obter a referência do elemento HTML onde a lista será adicionada
  const listaContainerDecomp = document.querySelector(".options-list.decomp");
  // Definir estilo CSS para remover marcador de pontos
  listaContainerDecomp.style.listStyleType = "none";
  // Número inicial para os atributos data-value
  let valorAtual = 1;
  // Loop pelos valores e criar elementos <li> (itens da lista) para cada valor
  for (const dias of listaTodosSabados) {
    const li = document.createElement("li");
    li.textContent = dias; // Define o texto do item como o valor atual
    li.setAttribute("data-value", valorAtual); // Adicione o atributo data-value com o valor atual
    // Adicione um identificador único aos elementos <li> (por exemplo, um ID baseado no valor)
    li.setAttribute("id", "item-" + valorAtual);
    listaContainerDecomp.appendChild(li); // Adiciona o item à lista
    // Incrementa o valor atual para o próximo elemento
    valorAtual++;
    // Adicione um evento de clique a cada elemento <li>
    li.addEventListener("click", function (event) {
    const selectedText = li.textContent;
  if (li.classList.contains("selected-item")) {
    // Se o elemento já tiver a classe "selected-item", remova-a (clique para desselecionar)
    li.classList.remove("selected-item");
    const indexDecomp = selectedItemsDecomp.indexOf(selectedText);
    if (indexDecomp !== -1) {
      selectedItemsDecomp.splice(indexDecomp, 1);
    }
  } else {
    // Se o elemento não tiver a classe "selected-item", adicione-a (clique para selecionar)
    li.classList.add("selected-item");
    if (selectedItemsDecomp.indexOf(selectedText) === -1) {
      selectedItemsDecomp.push(selectedText);
    }
  }
      // Atualiza o conteúdo da div selected-options com os itens selecionados
      if (selectedItemsDecomp.length > 0) {
        selectedOptionsDivDecomp.textContent = `${selectedItemsDecomp.join(", ")}`;
        selectedOptionsDivDecomp.style.display = "block";
      } else {
        selectedOptionsDivDecomp.style.display = "none";
        selectedOptionsDivDecomp.innerHTML = "";
      }
      return selectedItemsDecomp
    });
  }
}


// Função para criar a lista dinâmica
function criarListaDinamicaNewave() {
  // Obter a referência do elemento HTML onde a lista será adicionada
  const listaContainerNewave = document.querySelector(".options-list.newave");
  // Definir estilo CSS para remover marcador de pontos
  listaContainerNewave.style.listStyleType = "none";
  // Número inicial para os atributos data-value
  let valorAtual = 1;
  // Loop pelos valores e criar elementos <li> (itens da lista) para cada valor
  for (const dias of listaUltimosSabados ) {
    const li = document.createElement("li");
    li.textContent = dias; // Define o texto do item como o valor atual
    li.setAttribute("data-value", valorAtual); // Adicione o atributo data-value com o valor atual
    // Adicione um identificador único aos elementos <li> (por exemplo, um ID baseado no valor)
    li.setAttribute("id", "item-" + valorAtual);
    listaContainerNewave.appendChild(li); // Adiciona o item à lista
    // Incrementa o valor atual para o próximo elemento
    valorAtual++;
    // Adicione um evento de clique a cada elemento <li>
    li.addEventListener("click", function (event) {
      // Obter o texto dentro do elemento <li> clicado
      const selectedText = li.textContent;
      if (li.classList.contains("selected-item")) {
        // Se o elemento já tiver a classe "selected-item", remova-a (clique para desselecionar)
        li.classList.remove("selected-item");
        const indexNewave = selectedItemsNewave.indexOf(selectedText);
        if (indexNewave !== -1) {
          selectedItemsNewave.splice(indexNewave, 1);
        }
      } else {
        // Se o elemento não tiver a classe "selected-item", adicione-a (clique para selecionar)
        li.classList.add("selected-item");
        if (selectedItemsNewave.indexOf(selectedText) === -1) {
          selectedItemsNewave.push(selectedText);
        }
      }
      // Atualiza o conteúdo da div selected-options com os itens selecionados
      if (selectedItemsNewave.length > 0) {
        selectedOptionsDivNewave.textContent = `${selectedItemsNewave.join(", ")}`;
        selectedOptionsDivNewave.style.display = "block";
      } else {
        selectedOptionsDivNewave.style.display = "none";
        selectedOptionsDivNewave.innerHTML = "";

      }
      return selectedItemsNewave
    });
  }
}



// ############################## CRIANDO GRAFICOS CHART JS, CONFIGURAÇÕES ############################## //



//configurando o grafico, lembrando que a configuração do xAxes(legenda do X) é feita dentro de cada ajax para conseguir deixar diferente dependendo do modelo escolhido.
gradientChartOptionsConfiguration = {
  responsive: true,
  legend: {
    display:true,
    position: 'bottom',
},
scales:   {
    yAxes: [
                {
                id: 'eixo_carga',
                type: 'linear',
                position: 'left',
                scaleLabel: {
                  display: true,
                  labelString: 'VOLUME (MWh)',
                  fontSize: 20,
                },
                gridLines: {
                  drawOnChartArea: true,
                },
              },
  ],
  xAxes: []
}
};

gradientChartOptionsConfiguration.tooltips = {
  callbacks: {
    beforeLabel: function (tooltipItem, data) {
      const datasetIndex = tooltipItem.datasetIndex;
      const dataIndex = tooltipItem.index;
      const value = data.datasets[datasetIndex].data[dataIndex].y;
      return `carga: ${value} MWh`;
    },
    label: function (tooltipItem, data) {
      const datasetLabel = data.datasets[tooltipItem.datasetIndex].label || '';
      return `${datasetLabel}`;
    }
  }
};

var charts = {}
var chartVazao = {}
var graphs = document.getElementById("graphs")
var keyOrder

function create_chart(submercado){
  for (submercado of Object.values(submerc)){
      var div_aux = document.createElement("div");
      div_aux.classList = "mt-5 card rounded"
      var canvas_aux = document.createElement("canvas");
      canvas_aux.id = "chart"+submercado
      canvas_aux.setAttribute("height", 120);
      div_aux.appendChild(canvas_aux);
      graphs.appendChild(div_aux);
      var ctx = document.getElementById("chart"+submercado).getContext("2d");
      ctx.canvas.width = $(window).width();
      ctx.canvas.height = $(window).height()*.8;
      
      gradientChartOptionsConfiguration['title'] = {
        display: true,
        text: submercado,
        fontSize: 20
      }
      
      charts[submercado] = new Chart(ctx ,{
        type: 'bar',
        options:gradientChartOptionsConfiguration,  
      })
      chartVazao[submercado] = []
    }
  }
  
