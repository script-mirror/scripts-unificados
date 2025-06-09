var botoes

let btnGerarPrevs = document.getElementById("btnGerarPrevs");
let anoDesejadoInput = document.getElementById("inputAnoDesejado");
let anoBaseInput = document.getElementById("inputAnoBase");

$(window).on('load', function(){
    $('[data-toggle="tooltip"]').tooltip()
    // Remove createSelect() and createBtnAno() calls
})


anoDesejadoInput.addEventListener("change", function(){
  var anoDesejadoValue = anoDesejadoInput.value;

  // checando se é um numero e se ele esta entre o ano 1930 ate 2100
  if (isNumeric(anoDesejadoValue) && anoDesejadoValue >= 1930 && anoDesejadoValue <= 2100) {
    $("#btnGerarPrevs").prop("disabled", false);
    document.getElementById("btnGerarPrevs").addEventListener("click", fazerRequisicoesEmSerie);
    // Create select and buttons based on user input
    createSelect(parseInt(anoDesejadoValue));
    createBtnAno(parseInt(anoDesejadoValue));
  } else {
    $("#btnGerarPrevs").prop("disabled", true);
    // Clear select and buttons if invalid input
    clearYearElements();
  }
});

// Add event listener for base year input
anoBaseInput.addEventListener("change", function(){
  var anoDesejadoValue = anoDesejadoInput.value;
  if (isNumeric(anoDesejadoValue) && anoDesejadoValue >= 1930 && anoDesejadoValue <= 2100) {
    createSelect(parseInt(anoDesejadoValue));
    createBtnAno(parseInt(anoDesejadoValue));
  }
});

document.getElementById('btnIniciarGerador').addEventListener('click', function () {
  document.querySelector('.paginaCompleta').style.display = 'block';
  $.ajax({
    type: "GET",
    url: "/middle/API/remove/geradorPrevs",
    success: function (data) { 
    },
    error: function () {
      reject("Erro, ao remover.");
    }
  });
});

// Function to check if a value is numeric
function isNumeric(value) {
  return !isNaN(parseFloat(value)) && isFinite(value);
}


btnAnosList = document.getElementById('Anos')
btnAnosList.addEventListener("click", function () {
  anos = []
  anos_aux = []
  var dt_inicial = btnAnosList.value
  var dt_final = parseInt(anoDesejadoInput.value) - 1; // Use user input instead of current year
  var dt_aux = dt_inicial
  while(dt_aux<=dt_final){
    anos.push(dt_aux)
    dt_aux = parseInt(dt_aux) + 1
    dt_aux = dt_aux.toString()
  }

  removeAllSuccess()
  botoes = document.querySelectorAll('.btn-anos'); // Update botoes reference
  botoes.forEach(b => {
      if (anos.includes(b.textContent)) {
          b.classList.remove('btn-light')
          b.classList.add('btn-success');
          anos_aux.push(b.textContent)
      }
    })
})


const btnListDiv = document.getElementById("btnListAnos")
var selectYear = document.getElementById('Anos')
function createSelect(anoDesejado){
  // Clear existing options
  selectYear.innerHTML = '';
  
  var anoBaseValue = anoBaseInput.value;
  dt_inicial = (isNumeric(anoBaseValue) && anoBaseValue >= 1930) ? parseInt(anoBaseValue) : 2014;
  dt_final = anoDesejado - 1; // Use user input year
  var dt_aux = dt_final
  
  while(dt_aux >= dt_inicial){
    let option = document.createElement('option');
    option.value = dt_aux
    option.textContent = dt_aux;
    selectYear.appendChild(option);
    dt_aux = dt_aux-1
  }
}

var btnDiv = document.getElementById("btnAnos")

function createBtnAno(anoDesejado){
  // Clear existing buttons
  btnDiv.innerHTML = '';
  
  var anoBaseValue = anoBaseInput.value;
  dt_inicial = (isNumeric(anoBaseValue) && anoBaseValue >= 1930) ? parseInt(anoBaseValue) : 2014;
  dt_final = anoDesejado; // Use user input year

  var dt_aux = dt_final
  while(dt_aux >= dt_inicial){
    var btn_aux = document.createElement("button");
    btn_aux.type = 'button';
    btn_aux.classList.add('btn', 'btn-light', 'btn-anos','mb-1','ml-1');
    btn_aux.textContent  = dt_aux
    btnDiv.appendChild(btn_aux)
    dt_aux = dt_aux-1
  }
  
  // Update botoes reference after creating new buttons
  botoes = document.querySelectorAll('.btn-anos');
}

// Add function to clear year elements
function clearYearElements() {
  document.getElementById('Anos').innerHTML = '';
  document.getElementById('btnAnos').innerHTML = '';
  anos = [];
  anos_aux = [];
}

var anos=[]
var anos_aux = []
btnDiv.addEventListener('click', (event) => {
  if (event.target.classList.contains('btn-anos')) {

    if(event.target.classList.contains('btn-light')){
    event.target.classList.remove('btn-light')
    event.target.classList.add('btn-success');
    anos_aux.push(event.target.textContent)
    }

    else if(event.target.classList.contains('btn-success')){
      event.target.classList.remove('btn-success')
      event.target.classList.add('btn-light');
      anos_aux = anos.filter(item => item !== event.target.textContent)
    }
    anos = anos_aux
  }
});


function removeAllSuccess(){
// Update botoes reference before using
botoes = document.querySelectorAll('.btn-anos');
// Adiciona um ouvinte de evento de clique para cada botão
botoes.forEach(b => b.classList.remove('btn-success'));
botoes.forEach(b => b.classList.add('btn-light'));

}

let completedRequests = 0;

async function fazerRequisicoesEmSerie() {

  document.getElementById('loading-spinner').style.display = 'block';
  document.getElementById('loading').style.display = 'block';
  for (let i = 0; i < anos_aux.length; i++) {
    try {
      const result = await fazerRequisicao(anoDesejadoInput.value, anos_aux[i]);
      console.log(result); 
    } catch (error) {
      console.error(error);
    } finally {
      completedRequests++;

      // Checando se a request foi terminada
      if (completedRequests === anos_aux.length) {

        document.getElementById('loading-spinner').style.display = 'none';
        document.getElementById('loading').style.display = 'none';

        completedRequests = 0;

        $("#btn-download").show();
      }
    }
    await sleep(3000); // 3 segundos de timmer para rodar o proximo
  }
}

function fazerRequisicao(anoDesejado, anoComparativo) {
  return new Promise((resolve, reject) => {
    $.ajax({
      type: "GET",
      url: "/middle/API/GET/geradorPrevs",
      data: { anoDesejado: anoDesejado, anoComparativo: anoComparativo},
      success: function (data) {
        resolve(data);
      },
      error: function () {
        reject("Erro, verificar dados inseridos.");
      }
    });
  });
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Função para realizar o download
function realizarDownload() {
  $.ajax({
    type: "GET",
    url: "/middle/API/download/geradorPrevs",
    success: function(data, textStatus, xhr) {
      if (xhr.status === 200 ) {
        window.location.href = "/middle/API/download/geradorPrevs";
      } else if (xhr.status === 404) {
        alert("Nenhum arquivo disponível para download.");
      } else {
        alert("Erro ao fazer download.");
      }
    },
    error: function(xhr, status, error) {
      console.error("Erro ao fazer download do arquivo:", error);
      alert("Erro ao fazer download.");
      $("#btnGerarPrevs").prop("disabled", true);
    },
    complete: function() {
      setTimeout(function() {
        alert('Download concluído com sucesso!!')
        location.reload(); 
      }, 3500);
    }
  });
}

// esconder botao download e desativar botao gerar prevs
$("#btn-download").hide();
$("#btnGerarPrevs").prop("disabled", true);

// Atribuir a função de download ao clique do botão de download
$("#btn-download").click(realizarDownload);

