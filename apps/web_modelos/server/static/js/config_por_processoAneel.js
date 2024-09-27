var inputProcesso = document.getElementById("inputProcesso");
var inputEfeito = document.getElementById("inputEfeito");
var inputAssunto = document.getElementById("inputAssunto");

var inputRelator = document.getElementById("inputRelator");
var inputSuperintendencia = document.getElementById("inputSuperintendencia");

let botaoAdicionar = document.getElementById("btn-Adicionar");
let corpoTab = document.getElementById("corpoTabela");
let botaoSalvar = document.getElementById("btn-Salvar");
var objetoPalavrasSelecionadas = {};
var index = 0;
let btnSwitch_processos = document.getElementById("nav-processos")





// MODIFICAR CELULAS

var table = document.getElementById('myTable');


// table.addEventListener('click', function(event) {
  // var target_click = event.target;
  // var originalText = target_click.innerText;

  table.addEventListener('input', function(event) {


    var target = event.target;
    var index_coluna = target.cellIndex
    var modifiedText = target.innerText;
    var index_objeto = target.parentNode.rowIndex -1
    var nome_da_coluna = table.getElementsByTagName('th')[index_coluna].innerText.toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '')
    console.log(nome_da_coluna)
    objetoPalavrasSelecionadas[index_objeto][nome_da_coluna] = modifiedText
    index_objeto

    // for (var i=0; i < objetoPalavrasSelecionadas.length;i++){
        
    //   if(objetoPalavrasSelecionadas[i][nome_da_coluna] == originalText){
        
    //     objetoPalavrasSelecionadas[i][nome_da_coluna] = modifiedText

    //   }
    //     }
    // originalText = modifiedText

  });

// });



$.ajax({
  method: "GET",
  url: "/middle/API/get/processos-Aneel",
  data: {},
}).done(function (objeto) {
  for (let idx in objeto) {
    row = objeto[idx];
    for(item in row){
      if (row[item] == null || row[item] == undefined){
        row[item] = "-"
      }
    }
    adicionaLinha(row["id"], row["processo"], row["efeito"], row["assunto"],row["relator"],row["superintendencia"], row["ultimo_documento"]);
    index = row["id"];
  }
  objetoPalavrasSelecionadas = objeto;
});

botaoSalvar.addEventListener("click", function () {
  dataToSend = JSON.stringify(objetoPalavrasSelecionadas);

  $.ajax({
    contentType: "application/json; charset=utf-8",
    dataType: "json",
    type: "POST",
    url: "/middle/API/set/processos-Aneel",
    data: dataToSend,
    success: function (data) {
      alert("Dados alterados com sucesso!");
    },
    error: function () {
      alert("Erro, tente novamente!");
    }
  });
});

botaoAdicionar.addEventListener("click", function () {
  if (inputProcesso.value == "" || inputEfeito.value == "" || inputAssunto == "" || inputRelator == "" || inputSuperintendencia == "") {
    alert("Complete os campos em branco");
  } else {
    index = parseInt(index) + 1;
    adicionaLinha(index, inputProcesso.value, inputEfeito.value, inputAssunto.value, inputRelator.value, inputSuperintendencia.value);
    objetoPalavrasSelecionadas.push({
      id: index,
      processo: inputProcesso.value,
      efeito: inputEfeito.value,
      assunto: inputAssunto.value,
      relator: inputRelator.value,
      superintendencia:inputSuperintendencia.value,
      ultimo_documento: "",
    });
    inputProcesso.value = "";
    inputEfeito.value = "";
    inputAssunto.value = "";
    inputRelator.value = "";
    inputSuperintendencia.value ="";
  }
});

function removerPalavra(elemento, processo) {
  var arraypalavras = objetoPalavrasSelecionadas.filter(function (e) {
    return e.processo != processo
  })
  objetoPalavrasSelecionadas = arraypalavras
  apagaLinha = elemento.parentNode.parentNode.remove();
}

function adicionaLinha(index, processo, efeito, assunto,relator,superintendencia, ultimo_documento) {
  let _html = "";
  _html += `<tr>
      <th class = 'hidden d-none data-index='${index}'>${index} </th>
        <td contenteditable="true">${processo}</td>
        <td contenteditable="true">${efeito}</td>
        <td contenteditable="true" class = "assunto justify-content-center" >${assunto}</td>
        <td contenteditable="true">${relator}</td>
        <td contenteditable="true">${superintendencia}</td>
        <td>${ultimo_documento}</td>
        <td class="align-content-center"><button data-index='${index}' class="btn btn-outline-primary botaoRemover" type="button"
        onclick="removerPalavra(this,'${processo}')">X</button></td>
      </tr>`;
  corpoTab.innerHTML += _html;
}


function relocate_assuntos() {
  location.href = "/middle/config-assuntos-Aneel";
} 
