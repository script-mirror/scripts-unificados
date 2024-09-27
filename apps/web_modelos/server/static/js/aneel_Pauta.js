var inputPalavra = document.getElementById("inputPalavra");
var inputTag = document.getElementById("inputTag");
let botaoAdicionar = document.getElementById("btn-Adicionar");
let corpoTab = document.getElementById("corpoTabela");
let botaoSalvar = document.getElementById("btn-Salvar");
var objetoPalavrasSelecionadas = {};
var index = 0;

$.ajax({
  method: "GET",
  url: "/middle/API/get/palavras-Aneel",
  data: {},
}).done(function (objeto) {
  for (let idx in objeto) {
    row = objeto[idx];
    adicionaLinha(row["id"], row["palavra"], row["tag"]);
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
    url: "/middle/API/set/palavras-Aneel",
    data: dataToSend,
    success: function (data) {
      alert("Dados alterados com sucesso!");
    },
    error: function(){
      alert("Erro, tente novamente!");
    }
  });
});

botaoAdicionar.addEventListener("click", function () {
  if (inputPalavra.value == "" || inputTag.value == "") {
    alert("Complete os campos em branco");
  } else {
    index = parseInt(index) + 1;
    adicionaLinha(index, inputPalavra.value, inputTag.value);
    objetoPalavrasSelecionadas.push({
      id: index,
      palavra: inputPalavra.value,
      tag: inputTag.value,
    });
    inputPalavra.value = "";
    inputTag.value = "";
  }
});

function removerPalavra(elemento,palavra) {
  var arraypalavras = objetoPalavrasSelecionadas.filter(function(e){
    return e.palavra != palavra
  })
  objetoPalavrasSelecionadas = arraypalavras
  apagaLinha = elemento.parentNode.parentNode.remove();
}

function adicionaLinha(index, palavra, tag) {
  let _html = "";
  _html += `<tr>
      <th data-index='${index}'>${index}</th>
        <td>${palavra}</td>
        <td>${tag}</td>
        <td class="align-content-center"><button data-index='${index}' class="btn btn-outline-primary botaoRemover" type="button"
        onclick="removerPalavra(this,'${palavra}')">X</button></td>
      </tr>`;
  corpoTab.innerHTML += _html;
}