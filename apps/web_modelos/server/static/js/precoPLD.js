var ano = document.getElementById("ano");
var pldMaxHora = document.getElementById("pldMaxHora");
var pldMaxEstr = document.getElementById("pldMaxEstr");
var pldMin = document.getElementById("pldMin");
var custoDeficit = document.getElementById("custoDeficit");
let botaoAdicionar = document.getElementById("btn-Adicionar");
let corpoTab = document.getElementById("corpoTabela");
let botaoSalvar = document.getElementById("btn-Salvar");
var objetoAnosSelecionados = [];

$.ajax({
  method: "GET",
  url: "/middle/API/get/precoPLD",
  data: {},
}).done(function (objeto) {
  // Atualiza o objetoAnosSelecionados
  objetoAnosSelecionados = objeto;

  // Limpa o conteúdo da tabela
  corpoTab.innerHTML = "";

  // Adiciona as linhas ordenadas à tabela
  objetoAnosSelecionados.sort((a, b) => a.ano - b.ano);
  objetoAnosSelecionados.forEach((row) => {
    adicionaLinha(row.ano, row.pldMaxHora, row.pldMaxEstr, row.pldMin, row.custoDeficit);
  });
});

botaoSalvar.addEventListener("click", function () {
  dataToSend = JSON.stringify(objetoAnosSelecionados);

  $.ajax({
    contentType: "application/json; charset=utf-8",
    dataType: "json",
    type: "POST",
    url: "/middle/API/set/precoPLD",
    data: dataToSend,
    success: function (data) {
      alert("Dados alterados com sucesso!");
    },
    error: function () {
      alert("Erro, verificar dados inseridos.");
    },
  });
});

botaoAdicionar.addEventListener("click", function () {
  if (
    ano.value == "" ||
    pldMaxHora.value == "" ||
    pldMaxEstr.value == "" ||
    pldMin.value == "" ||
    custoDeficit.value == ""
  ) {
    alert("Complete os campos em branco");
  } else {
    // Adiciona a nova linha ao objetoAnosSelecionados
    objetoAnosSelecionados.push({
      ano: ano.value,
      pldMaxHora: pldMaxHora.value,
      pldMaxEstr: pldMaxEstr.value,
      pldMin: pldMin.value,
      custoDeficit: custoDeficit.value,
    });

    // Ordena o objetoAnosSelecionados pelo ano
    objetoAnosSelecionados.sort((a, b) => a.ano - b.ano);

    // Limpa o conteúdo da tabela
    corpoTab.innerHTML = "";

    // Adiciona as linhas ordenadas à tabela
    objetoAnosSelecionados.forEach((row) => {
      adicionaLinha(row.ano, row.pldMaxHora, row.pldMaxEstr, row.pldMin, row.custoDeficit);
    });

    // Limpa os campos de entrada
    ano.value = "";
    pldMaxHora.value = "";
    pldMaxEstr.value = "";
    pldMin.value = "";
    custoDeficit.value = "";
  }
});

function removerAno(elemento, ano) {
  // Filtra o objetoAnosSelecionados para remover o ano especificado
  objetoAnosSelecionados = objetoAnosSelecionados.filter((e) => e.ano != ano);

  // Atualiza a tabela após a remoção
  atualizarTabela();
}

function adicionaLinha(ano, pldMaxHora, pldMaxEstr, pldMin, custoDeficit) {
  let _html = `<tr>
      <th data-index='${ano}'>${ano}</th>
      <td>${pldMaxHora}</td>
      <td>${pldMaxEstr}</td>
      <td>${pldMin}</td>
      <td>${custoDeficit}</td>
      <td class="align-content-center"><button data-index='${ano}' class="btn btn-outline-primary botaoRemover" type="button"
        onclick="removerAno(this,'${ano}')">X</button></td>
    </tr>`;

  corpoTab.innerHTML += _html;
}

function atualizarTabela() {
  // Limpa o conteúdo da tabela
  corpoTab.innerHTML = "";

  // Adiciona as linhas ordenadas à tabela
  objetoAnosSelecionados.forEach((row) => {
    adicionaLinha(row.ano, row.pldMaxHora, row.pldMaxEstr, row.pldMin, row.custoDeficit);
  });
}
