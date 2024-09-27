let botaoRestart = document.getElementById("btn-Restart");
let botoes = [document.getElementById("btn-1"), document.getElementById("btn-2"), document.getElementById("btn-3"), document.getElementById("btn-4"), document.getElementById("btn-5")];

var botaoSelecionado = [];

function atualizarArray(sessaoSolicitada) {
  botaoSelecionado = [sessaoSolicitada];
}

botoes.forEach(function(botao) {
  botao.addEventListener("click", function () {
    let sessaoSolicitada = botao.value;
    atualizarArray(sessaoSolicitada);
  });
});

botaoRestart.addEventListener("click", function () {

var confirmar = confirm(`Tem certeza que deseja reiniciar a sessão ${botaoSelecionado[0]}?`);
// Verificar se o usuário clicou em "OK"
if (confirmar) {
  $.ajax({
    contentType: "application/json; charset=utf-8",
    dataType: "json",
    type: "POST",
    url: "/middle/API/set/vnc_kill_session",
    data: botaoSelecionado[0],
    success: function (data) {
      alert(`Sessão: ${data} foi resetada com sucesso!`);
    },
    error: function () {
      alert("Erro, verificar dados inseridos.");
    }
  });
} else {
  // O usuário clicou em "Cancelar" ou fechou o modal
  alert("Operação cancelada!!");
}
});
