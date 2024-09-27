// Variavel global
var limiarChuvaMax = 20;
var color1 = [20, 100, 210];
// COLOR1 = rgb(20, 100, 210);
var color2 = [255,255,255];
// COLOR2 = rgb(255,255,255);
var color3 = [40, 130, 240];
// COLOR3 = rgb(40, 130, 240);
var color4 = [150, 210, 250];
// COLOR4 = rgb(150, 210, 250);
var corNeutra = [245, 201, 191];
// corNeutra = rgb(245, 201, 191);
var corNeutra2 = [225, 20, 0];
// corNeutra2 = rgb(225, 20, 0);
var datasSelecionadas = [];
var rodadasPesquisadas = [];
var grupoEscolhido = [];
let tit = '';
const dataInput = document.getElementById('input');
dataInput.value = dataAtual();
var checkboxesSelecionados = [];
const semanasAgrupadasDiff = [];
const semanasAgrupadas = [];
let semanasAgrupadaFiltradas = []; 



document.getElementById("sidebarCollapse").addEventListener("click", function() {
  var sidebar = document.getElementById("sidebar");
  sidebar.classList.toggle("active");
});



function atualizaTabela() {
  
  const modeloSelecionado = document.getElementById('program').value;
  var grupoEscolhido = $("input[name='exampleRadios']:checked").val();
  const hr = document.getElementById("horaRodada").value;
  modalLoading.style.display = "block";
  getPrevisao(dataInput.value, modeloSelecionado, grupoEscolhido, hr);
}

function erase() {
  $('#div-tabelas').html(' ');
}

function clearTable() {
  let index = rodadasPesquisadas.indexOf(this.dataset.id)
  rodadasPesquisadas.splice(index, 1);
  document.getElementById(this.dataset.id).remove();
  // erase()
  // erase(), window.location.reload()
  checkboxesSelecionados.pop();
  datasSelecionadas.pop();
}

$('#div-tabelas').delegate(".js-limpaTabela", "click", clearTable);



//Alterando padrao da data entre yyyy-mm-dd to dd/MM/yyyy
//Existe uma variavel global chamada rodadasPesquisadas
function getPrevisao(data, modelo, grupo, hora) {
  
  const [ano, mes, dia] = data.split('-');
  let rod = `precipitacao-${dia}${mes}${ano} - ${modelo} - ${hora} - ${grupo}`;
  if (rodadasPesquisadas.includes(rod)) {
    return;
  }
  
  rodadasPesquisadas.push(rod);
  
  // Conectando ao banco de dados local
  $.ajax({
    method: "GET",
    url: "/middle/API/get/resultado_chuvas",
    data: { dt_rodada: `${ano}${mes}${dia}`, modelo: modelo, grupo: grupo, hr: hora },
  }).done(function (dados) {
    if (!dados) {
      return alert("Previsão para data não encontrada.");
    }
    let previsao = dados;
    const dicionarioSubmercados = {
      Sudeste : dados[Object.keys(dados)[2]],
      Sul : dados[Object.keys(dados)[3]],
      Nordeste : dados[Object.keys(dados)[0]],
      Norte : dados[Object.keys(dados)[1]],
    }
    if (!dados || Object.keys(dicionarioSubmercados).length === 0) {
      alert("Previsão para data não encontrada.");
    } else {
      inicioSemanaEletrica = obterSabadosDoMeses(data);
      montaTabela(dicionarioSubmercados, `${ano}-${mes}-${dia}`, modelo, grupo, hora, color1, color2);
      // atribuindo valor do grupo escolhido para usar na tabela de diferença.
      grupoEscolhido = grupo;
      localStorage.setItem(`${data} ${hora} ${modelo} ${grupo}`, JSON.stringify(dicionarioSubmercados));
    }
    }).fail(function(jqXHR, textStatus, errorThrown) {
        console.error("Erro na requisição AJAX:", textStatus, errorThrown);
        if (jqXHR.status === 0) {
            // Erro de conexão
            alert("Ocorreu um erro na conexão. Por favor, clique em recarregar página. Se o problema persistir, verifique a conexão de rede.");
        } else if (jqXHR.status === 404 || jqXHR.status === 500) {
            // Dados não encontrados (erro 404 ou 500)
            alert(`Os dados do modelo=${modelo},rodada=${hora},dia= ${dia}/${mes}/${ano} não foi encontrado no banco de dados.Tente novamente com outros dados`);
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


function obterSabadosDoMeses(dataInicial) {
  const sabados = [];

  const dataInicialFormat = new Date(dataInicial);
  // Obtém o mês
  var mes = dataInicialFormat.getUTCMonth();
  
  // Se for janeiro, ajusta o mês para dezembro e retorna o ano
  if (mes === 0) {
    mes = 12;
    const ano = dataInicialFormat.getUTCFullYear() - 1;
    var dataMesAnterior = `${ano}-${mes.toString().padStart(2, '0')}-${dataInicialFormat.getUTCDate()}`;
  } else {
    // Se não for janeiro, apenas diminui o mês
    mes = mes.toString().padStart(2, '0');
    var dataMesAnterior = `${dataInicialFormat.getUTCFullYear()}-${mes}-${dataInicialFormat.getUTCDate()}`;
  }
  const data = new Date(dataMesAnterior);
  // Loop para percorrer os próximos meses
  for (let i = 0; i < 3; i++) {
    var ano = data.getFullYear();
    var mes = data.getMonth() + i;

    if (mes === 12) {
      mes = 0;
      ano = ano + 1;
    }
    if (mes === 13) {
      mes = 1;
      ano = ano + 1;
    }

    // Loop para percorrer todos os dias do mês
    for (let dia = 1; dia <= 31; dia++) {
      // Criar uma nova data para o dia atual do loop
      const dataAtual = new Date(ano, mes, dia);

      if (
        dataAtual.getMonth() === mes || 
        (i === 1 && dataAtual.getMonth() === 0)  // Para o próximo mês, verifique janeiro do próximo ano
      ) {
        // Verificar se o dia é um sábado (no JavaScript o domingo é 0)
        if (dataAtual.getDay() === 6) {
          // Adicionar a data formatada ao array sabados
          sabados.push(formatarData(dataAtual));
        }
      }
    }
  }
  return sabados;
}



function montaTabela(dados, dataSelecionada, modelo, grupo, hora, corEscura, corClara, tabelaDiferenca = false) {
  const [ano, mes, dia] = dataSelecionada.split('-');
  const idTabela = `precipitacao-${dia}${mes}${ano} - ${modelo} - ${hora}`;
  const caption = `
  <caption >
  <div class="col-12 row justify-content-between">
  ${tabelaDiferenca == false ? `${modelo}_${dia}${mes}${ano.substring(2, 4)}${hora}z` : `${modelo}`}
    <div class="justify-content-center">
      <label class="lblTabela">Selecione a tabela</label>
      <input type="checkbox" class="diferenca ml-1" data-modelo="${modelo}" data-hora="${hora}" data-dia="${dataSelecionada}" data-grupo="${grupo}">
      <input data-id="${idTabela}" class="js-limpaTabela ml-1" type="button" value="X">
    </div>
  </div>
</caption>`;

// Montando cabeçalho da tabela 
  const key = Object.keys(dados)[0];
  const cabecalho = [];

  cabecalho.push(`<thead>`, `<tr>`, `<th>submercado</th>`,`<th>bacia</th>`)
  const datasAdicionadas = new Set(); // Conjunto para armazenar datas únicas

 // Contagem de bacias por região
 const baciasPorRegiao = {};
 for (const regiao in dados) {
   baciasPorRegiao[regiao] = Object.keys(dados[regiao]).length;
 }


   for (const regiao in dados) {
    for (const bacia in dados[regiao]){
      for (const dias in dados[regiao][bacia]){
        // Separe o dia, mês e ano para verificar se a data já foi adicionada ao cabeçalho
        [a, m, d] = dias.split('/')
        const dataFormatada = `${d}/${m}`;
        if (!datasAdicionadas.has(dataFormatada)) {
          cabecalho.push(`<th>${dataFormatada}</th>`);
          datasAdicionadas.add(dataFormatada); // Adicione a data ao conjunto
        }
      }
    }
  }

const semanasAgrupadas = []; 

Object.keys(dados).forEach(regiao => {
    Object.keys(dados[regiao]).forEach(bacia => {
        var datas = Object.keys(dados[regiao][bacia]);

        // Reset the array for each iteration
        semanasAgrupadas.length = 0;
        inicioSemanaEletrica.forEach((inicio, index) => {
          const proximoInicio = inicioSemanaEletrica[index + 1];
          const isLastIteration = index === inicioSemanaEletrica.length - 1;
      
          let semana;
      
          if (isLastIteration) {
              semana = datas.filter(data => data > inicio);
          } else {
              semana = datas.filter(data => data > inicio && data <= proximoInicio);
          }
          
          semanasAgrupadas.push(semana);
      });
      
      semanasAgrupadaFiltradas = semanasAgrupadas.filter(array => array.length > 0);
      semanasAgrupadaFiltradas.forEach((datas, index) => {

            const semana = `Semana ${index + 1}`;
            let soma = 0;

            datas.forEach(data => {
                const valor = dados[regiao][bacia][data];

                if (valor !== "-") {
                    soma += parseFloat(valor);
                }
            });
        });
    });
});
for (let i = 0; i < semanasAgrupadaFiltradas.length; i++) {
    cabecalho.push(`<th>Semana ${i + 1}</th>`);
}

cabecalho.push(`</tr>`, `</thead>`);
  
const linha = [];
linha.push(`<tbody>`);
for (const regiao in dados) {
  linha.push(`<th rowspan="${baciasPorRegiao[regiao]}">${regiao}</th>`);
  for (const bacia in dados[regiao]) {
    linha.push(`<th>${bacia}</th>`);

    let somaLinha = 0; // Variável para armazenar a somaFormatada da linha
    let somaFormatada = 0; 

    for (const dias in dados[regiao][bacia]) {
      const valor = dados[regiao][bacia][dias];
      let peso = valor / limiarChuvaMax;
      if (peso > 1) peso = 1;
      let corFundo = (valor == '-') ? color2 : pickHex(peso, corEscura, corClara);
      let corTexto = "black";
      if (peso > 0.5) corTexto = "white";
      if (peso >= 0) corFundo = pickHex(peso, corEscura, color2);
      if (peso <= 0) corFundo = pickHex(peso * -1, corNeutra2, color2);
      linha.push(`<td style="background-color: rgb(${corFundo.join()}); color: ${corTexto};">${valor}</td>`);

    }


    semanasAgrupadaFiltradas.forEach((datas, index) => {
      const semana = `Semana ${index + 1}`;

      let soma = 0;
      // Iterar sobre as datas da semana
      datas.forEach(data => {
          
          const valor = dados[regiao][bacia][data];
          if (valor !== "-") {
              soma += parseFloat(valor);
          }
      });

      const somaFormatada = soma.toFixed(2);
      let numeroArredondado = Math.round(somaFormatada);
      let peso = numeroArredondado / limiarChuvaMax;
      if (peso > 1) peso = 1;
      let corFundo = (numeroArredondado == '-') ? color2 : pickHex(peso, corEscura, corClara);
      let corTexto = "black";
      if (peso > 0.5) corTexto = "white";
      if (peso >= 0) corFundo = pickHex(peso, corEscura, color2);
      if (peso <= 0) corFundo = pickHex(peso * -1, corNeutra2, color2);
      linha.push(`<td style="background-color: rgb(${corFundo.join()}); color: ${corTexto};">${numeroArredondado}</td>`);
  });

    linha.push(`</tr>`);
  }
}
linha.push(`</tbody>`);

  // Adicionando tabela na tela
  const tabela = [];
  tabela.push(`<table id="${idTabela}" class="table table-bordered tabelas-precipitacao tabela-fixalargura">`, caption, cabecalho.join(''), linha.join(''), `</table>`);
  const root = document.getElementById('div-tabelas');
  root.insertAdjacentHTML('beforeend', tabela.join(''));

}


// Adiciona um event listener aos checkboxes com a classe "diferenca"
document.querySelector("#div-tabelas").addEventListener("change", function (e) {
  if (e.target.classList.contains("diferenca")) {
    // Verifica se o checkbox foi marcado ou desmarcado
    if (e.target.checked) {
      // Adiciona o checkbox à lista de checkboxes selecionados
      checkboxesSelecionados.push(e.target);
      const data = {};
      data[`${e.target.dataset.dia} ${e.target.dataset.hora} ${e.target.dataset.modelo} ${e.target.dataset.grupo}`] = e.target.dataset.modelo;
      datasSelecionadas.push(data);
    } else {
      // Remove o checkbox da lista de checkboxes selecionados
      const index = checkboxesSelecionados.findIndex((checkbox) => checkbox === e.target);
      checkboxesSelecionados.splice(index, 1);
      const indexobj = datasSelecionadas.findIndex(function (obj) {
        return obj[`${e.target.dataset.dia} ${e.target.dataset.hora} ${e.target.dataset.modelo} ${e.target.dataset.grupo}`]
      });
      datasSelecionadas.splice(indexobj, 1);
    }

    // Limpa a lista se não houver checkboxes selecionados
    if (checkboxesSelecionados.length > 2) {
      // Desmarca o checkbox atual e exibe um aviso
      e.target.checked = false;
      alert("Você só pode selecionar até duas tabelas para comparação.");
      checkboxesSelecionados.pop(); // Remove o último checkbox adicionado à lista
      datasSelecionadas.pop();
      
    }
  }
});


//Comparando as tabelas
document.querySelector("#btn-3").addEventListener("click", function (e) {
    if (this.classList.contains("disabled")) return false;

    // Variável para armazenar o título do comparativo de tabelas
    let tit = "";

    // Mapeamento dos dados das datas selecionadas
    const dados = datasSelecionadas.map(function (data) {
        const chave = Object.keys(data)[0];
        const dados = localStorage.getItem(chave);

        // Convertendo datas do título do comparativo de tabelas
        const [ano, mes, dia_hora] = chave.split("-");
        const [dia, hora] = dia_hora.split(" ");
        tit = tit + `${data[chave]}_${dia}${mes}${ano.substring(2, 4)}${hora}z - `;
        return JSON.parse(dados);
    });

    // Removendo os últimos 3 caracteres do título para eliminar o último ' - '
    const result = tit.substring(0, tit.length - 3);

    // Obtendo os dados das duas fontes (sourceA e sourceB)
    const sourceA = dados[0];
    const sourceB = dados[1];

    // capturando a primeira data apenas para saber a ordem da conta para tirar a diferenca de valores
    var chavessourceA= Object.keys(sourceA["Sudeste"]["Correntes"]);
    var chavessourceB = Object.keys(sourceB["Sudeste"]["Correntes"]);

    const diff = {};

// Iterando pelas regiões em sourceA
for (const regiao in sourceA) {
    const baciasA = sourceA[regiao];
    const baciasB = sourceB[regiao];
    if (!baciasB) continue; // Verificando se a região também existe em sourceB

    const diffBacia = {};

    // Iterando pelas bacias da região atual
    for (const bacia in baciasA) {
        const diasDataA = baciasA[bacia];
        const diasDataB = baciasB[bacia];
        if (!diasDataB) continue; // Verificando se a bacia também existe em sourceB

        const diffPrecipitacoes = {};

        // Iterando pelos dias de precipitação na bacia atual
        for (const dia in diasDataA) {
            const precipitacaoA = diasDataA[dia];
            const precipitacaoB = diasDataB[dia];

            if (precipitacaoA === undefined || precipitacaoB === undefined) {
                diffPrecipitacoes[dia] = "-";
            } else {
                // Verificando a ordem das datas para calcular a diferença correta
                const diffValue = (chavessourceA[0] > chavessourceB[0]) ? (precipitacaoA - precipitacaoB).toFixed(2) : (precipitacaoB - precipitacaoA).toFixed(2);
                diffPrecipitacoes[dia] = diffValue;
            }
        }
        diffBacia[bacia] = diffPrecipitacoes;
    }

    diff[regiao] = diffBacia;
}


  Object.keys(diff).forEach(regiao => {
    // Iterar sobre as bacias em cada região
    Object.keys(diff[regiao]).forEach(bacia => {
        // Obter as datas da bacia
        var datas = Object.keys(diff[regiao][bacia]);

        const semanasAgrupadasDiff = []; // Reinicializa o array para cada bacia

        inicioSemanaEletrica.forEach((inicio, index) => {
            const proximoInicio = inicioSemanaEletrica[index + 1];
            if (proximoInicio) {
                const semana = datas.filter(data => data > inicio && data <= proximoInicio);
                semanasAgrupadasDiff.push(semana);
            }
        });

        semanasAgrupadasDiff.forEach((datas, index) => {
            const semana = `Semana ${index + 1}`;
            let soma = 0;

            // Iterar sobre as datas da semana
            datas.forEach(data => {
                const valor = diff[regiao][bacia][data];

                if (valor !== "-") {
                    soma += parseFloat(valor);
                }
            });
            // Arredondar e armazenar a soma na semana
            const somaArredondada = Math.round(soma);
            // console.log(`${regiao} - ${bacia} - ${semana}: ${somaArredondada}`);
        });
    });
});
  
    // Chamando a função montaTabela com os dados obtidos
    montaTabela(diff, dataAtual(), result, grupoEscolhido, " ", color3, color4, true);

    // Resetando a variável tit para evitar conflitos em cliques futuros
    tit = "";
});


function formatarData(data) {
  const ano = data.getFullYear().toString();
  const mes = String(data.getMonth() + 1).padStart(2, '0');
  const dia = String(data.getDate()).padStart(2, '0');
  return `${ano}/${mes}/${dia}`;
}

function dataAtual() {
  var hoje = new Date();
  var dd = String(hoje.getDate()).padStart(2, '0');
  var mm = String(hoje.getMonth() + 1).padStart(2, '0'); 
  var yyyy = hoje.getFullYear();
  return yyyy + '-' + mm + '-' + dd;
}

// Função para definir cor de fundo para tabela de acordo com o peso do texto
function pickHex(weight, cor1, cor2) {
  var w1 = weight;
  var w2 = 1 - w1;
  var rgb = [Math.round(cor1[0] * w1 + cor2[0] * w2),
  Math.round(cor1[1] * w1 + cor2[1] * w2),
  Math.round(cor1[2] * w1 + cor2[2] * w2)];
  return rgb;
}
// Colocando funções nos botões //
//desabilita o botão no início
document.getElementById("btn-1").hidden = true;
document.getElementById("btn-2").hidden = true;
document.getElementById("btn-3").hidden = true;

//cria um event listener que escuta mudanças no input
document.getElementById("program").addEventListener("input", function (event) {
  //busca conteúdo do input
  var conteudo = document.getElementById("program").value;
  //valida conteudo do input 
  if (conteudo !== null && conteudo !== '') {
    //habilita o botão
    document.getElementById("btn-1").hidden = false;
    document.getElementById("btn-2").hidden = false;
    document.getElementById("btn-3").hidden = false;
  } else {
    //desabilita o botão se o conteúdo do input ficar em branco
    document.getElementById("btn-1").hidden = true;
    document.getElementById("btn-2").hidden = true;
    document.getElementById("btn-3").hidden = true;
  }
});

// RODAR FUNÇÃO SEM PRECISAR CLICAR NOS BOTÕES
// window.onload = function() {
//   getPrevisao('2023-12-16', 'PCONJUNTO', 'submercado', '00')
//   getPrevisao('2023-12-17', 'PCONJUNTO', 'submercado', '00')
// };


