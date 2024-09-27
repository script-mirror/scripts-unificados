
const calcular = document.getElementById("calcular");
const calcularCMO = document.getElementById("calcularCMO");

function capturarValores() {
    const PLDmensalMedio = document.getElementById("PLDmensalMedio");
    const PrevisaoPLDdiario = document.getElementById("PrevisaoPLDdiario");
    const diasAcimaPiso = document.getElementById("diasAcimaPiso");
    const PLDhorarioMediaDiaria = document.getElementById("PLDhorarioMediaDiaria");

    const mensalMedio = parseFloat(PLDmensalMedio.value.replace(",", "."));
    const PLDdiario = parseFloat(PrevisaoPLDdiario.value.replace(",", "."));
    const AcimaPiso = parseFloat(diasAcimaPiso.value.replace(",", "."));
    const horarioMediaDiaria = parseFloat(PLDhorarioMediaDiaria.value.replace(",", "."));
    return [PLDdiario, mensalMedio, AcimaPiso, horarioMediaDiaria];
}

function capturarCMO() {
    const horasAcimaPiso = document.getElementById("horasAcimaPiso");
    const CMOacimaDoPiso = document.getElementById("CMOacimaDoPiso");
    const cmo_acimaDoPiso = parseFloat(CMOacimaDoPiso.value.replace(",", "."));
    const horas_AcimaPiso = parseFloat(horasAcimaPiso.value.replace(",", "."));
    return [cmo_acimaDoPiso,horas_AcimaPiso];
}

function formatarData(data) {
    const dia = ('0' + data.getDate()).slice(-2);
    const mes = ('0' + (data.getMonth() + 1)).slice(-2);
    const ano = data.getFullYear();
    return ano + '-' + mes;
}
window.onload = function() {
    // Obtém a data atual
    const dataAtual = new Date();
    // Formata a data no formato aaaa-mm-dd
    const dataFormatada = formatarData(dataAtual);
    // Obtém o campo de entrada e define seu valor para a data atual
    const campoData = document.getElementById('data');
    campoData.value = dataFormatada;
    // Dispara um evento de mudança no campo de entrada
    const evento = new Event('change');
    campoData.dispatchEvent(evento);
};

const AtualizarPagina = document.getElementById("Atualizar");
AtualizarPagina.addEventListener('click', function () {
    location.reload();
});

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


const selecionandoMesCalendario = document.getElementById("data");
selecionandoMesCalendario.addEventListener('click', function () {
    let diasAcimaPiso = document.getElementById("diasAcimaPiso");
    let PLDhorarioMediaDiaria = document.getElementById("PLDhorarioMediaDiaria");
    let CMOacimaDoPiso = document.getElementById("CMOacimaDoPiso");
    diasAcimaPiso.value = "";
    PLDhorarioMediaDiaria.value = "";
    CMOacimaDoPiso.value = "";

  });

function atualizarTabela() {
    document.getElementById('loading-spinner').style.display = 'block';
    document.getElementById('loading').style.display = 'block';

    // Inicializa a lista valoresPLDTabelaAlterados fora do escopo da chamada AJAX
    const valoresPLDTabelaAlterados = []; // Uma lista para armazenar os valores alterados


    const data = document.getElementById('data').value;
    const dataDividida = data.split('-');
    const ano = dataDividida[0];
    const mes = dataDividida[1];
    const dia = '01';

    // Crie a string no formato "YYYYMMDD"
    const dataFormatada = `${ano}${mes}${dia}`;
    const diasNoMes = new Date(ano, mes, 0).getDate();
    const valoresPLDTabela = []; // Uma lista para armazenar os valores

    $.ajax({
        method: "GET",
        url: `/middle/API/GET/valor_pld?dataInicial=${dataFormatada}`,
        data: {},
    }).done(function (objeto) {
        document.getElementById('tabela').innerHTML = '<tr><th>Data</th><th>Valor (R$)</th></tr>';
        // Cria a tabela somente quando a requisição estiver concluída
        const objeto_valor_pld = objeto[0];
        const valor_pldMinimoAno = objeto[1];

        const valores = capturarValores();
        const PLDdiario = valores[0];

        if (isNaN(PLDdiario) || PLDdiario != valor_pldMinimoAno) {
            document.getElementById('PrevisaoPLDdiario').value = valor_pldMinimoAno.toFixed(2);
        }
        // Adiciona uma linha para cada dia do mês
        for (let i = 1; i <= diasNoMes; i++) {
            // Cria uma nova linha
            const linha = document.createElement('tr');

            // Cria a coluna 'Data'
            const data = document.createElement('td');
            data.innerText = `${i}/${mes}/${ano}`;
            linha.appendChild(data);

            // Cria a coluna 'Valor'
            const valor = document.createElement('td');
            const chaveData = `${ano}${mes}${String(i).padStart(2, '0')}`;
            if (objeto_valor_pld[chaveData] !== undefined) {
                valor.innerText = objeto_valor_pld[chaveData].toFixed(2); // Formata o valor com duas casas decimais
                valor.style.backgroundColor = '#7adb7a';
                valoresPLDTabela.push(objeto_valor_pld[chaveData].toFixed(2)); // Adiciona o valor à lista

            } else {
                valor.innerText = valor_pldMinimoAno.toFixed(2); // Valor padrão se não houver um valor correspondente no banco de dados
                valor.style.backgroundColor = '#9c339b';
                valor.style.color = 'white';
                valoresPLDTabela.push(valor_pldMinimoAno).toFixed(2); // Adiciona o valor padrão à lista

            }

            // Adiciona um evento de clique para permitir a edição
            valor.addEventListener('click', function () {
                // Cria um campo de entrada
                const input = document.createElement('input');
                input.type = 'text';
                input.value = valor.innerText;

                // Adiciona um evento de perda de foco ao campo de entrada para salvar a edição
                input.addEventListener('blur', function () {
                    valor.innerText = input.value;

                    // Atualiza a lista com o valor editado
                    const valorEditado = parseFloat(input.value);
                    const indice = i - 1; // Índice do array (começando do 0)

                    if (isNaN(valorEditado)) {
                        // Caso seja NaN, use o valor padrão
                        valoresPLDTabelaAlterados[indice] = parseFloat(valor_pldMinimoAno);
                    } else {
                        valoresPLDTabelaAlterados[indice] = valorEditado;
                    }

                    // Recalcula a média com base em valoresPLDTabelaAlterados
                    const mediaPLDmensal = calcularMediaPLDmensal(valoresPLDTabelaAlterados).toFixed(2);
                    document.getElementById('PLDmensalMedio').value = mediaPLDmensal;
                });

                // Substitui o texto pelo campo de entrada
                valor.innerHTML = '';
                valor.appendChild(input);

                // Define o foco no campo de entrada
                input.focus();
            });

            linha.appendChild(valor);
            // Adiciona a linha à tabela
            document.getElementById('tabela').appendChild(linha);
        }

        // Inicializa valoresPLDTabela com os valores originais
        for (let i = 1; i <= diasNoMes; i++) {
            const chaveData = `${ano}${mes}${String(i).padStart(2, '0')}`;
            if (objeto_valor_pld[chaveData] !== undefined) {
                valoresPLDTabelaAlterados.push(objeto_valor_pld[chaveData].toFixed(2));
            } else {
                valoresPLDTabelaAlterados.push(valor_pldMinimoAno.toFixed(2));
            }
        }

        const mediaPLDmensalInicial = calcularMediaPLDmensal(valoresPLDTabela).toFixed(2);
        document.getElementById('PLDmensalMedio').value = mediaPLDmensalInicial;

        document.getElementById('loading-spinner').style.display = 'none';
        document.getElementById('loading').style.display = 'none';
    }).fail(function () {
        // Se a requisição falhar, esta função será chamada
        alert("Não foi possível achar os valores para essa data no banco de dados");
        document.getElementById('loading-spinner').style.display = 'none';
        document.getElementById('loading').style.display = 'none';
    });    

}


function calcularMediaPLDmensal(valoresDaTabela) {
    let soma = 0;
    // Converter os valores para números e somar
    for (let i = 0; i < valoresDaTabela.length; i++) {
        soma += parseFloat(valoresDaTabela[i]);
    }
    // Calcular a média
    if (valoresDaTabela.length > 0) {
        const media = soma / valoresDaTabela.length;
        return media;
    } else {
        return 0; // Média é zero se não houver valores
    }
}

function calcularNumeroDiasMes(){
    const data = document.getElementById('data').value;
    const dataDividida = data.split('-');
    const ano = dataDividida[0];
    const mes = dataDividida[1];
    const dia = '01';

    // Crie a string no formato "YYYYMMDD"
    const dataFormatada = `${ano}${mes}${dia}`;
    const diasNoMes = new Date(ano, mes + 1, 0).getDate();
    return diasNoMes
}


const diasAcimaPiso = document.getElementById("diasAcimaPiso");
const valorPLDhorarioAcimaPiso = document.getElementById('PLDhorarioMediaDiaria');

let diasAcimaPisoAlterado = false;
let valorPLDhorarioAlterado = false;

diasAcimaPiso.addEventListener('change', function () {
    diasAcimaPisoAlterado = true;
    let valordiasAcimaPisoAlterado= diasAcimaPiso.value;
    verificarExibirDiv();
    return valordiasAcimaPisoAlterado
});

valorPLDhorarioAcimaPiso.addEventListener('change', function () {
    valorPLDhorarioAlterado = true;
    let PLDhorarioAcimaPiso= valorPLDhorarioAcimaPiso.value;
    verificarExibirDiv();
    return PLDhorarioAcimaPiso
});

function verificarExibirDiv() {
    if (diasAcimaPisoAlterado && valorPLDhorarioAlterado) {
        document.getElementById('divAtualizarTabelaPLD').style.display = 'block';
        document.getElementById('divAtualizarTabelaPLD').style.display = 'flex';
        document.getElementById('divAtualizarTabelaPLD').style.justifyContent = 'center';
        document.getElementById('divAtualizarTabelaPLD').style.alignItems = 'center';
    }
}


calcular.addEventListener("click", function() {
        const valores = capturarValores();
        const PLDdiario = valores[0];
        const mensalMedio = valores[1];
        const AcimaPiso = valores[2];
        const horarioMediaDiaria = valores[3];
        const resultados = capturarCMO();
        const cmoacimaDoPiso = resultados[0];
        const horas_AcimaPiso = resultados[1];

    const diasNoMes = calcularNumeroDiasMes();
    const diasExistentesNoMes = diasNoMes;


    if (valores.every(item => isNaN(item))) {
        document.getElementById('divAtualizarTabelaPLD').style.display = 'none';
        alert("Todos os campos estão vazios, você deve preencher no mínimo 3 campos sendo que o campo 'Previsão PLD diário' sempre deve ter algum valor")

    }else if (valores.every(item => !isNaN(item))) {

        alert("Todos os campos estão preenchidos, você deve deixar um dos campos vazio exceto o campo 'Previsão PLD diário'")

    }else if (isNaN(PLDdiario)) {
        alert("O campo 'Previsão PLD diário' está vazio, esse campo não pode ficar vazio.")

    } else if (isNaN(mensalMedio) && isNaN(AcimaPiso) || isNaN(horarioMediaDiaria) && isNaN(AcimaPiso) || isNaN(horarioMediaDiaria) && isNaN(mensalMedio)) {
        alert("Faltou preencher os campos.");

    } else if (isNaN(horas_AcimaPiso)) {
        alert("Por favor, preencha o campo 'horas acima do piso'.");
    
    }else if (isNaN(mensalMedio)) {
        resposta1 = ((diasExistentesNoMes - AcimaPiso) * PLDdiario + AcimaPiso * horarioMediaDiaria) / diasExistentesNoMes
        resposta1 = resposta1.toFixed(2); 
        PLDmensalMedio.value = resposta1;
        return resposta1

    }else if (isNaN(AcimaPiso)) {
        resposta2 = (mensalMedio - PLDdiario)*diasExistentesNoMes/(horarioMediaDiaria-PLDdiario)
        resposta2 = resposta2.toFixed(2); 
        diasAcimaPiso.value = resposta2;
        document.getElementById('divAtualizarTabelaPLD').style.display = 'block';
        document.getElementById('divAtualizarTabelaPLD').style.display = 'flex';
        document.getElementById('divAtualizarTabelaPLD').style.justifyContent = 'center';
        document.getElementById('divAtualizarTabelaPLD').style.alignItems = 'center';
        return resposta2

    }else if (isNaN(horarioMediaDiaria)) {
        resposta3 = ((mensalMedio * diasExistentesNoMes - PLDdiario *diasExistentesNoMes)/AcimaPiso)+PLDdiario
        resposta3 = resposta3.toFixed(2); 
        PLDhorarioMediaDiaria.value = resposta3;
        document.getElementById('divAtualizarTabelaPLD').style.display = 'block';
        document.getElementById('divAtualizarTabelaPLD').style.display = 'flex';
        document.getElementById('divAtualizarTabelaPLD').style.justifyContent = 'center';
        document.getElementById('divAtualizarTabelaPLD').style.alignItems = 'center';

        calcular_CMO(resposta3);

        return resposta3
    }

    });



    function calcular_CMO(resposta3){
        const valores = capturarValores();
        const PLDdiario = valores[0];
        const mensalMedio = valores[1];
        const AcimaPiso = valores[2];
        const horarioMediaDiaria = valores[3];
        const resultados = capturarCMO();
        const cmoacimaDoPiso = resultados[0];
        const horas_AcimaPiso = resultados[1];

        let CMOacimaDoPiso = document.getElementById("CMOacimaDoPiso");
        resposta4 = (horarioMediaDiaria*24 - PLDdiario * (24 - horas_AcimaPiso))/horas_AcimaPiso;
        resposta4 = resposta4.toFixed(2); 
        CMOacimaDoPiso.value = resposta4;

    }

    PLDhorarioMediaDiaria.addEventListener("click", function(){
        let CMOacimaDoPiso = document.getElementById("CMOacimaDoPiso");
        CMOacimaDoPiso.value = '';
    })

    PLDhorarioMediaDiaria.addEventListener("change", function() {

        const valores = capturarValores();
        const PLDdiario = valores[0];
        const mensalMedio = valores[1];
        const AcimaPiso = valores[2];
        const horarioMediaDiaria = valores[3];
        const resultados = capturarCMO();
        const cmoacimaDoPiso = resultados[0];
        const horas_AcimaPiso = resultados[1];
    
        const diasNoMes = calcularNumeroDiasMes();
        const diasExistentesNoMes = diasNoMes;
   
        if (isNaN(horarioMediaDiaria)) {
             let CMOacimaDoPiso = document.getElementById("CMOacimaDoPiso");
             CMOacimaDoPiso.value = '';

        }else if (isNaN(cmoacimaDoPiso)) {
            resposta4 = (horarioMediaDiaria*24 - PLDdiario * (24 - horas_AcimaPiso))/horas_AcimaPiso;
            resposta4 = resposta4.toFixed(2); 
            CMOacimaDoPiso.value = resposta4;
            return resposta4;

        } 
    });

    const horasAcimaPiso = document.getElementById("horasAcimaPiso");
    horasAcimaPiso.addEventListener("click", function(){
        let CMOacimaDoPiso = document.getElementById("CMOacimaDoPiso");
        CMOacimaDoPiso.value = '';
    })


    horasAcimaPiso.addEventListener("change", function() {

        const valores = capturarValores();
        const PLDdiario = valores[0];
        const mensalMedio = valores[1];
        const AcimaPiso = valores[2];
        const horarioMediaDiaria = valores[3];
        const resultados = capturarCMO();
        const cmoacimaDoPiso = resultados[0];
        const horas_AcimaPiso = resultados[1];
    
        const diasNoMes = calcularNumeroDiasMes();
        const diasExistentesNoMes = diasNoMes;
    
        if (isNaN(horas_AcimaPiso)) {
             let CMOacimaDoPiso = document.getElementById("CMOacimaDoPiso");
             CMOacimaDoPiso.value = '';

        }else if (isNaN(cmoacimaDoPiso)) {
            resposta4 = (horarioMediaDiaria * 24 - PLDdiario * (24 - horas_AcimaPiso))/horas_AcimaPiso;
            resposta4 = resposta4.toFixed(2); 
            CMOacimaDoPiso.value = resposta4;
            return resposta4;

        } 
    });


    function atualizarTabela_pld(dias_Acima_Piso, pld_horario_acimaPiso) {

        document.getElementById('loading-spinner').style.display = 'block';
        document.getElementById('loading').style.display = 'block';
        const valoresPLD_Tabela = []; // Uma lista para armazenar os valores

        const dataAtual = new Date();
        const diaAtual = dataAtual.getDate();

        const data = document.getElementById('data').value;
        const dataDividida = data.split('-');
        const ano = dataDividida[0];
        const mes = dataDividida[1];
        const dia = '01';

        // Crie a string no formato "YYYYMMDD"
        const dataFormatada = `${ano}${mes}${dia}`;
        const diasNoMes = new Date(ano, mes, 0).getDate();

        const tabela = document.getElementById('tabela');
        const linhas = tabela.getElementsByTagName('tr');

        
        // Coleta todos os valores presentes na coluna "Valor (R$)" antes de começar a atualização
        for (let j = 1; j < linhas.length; j++) {
            const colunas = linhas[j].getElementsByTagName('td');
            const valorColuna = parseFloat(colunas[1].innerText);
            valoresPLD_Tabela.push(isNaN(valorColuna) ? 0 : valorColuna);
        }
    
        for (let i = 1; i <= dias_Acima_Piso; i++) {
            const diaFuturo = diaAtual + i;
            for (let j = 1; j < linhas.length; j++) {
                const colunas = linhas[j].getElementsByTagName('td');
                const dataLinha = colunas[0].innerText.split('/');
                const diaTabela = parseInt(dataLinha[0]);
    
                if (diaTabela === diaFuturo) {
                    // Encontrou uma correspondência, atualize o valor
                    const valorAtualizado = pld_horario_acimaPiso.toFixed(2);
                    colunas[1].innerText = valorAtualizado;
                    valoresPLD_Tabela[j - 1] = parseFloat(valorAtualizado);
                }
           
        }
    }

    setTimeout(function () {
        document.getElementById('loading-spinner').style.display = 'none';
        document.getElementById('loading').style.display = 'none';
    }, 500); // 500 milissegundos = 1/2 segundo

    const mediaPLDmensalInicial = calcularMediaPLDmensal(valoresPLD_Tabela).toFixed(2);
    document.getElementById('PLDmensalMedio').value = mediaPLDmensalInicial;

    }

    // Evento de clique para o botão atualizarTabelaPLD
    const botaoatualizarTabelaPLD = document.getElementById("atualizarTabelaPLD");
    botaoatualizarTabelaPLD.addEventListener('click', function () {
        const valores = capturarValores();
        var dias_Acima_Piso = valores[2];
        var pld_horario_acimaPiso = valores[3];

        // Verifica se os valores não são NaN antes de chamar a função
    if (!isNaN(dias_Acima_Piso) && !isNaN(pld_horario_acimaPiso)) {
        atualizarTabela_pld(dias_Acima_Piso, pld_horario_acimaPiso);
    } else {
        // Mostra um alerta se os valores forem NaN
        alert("Os valores no campo 'dias acima do piso' ou 'PLD horário (média diária)' está invalido. Por favor, ajuste e clique novamente");
    }

    });
    


