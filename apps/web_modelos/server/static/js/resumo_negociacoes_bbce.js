function fetchApi(url, params){
    params = new URLSearchParams(params)
    return $.ajax({
        url: `/api/v2/${url}?${params}`,
        type: 'GET',
        dataType: 'json',
        success: function (data) {
            return data;
        },
        error: function (jqXHR, textStatus, errorThrown) {
            console.error(textStatus)
            console.error(errorThrown)
            return null;
        }
    });
}
function exibirResumo(dados){
    const tbody = $('#table-body');
    tbody.empty();

    for(let i = 0; i < dados.length; i++){
        for(key in dados[i]){
            dados[i][key] = dados[i][key] == null ? "-" : dados[i][key]
        }
        let row = $('<tr>');
        let classeTipo = dados[i]['produto'].includes('MEN') ? 'table-success' : dados[i]['produto'].includes('TRI') ? 'table-warning' : dados[i]['produto'].includes('SEM') ? 'table-danger' : 'table-primary';
        let classeVariacao = isNaN(Number(dados[i]['change_percent'])) ? '' : dados[i]['change_percent'] > 0 ? 'verde' : 'vermelho';
        row.append($('<td>').addClass(classeTipo).text(dados[i]['produto'].slice(0,3)));
        row.append($('<td>').addClass(classeTipo).text(dados[i]['produto'].slice(4, dados[i]['produto'].length)));
        row.append($('<td>').addClass(classeTipo).text(financial(dados[i]['preco_fechamento_anterior'])));
        row.append($('<td>').addClass(classeTipo).text(dados[i]['datetime_fechamento_anterior'] != '-' ? moment(dados[i]['datetime_fechamento_anterior']).format('DD/MM') : '-'));
        row.append($('<td>').addClass(classeTipo).text(financial(dados[i]['close'])));
        row.append($('<td>').addClass(classeTipo).text(dados[i]['hora_fechamento'].slice(0, 5)));
        row.append($('<td>').addClass(classeTipo).text(formatFloat(dados[i]['volume'])));
        row.append($('<td>').addClass(classeTipo).text(dados[i]['total']));
        row.append($('<td>').addClass(classeTipo).addClass(classeVariacao).text(financial(dados[i]['change_value'])));
        row.append($('<td>').addClass(classeTipo).addClass(classeVariacao).text(percent(dados[i]['change_percent'])));

        row.append($('<td>').addClass(classeTipo).text(financial(dados[i]['open'])));
        row.append($('<td>').addClass(classeTipo).text(financial(dados[i]['high'])));
        row.append($('<td>').addClass(classeTipo).text(financial(dados[i]['low'])));


        tbody.append(row);
    }
}



async function atualizarDados(){
    const searchParams = new URL(document.URL);
    const datas = searchParams.searchParams.get('datas') == null ? moment().format('yyyy-MM-DD') : searchParams.searchParams.get('datas');
    let dados = await fetchApi('bbce/resumos-negociacoes/negociacoes-de-interesse', {'datas':datas, 'categoria_negociacao':'Mesa'});
    let ultimoUpdate = await fetchApi('bbce/resumos-negociacoes/negociacoes-de-interesse/ultima-atualizacao', {'categoria_negociacao':'Mesa'})
    $('#momento-atualizacao').text(moment(ultimoUpdate['ultimo_update']).format('DD/MM/yyyy HH:mm:ss'));
    exibirResumo(dados);
}

$(document).ready(async function () {
    await atualizarDados();
    setInterval(atualizarDados, 1000*60*10);
  });

function formatFloat(x){
    return isNaN(Number.parseFloat(x)) ? '-' : (Number.parseFloat(x).toFixed(2)).replace('.', ',');
}

function financial(x) {
    return Number.parseFloat(x) < 0 ? `-R$${formatFloat(Math.abs(x))}` : `R$${formatFloat(x)}`;
  }
function percent(x){
    return formatFloat(x)+'%';
}


  
class NegociacaoVazia{
    constructor(){
        this.change_percent = '-'
        this.change_value = '-'
        this.close = '-'
        this.open = '-'
        this.high = '-'
        this.low = '-'
        this.volume = '-'
        this.date = '-'
        this.produto = '-'
        this.total = '-'
        this.preco_medio = '-'
    }
}