async function TabelaComparacao(modelo){
    const promise = await fetch('/middle/API/get/tabela-diferenca-modelos?data_rodada=27/03/2024&hr_rodada=0&nome_modelo_1=pconjunto&nome_modelo_2=gefs');
    let tabela = await promise.text();
    const parser = new DOMParser();
    tabela = parser.parseFromString(tabela, "text/html")
    let stylesElements = tabela.getElementsByTagName('style')
    let tableStyle = stylesElements[stylesElements.length-1].outerHTML

    $('head').append(tableStyle);
    
    tabela = tabela.body.outerHTML.replace('<body>', '').replace('</body>', '')

    const { createApp, ref } = Vue;
    createApp({
        delimiters : ['[[', ']]'],
        template: tabela
        
    }).mount(`#tabelas-diferenca-previsao`);
}
// TabelaComparacao("")