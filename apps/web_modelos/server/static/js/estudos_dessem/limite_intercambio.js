function plotarLineIntercambio(ctx, dados, nome) {
        const labels = ["limite_superior", "limite_inferior", "limite_utilizado"];
        let datasets = [];
        for(let label of labels){
            datasets.push(new Dataset(label, dados[label], false, coresDessem[label], "line"))
        }
        const data = {
            datasets: datasets
        };
        const config = {
            type: "line", data: data, options: new Options("Data/Hora", nome, false)
        };
        return new Chart(ctx, config);
}

async function loadLimiteIntercambio(){
    let navMain = document.getElementById('nav-main');
    // navMain.style.display = 'none';
    await criarSelectOption();
    const intercambio = document.getElementById("line-limite-intercambio");
    const datas = await getDatas()
    let dataInicial = datas['dt_inicial'];
    let dataFinal = datas['dt_final'];
    const dados = await getDadosDessem("limites-intercambio", dataInicial, dataFinal);
    charts.push(plotarLineIntercambio(intercambio, dados));
    
    
    // navMain.style.display = 'flex';
}

async function filtrarLimiteIntercambio(inicio, fim){
    let navMain = document.getElementById('nav-main');
    // navMain.style.display = 'none';
    const dados = await getDadosDessem("limites-intercambio", inicio, fim);

    const lineIntercambio = Chart.getChart("line-limite-intercambio");


    lineIntercambio["config"]["_config"]["data"]["datasets"][0]["data"] = dados["limite_superior"];
    lineIntercambio["config"]["_config"]["data"]["datasets"][1]["data"] = dados["limite_inferior"];
    lineIntercambio["config"]["_config"]["data"]["datasets"][2]["data"] = dados["limite_utilizado"];

    lineIntercambio.update();
    // navMain.style.display = 'flex';
}

async function getNomesRe(){
    const promise = await fetch(`/middle/API/get/nomes-re`);
    const response = await promise.json()
    return response
}

async function listarNomesRe(){

    $('#select-nome-re').empty();    
    
    nomesRe["nomesRE"].forEach(nome => {
        option = document.createElement("option");
        option.innerText = nome;    
        option.value = nome;
        select.appendChild(option);
        
    });
    
    let firstOptionValue = $('#select-nome-re option:first').val();

    $('#select-nome-re').val(firstOptionValue);
    $(select).trigger('change');
    $('#select-nome-re').selectpicker('refresh');

}

async function criarSelectOption() {
    $("#select-nome-re").empty();
    let select = document.getElementById("select-nome-re");
    let nomesRe = await getNomesRe();
    nomesRe["nomesRE"].forEach((nome)=>{
        let option = document.createElement("option");
        option.value = nome;
        option.textContent = nome;
        $("#select-nome-re").append(option);

    })

    $("#select-nome-re").selectpicker("refresh");
    let firstOptionValue = $('#select-nome-re option:first').val();

    $('#select-nome-re').val(firstOptionValue);
    $(select).trigger('change');
//   let firstOptionValue = $("#select-nome-re option:first").val();
//   $("#select-nome-re").val() = firstOptionValue;
    
  }


function getRe(){
    return document.getElementById('select-nome-re').value
}