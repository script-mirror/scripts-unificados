
let chartDataArray = [];

$(document).ready(function () {
  anychart.theme("darkGlamour");

  // Função para pegar o evento de click
  // function handleItemClick(event) {
  //   event.preventDefault();
  //   const item = $(this).text();

  //   if (selectedItems.includes(item)) {
  //     // Deselect item
  //     selectedItems = selectedItems.filter(i => i !== item);
  //     $(this).removeClass("selected");
  //   } else if (selectedItems.length < 3) {
  //     // Select item
  //     selectedItems.push(item);
  //     $(this).addClass("selected");
  //   } else {
  //     alert("Você só pode selecionar até 3 produtos.");
  //   }
  // }

  // adicionando de forma automatica todos os produtos existente no banco de dados no menu dropdown
  $.ajax({
    method: "GET",
    url: "/middle/API/get/produtos-bbce",
    data: {},
  }).done(function (objeto) {
    const dropdownMenu = $("#dynamicDropdown");
    const dropdownMenu2 = $("#dynamicDropdown2");
    objeto.forEach(row => {
      const produtoBBCE = $("<option>", {
        value: row["str_produto"],
        href: "#",
        text: row["str_produto"]
      });
      dropdownMenu.append(produtoBBCE);
      dropdownMenu2.append(produtoBBCE.clone(true));
    });
    $('#dynamicDropdown').selectpicker('refresh');
    $('#dynamicDropdown2').selectpicker('refresh');

    // Salva o produto selecionado do dropdown
    window.objetoPalavrasSelecionadas = objeto;
  });


  $('#plotButton').on('click', function () {
    $('#loadingIndicator').show();
    const selectedItems = [$("#dynamicDropdown").val(), $("#dynamicDropdown2").val()]
    fetchChartData(selectedItems);

  });

  async function fetchChartData(produtos) {
    chartDataArray = []; //iniciando a lista de graficos
    const promises = [];

    produtos.forEach(produto => { 

      const promise = new Promise((resolve, reject) =>{
        $.ajax({
          method: "GET",
          dataType: "json",
          url: `/middle/API/get/bbce/resumos-negociacoes?produto=${produto}&categoria_negociacao=Mesa`,
        }).done(function (response) {
          if (Array.isArray(response)) {
            processamentoDadosGrafico(produto, response);
          } else if (response && response.data && Array.isArray(response.data)) {
            processamentoDadosGrafico(produto, response.data);
          } else {
            alert("Formato recebido inválido, verificar dados recebidos");
          }
          resolve(response);
        }).fail(function (jqXHR, textStatus, errorThrown) {
          console.error("falha requisição AJAX:", textStatus, errorThrown);
          alert("Falha na requisição. Verificar o banco de dados");
          reject(errorThrown)
        });

      });
      promises.push(promise)
    });

    await Promise.all(promises);
    const data = await getSpread(produtos[0], produtos[1]);
    console.log(data)
    createLineChart(data);

  }
  async function getSpread(produto1, produto2){
    const promise = await fetch(`/middle/API/get/bbce/resumos-negociacoes/spread/preco-medio?produto1=${produto1}&produto2=${produto2}&categoria_negociacao=Mesa`);
    return await promise.json();
  }

  function processamentoDadosGrafico(produto, data) {
    const chartData = data.map(item => ({
      date: item.date,
      open: item.open,
      high: item.high,
      low: item.low,
      close: item.close,
      volume: item.volume
    }));
    const index = produto == $("#dynamicDropdown").val() ? 0 : 1;
    chartDataArray[index] = { title: produto, data: chartData };
    const selectedItems = [$("#dynamicDropdown").val(), $("#dynamicDropdown2").val()]
    if (chartDataArray.length === selectedItems.length) {
      loadChartData(chartDataArray);
      $('#loadingIndicator').hide();
    }
  }

  function createCandlestickChart(containerId, chartData, title) {
    let chart = anychart.stock();


    let plot = chart.plot(0);
    plot
      .yGrid(true)
      .xGrid(true)
      .yMinorGrid(true)
      .xMinorGrid(true);
    plot.height("33%");

    let dataTable = anychart.data.table("date");
    dataTable.addData(chartData);

    let mapping = dataTable.mapAs({
      open: "open",
      high: "high",
      low: "low",
      close: "close",
      value: "volume"
    });

    let series = plot.candlestick(mapping);
    series.name(title);
    series.legendItem().iconType("rising-falling");
    series.fallingFill("#EA4D6455");
    series.fallingStroke("#EA4D64");
    series.risingFill("#72BD7155");
    series.risingStroke("#72BD71");

    // Adicionando valor do EMA à tela conforme o usuário digita o valor

    let volumePlot = chart.plot(2);
    volumePlot
      .yGrid(true)
      .xGrid(true)
      .yMinorGrid(true)
      .xMinorGrid(true)
      .title("Volume");
    volumePlot.height("12%");

    let volumeSeries = volumePlot.column(mapping);
    volumeSeries.name("Volume");


    // Definindo os períodos disponíveis para seleção
    let rangeSelector = anychart.ui.rangeSelector();
    rangeSelector.render(chart);


    chart.container(containerId);
    chart.title(title);
    chart.draw();

    return chart;
  }

  function loadChartData(chartDataArray) {
    // const container = document.getElementById("charts-container");
    // container.innerHTML = '';

     chartDataArray.forEach((item, index) => {
      const chartDiv = document.getElementById(`chart-${index}`);
      chartDiv.innerHTML = '';
      // Cria o gráfico
      createCandlestickChart(chartDiv.id, item.data, item.title);
      // updateChartWidths();
    });


  }


  function createLineChart(data) {
    const container = document.getElementById("chart-2");
    container.innerHTML = '';
    // create an instance of a pie chart
    let table = anychart.data.table('date');

    table.addData(data);

    let mapping = table.mapAs({ x: 'date', value: 'spread' });
    let chart = anychart.stock();
    let series = chart.plot(0).area(mapping);
    series.name("Spread")
    let rangeSelector = anychart.ui.rangeSelector();
    rangeSelector.render(chart);
    chart.title("Produto 2 - Produto 1");
    chart.container("chart-2");
    chart.draw();
  }





});

