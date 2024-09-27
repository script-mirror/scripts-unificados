class Slider {
    #sliderElement;
    #datePickerElementInicio;
    #datePickerElementFim;
    #datas;
    sliderSetup;
    filtro;
    constructor(datas, sliderElement, datePickerElementInicio, datePickerElementFim, filtro, rangeInicial) {
      this.rangeInicial = rangeInicial == undefined ? 1 : rangeInicial;
      this.#sliderElement = sliderElement;
      this.#datePickerElementInicio = datePickerElementInicio;
      this.#datePickerElementFim = datePickerElementFim;
      this.#datas = datas;
      this.filtro = filtro;
      this.sliderSetup = this.#criarSlider();
      this.#exibirDatas(this.sliderSetup.value())
    }
  
    #criarSlider() {
      let min = 0;
      let max = this.#datas.length - 1;
      this.#datePickerElementInicio.addEventListener("change", () => this.filtro());
      this.#datePickerElementFim.addEventListener("change", () => this.filtro());
      return rangeSlider(this.#sliderElement, {
        min: min,
        max: max,
        value: [max - this.rangeInicial, max],
        onInput: () => {
          this.#exibirDatas(this.sliderSetup.value())
        },
        onThumbDragEnd: () => {
          this.filtro()
        },
        onRangeDragEnd: () => {
          this.filtro()
        }
      });
    }
  
    #exibirDatas(posicoesSlider) {
      this.#datePickerElementInicio.value = this.#datas[posicoesSlider[0]];
      this.#datePickerElementFim.value = this.#datas[posicoesSlider[1]];
      this.#datePickerElementInicio.setAttribute('min', this.#datas[0]);
      this.#datePickerElementInicio.setAttribute('max', this.#datas[this.#datas.length - 1]);
  
      this.#datePickerElementFim.setAttribute('min', this.#datas[0]);
      this.#datePickerElementFim.setAttribute('max', this.#datas[this.#datas.length - 1]);
    }
  
  }