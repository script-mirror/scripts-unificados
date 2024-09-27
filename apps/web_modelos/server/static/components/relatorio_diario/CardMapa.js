function CardMapa(modelo){
    const { createApp, ref } = Vue
    createApp({
        delimiters : ['[[', ']]'],
        template:`
            <div class="card-body d-flex flex-column justify-content-center" style="height: auto;">
                <form>
                    <div class="form-group">
                        <input id="data-inicial-${modelo}" type="date" name="${modelo}">
                        <label for="data-inicial-${modelo}">Data da rodada</label>
                    </div>
                </form>
                <div id="mapa-${modelo}" class="col-12" style="height: 90%;"></div>
                <div id="mapa-${modelo}-loading" class="spinner-border text-primary m-auto" role="status"
                    style="display: none;"> <span class="sr-only">Loading...</span></div>
            </div>
        `
    }).mount(`#card-mapa-${modelo}`)
}