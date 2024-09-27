<template>
    <div class="container filtro">
        <h4>Dados de entrada</h4>
        <div style="padding: 0; margin: 0; display: flex; flex-direction: row;">
            <form class="form-filtro">
                <div class="form-group">
                    <label for="dataTabela">Inicio data ref:</label>
                    <input type="date" id="dataTabela" class="form-control" v-model="dataTabela"
                        @change="atualizarTabelaMascara">
                    <label for="qtdDiasPrevisao">Quantidade de dias:</label>
                    <input type="number" id="qtdDiasPrevisao" class="form-control" v-model="qtdDiasPrevisao"
                        @change="atualizarTabelaMascara">

                </div>

                <div class="form-group">

                    <label for="dtRodadas">Rodadas do dia: </label>
                    <input type="date" class="form-control" id="dtRodadas" @change="atualizarDtRodada"
                        v-model="dtRodadas">
                    <multiselect id="modelos" @select="ajustarDatas" v-model="selectedOption" :options="options"
                        :multiple="false" :close-on-select="true" :clear-on-select="false" :preserve-search="true"
                        placeholder="Selecionar Modelo" label="nome" track-by="nome">
                    </multiselect>
                </div>
                <div class="form-group">
                    <label for="diasPrevisao"> Quantidade de dias previstos </label>
                    <input type="number" id="diasPrevisao" class="form-control" v-model="numDiasPrevisao">
                </div>
                <div class="form-group">
                    <label for="nomeModeloCombinado"></label>
                    <input type="text" class="form-control" id="nomeModeloCombinado" v-model="nomeModeloCombinado">
                </div>
                <div class="from-group">

                    <label for="dtInicial"> de </label>
                    <input type="date" id="dtInicial" class="form-control" v-model="dtInicial">

                    <label for="dtFinal"> até </label>
                    <input type="date" id="dtFinal" class="form-control" v-model="dtFinal">

                    <button type="button" class="btn btn-secondary" @click="adicionarQueryObj">Adicionar</button>
                    <button type="button" class="btn btn-secondary"
                        @click="() => { queryObj = []; nomeModeloCombinado = '' }">Limpar</button>
                    <span class="spinner-border" id="loading" role="status"></span>

                </div>


                <button type="button" class="btn btn-secondary" @click="fetchPrevisaoChuvaPorId">Gerar</button>
                <button type="button" class="btn btn-secondary" @click="plotarMapas">Plotar Mapas</button>

                <button type="button" class="btn btn-secondary" @click="inserirChuva">Inserir chuva</button>
                <button type="button" class="btn btn-secondary" @click="rodarSmap">Rodar SMAP</button>

                <div>{{ queryObj }}</div>
            </form>
            <div>
                <fieldset @change="(function () { resultadoChuva = {} })()">
                    <legend>Granularidade:</legend>
                    <div>
                        <input class="form-check-label" type="radio" name="granularidade" id="subbacia" value="subbacia"
                            v-model="granularidadeSelecionada">
                        <label for="subbacia">Subbacia</label>
                    </div>
                    <div>
                        <input class="form-check-label" type="radio" name="granularidade" id="bacia" value="bacia"
                            v-model="granularidadeSelecionada">
                        <label for="bacia">Bacia</label>
                    </div>
                    <div>
                        <input class="form-check-label" type="radio" name="granularidade" id="submercado"
                            value="submercado" v-model="granularidadeSelecionada">
                        <label for="submercado">Submercado</label>
                    </div>

                </fieldset>
            </div>
            <!-- </form> -->
        </div>
    </div>

    <div>
        <table id="tabela-smap">
            <tr>
                <th @click="(function () { console.log(resultadoChuva) })()">#</th>
                <th>Data ref</th>
                <th>Modelo</th>
                <th>Data rodada</th>
                <th>Data prevista</th>
                <th v-for="(i) in granularidade[granularidadeSelecionada]">{{ i['nome'] }}</th>

            </tr>
            <tr v-for="(linha, idx) in tabelaMascara">
                <td>{{ `d+${idx + 1}` }}</td>
                <td>{{ linha['dt'] }}</td>
                <td>{{ `${linha['Modelo']['nome']}` }}</td>
                <td>{{ linha['dtInicial'] }}</td>
                <td>{{ linha['dtPrevisao'] }}</td>
                <td style="text-align: center;" v-for="(i) in granularidade[granularidadeSelecionada]"
                    :key="linha['dt']"
                    :class="resultadoChuva[linha['dt']] == undefined ? '' : gerarCor((Number(resultadoChuva[linha['dt']][i['id']]['vl_chuva'])).toFixed(1))">
                    {{ resultadoChuva[linha['dt']] == undefined ? '' :
                        Number(resultadoChuva[linha['dt']][i['id']]['vl_chuva']).toFixed(1) }}
                </td>
            </tr>

        </table>
        <div class="d-flex mb-5">
            <div class="d-mapa">
                <h4>Sem. 1</h4>
                <Mapa ref='sem1' idMapa="sem1" />
            </div>
            <div class="d-mapa">
                <h4>Sem. 2</h4>
                <Mapa ref='sem2' idMapa="sem2" />
            </div>
            <div class="d-mapa">
                <h4>Sem. 3</h4>
                <Mapa ref='sem3' idMapa="sem3" />
            </div>
        </div>
    </div>

</template>

<script>
import moment from 'moment';
import 'moment/dist/locale/pt-br';
import Multiselect from 'vue-multiselect';

import { API_ENDPOINTS } from '@/config/endpoints.config';
import Mapa from './Mapa.vue';
import axios from "axios";
axios.defaults.withCredentials = true;




export default {
    components: { Multiselect, Mapa },
    data() {
        return {
            nomeModelo: '',
            dataTabela: '',
            modeloSelecionado: '',
            modelosDisponiveis: {},
            dtRodadas: '',
            dtInicial: '',
            dtFinal: '',
            qtdDiasPrevisao: 18,
            tabelaMascara: [],
            resultadoChuva: {},
            selectedOption: [],
            options: [
            ],
            granularidade: {
                "submercado": [],
                "bacia": [],
                "subbacia": []
            },
            granularidadeSelecionada: 'subbacia',
            queryObj: [],
            prevChuvaRes: [{}],
            momentPlaceholder: moment(),
            numDiasPrevisao: 7,
            nomeModeloCombinado: '',
            loadings: []
        };
    },
    async mounted() {
        this.dtRodadas = moment().format('YYYY-MM-DD');
        this.dataTabela = moment(this.dtRodadas).add(1, 'days').format('YYYY-MM-DD');
        this.atualizarTabelaMascara();

        this.fetchModelosDoDia();
        this.granularidade = {
            "submercado": await this.fetchGenerico('submercados'),
            "bacia": await this.fetchGenerico('bacias', { 'divisao': 'tb_chuva' }),
            "subbacia": await this.fetchGenerico('subbacias')
        };
        this.$refs.sem1.setupLeafletMap();
        this.$refs.sem2.setupLeafletMap();
        this.$refs.sem3.setupLeafletMap();
    },
    methods: {


        json2Table(json) {
            let cols = Object.keys(json[0]);


            let headerRow = cols
                .map(col => `<th>${col}</th>`)
                .join("");

            let rows = json
                .map(row => {
                    let tds = cols.map(col => `<td>${row[col]}</td>`).join("");
                    return `<tr>${tds}</tr>`;
                })
                .join("");

            //build the table
            const table = `
                <table>
                    <thead>
                        <tr>${headerRow}</tr>
                    <thead>
                    <tbody>
                        ${rows}
                    <tbody>
                <table>
        `;

            return table;
        },

        gerarCor(value) {
            const cores = ["c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12", "c13", "c14", "c15"]
            return value > 200 ? cores[14] :
                value > 150 ? cores[13] :
                    value > 100 ? cores[12] :
                        value > 75 ? cores[11] :
                            value > 50 ? cores[10] :
                                value > 40 ? cores[9] :
                                    value > 30 ? cores[8] :
                                        value > 25 ? cores[7] :
                                            value > 20 ? cores[6] :
                                                value > 15 ? cores[5] :
                                                    value > 10 ? cores[4] :
                                                        value > 5 ? cores[3] :
                                                            value > 1 ? cores[2] :
                                                                value > 0 ? cores[1] :
                                                                    cores[0];
        },
        fetchModelosDoDia() {
            let loading = document.getElementById('loading')
            loading.style.display = 'block';
            this.loadings.push('')
            axios.get(`${API_ENDPOINTS.rodadasDoDia}?dt=${this.dtRodadas}`)
                .then(response => {
                    this.options = [];
                    this.loadings.pop();
                    if (this.loadings.length == 0) {
                        loading.style.display = 'none';
                    }
                    response.data.forEach((rodada) => {
                        let nomeModelo = `${rodada.str_modelo} ${String(rodada.hr_rodada).padStart(2, '0')}z`;
                        this.options.push({ 'nome': nomeModelo, 'id_chuva': rodada.id_chuva });
                    });
                });
        },

        adicionarQueryObj() {
            this.queryObj.push(new PesquisaPrevisaoChuva(this.selectedOption['id_chuva'], this.dtInicial, this.dtFinal));
            try {

                let dtInicio = moment(this.dtInicial)
                let dtFim = moment(this.dtFinal)
                this.nomeModeloCombinado += this.nomeModeloCombinado == '' ? this.selectedOption['nome'].replaceAll(' ', '') : '-' + this.selectedOption['nome'].replaceAll(' ', '')
                while (dtInicio <= dtFim) {
                    let objetosFiltrados = this.tabelaMascara.filter(objeto => objeto.dt == dtInicio.format('DD/MM/YY'));
                    objetosFiltrados[0].Modelo = this.selectedOption
                    objetosFiltrados[0].dtInicial = moment(this.dtRodadas).format('DD/MM/YY')
                    objetosFiltrados[0].dtPrevisao = dtInicio.format('DD/MM/YY')
                    dtInicio.add(1, 'days')
                }



            } catch (error) {
                console.error(error);
            }
        },

        fetchPrevisaoChuvaPorId() {
            let loading = document.getElementById('loading')
            loading.style.display = 'block';
            this.loadings.push('')

            axios.post(`${API_ENDPOINTS.previsaoChuva[this.granularidadeSelecionada]}`,
                this.queryObj
            )
                .then(response => {
                    this.loadings.pop();
                    if (this.loadings.length == 0) {
                        loading.style.display = 'none';
                    }
                    response.data.forEach((prev) => {
                        const data = moment(prev['dt_prevista']).format('DD/MM/YY')
                        this.resultadoChuva[data] == undefined ? this.resultadoChuva[data] = {} : null;
                        this.resultadoChuva[data][prev['id']] = { 'vl_chuva': prev['vl_chuva'] };
                        // this.resultadoChuva[prev['id']] === undefined ? this.resultadoChuva[prev['id']] = [{'vl_chuva':prev['vl_chuva']}] : this.resultadoChuva[prev['id']].push({'vl_chuva':prev['vl_chuva']})
                    });
                });
        },
        atualizarDtRodada() {
            this.dataTabela = moment(this.dtRodadas).add(1, 'days').format('YYYY-MM-DD');
            this.atualizarTabelaMascara();
            this.fetchModelosDoDia();
        },
        atualizarTabelaMascara() {
            this.tabelaMascara = []
            let dtRef = moment(this.dataTabela)
            this.resultadoChuva = {}
            for (let i = 0; i < this.qtdDiasPrevisao; i++) {
                this.tabelaMascara.push({ 'dt': dtRef.format('DD/MM/YY'), 'Modelo': { 'nome': '' }, 'dtInicial': '', 'dtPrevisao': '' })
                dtRef.add(1, 'days')
            }
        },
        ajustarDatas() {

            this.dtInicial = this.dtInicial == '' ? moment(this.dtRodadas).add(1, 'days').format('YYYY-MM-DD') : moment(this.dtFinal).add(1, 'days').format('YYYY-MM-DD')

            this.dtFinal = moment(this.dtInicial).add(this.numDiasPrevisao-1, 'days').format('YYYY-MM-DD') > moment(this.tabelaMascara[this.tabelaMascara.length - 1].dt, 'DD/MM/YY').format('YYYY-MM-DD')
                ? moment(this.tabelaMascara[this.tabelaMascara.length - 1].dt, 'DD/MM/YY').format('YYYY-MM-DD') : moment(this.dtInicial).add(this.numDiasPrevisao-1, 'days').format('YYYY-MM-DD');

        },

        chuvaTabelaToCricao() {
            this.prevChuvaRes.forEach((obj) => {
                obj.cd_subbacia = obj.id;
                obj.id = '1';
                obj.modelo = this.nomeModeloCombinado;

                delete obj.hr_rodada;
                delete obj.dia_semana;
                delete obj.semana;
            })
        },
        inserirChuva() {
            let loading = document.getElementById('loading')
            loading.style.display = 'block';
            this.loadings.push('')

            axios.post(`${API_ENDPOINTS.previsaoChuva['subbacia']}`,
                this.queryObj
            )
                .then(response => {
                    this.loadings.pop();
                    if (this.loadings.length == 0) {
                        loading.style.display = 'none';
                    }
                    response.data.forEach((obj) => {
                        obj.cd_subbacia = obj.id;
                        obj.id = '1';
                        obj.modelo = this.nomeModeloCombinado;

                        delete obj.hr_rodada;
                        delete obj.dia_semana;
                        delete obj.semana;
                    })

                    loading.style.display = 'block';
                    this.loadings.push('')
                    axios.post(`${API_ENDPOINTS.previsaoChuva['post']}`,
                        response.data
                    ).then(() => {
                        this.loadings.pop();
                        if (this.loadings.length == 0) {
                            loading.style.display = 'none';
                        }
                    })
                });


        },
        rodarSmap() {
            let loading = document.getElementById('loading')
            loading.style.display = 'block';
            this.loadings.push('')
            axios.post(`${API_ENDPOINTS.smap}`,
                {
                    "dt_rodada": this.dtRodadas,
                    "hr_rodada": "0",
                    "str_modelo": this.nomeModeloCombinado
                }
            )
                .then(response => {
                    this.loadings.pop();
                    if (this.loadings.length == 0) {
                        loading.style.display = 'none';
                    }
                    alert(response.data.detail)


                })
                .catch(e => {
                    this.loadings.pop();
                    if (this.loadings.length == 0) {
                        loading.style.display = 'none';
                    }
                    alert(e.response.data.detail)
                })
        },
        async fetchGenerico(recurso, q) {
            q = new URLSearchParams(q)
            let loading = document.getElementById('loading');
            loading.style.display = 'block';
            const promise = await fetch(`${API_ENDPOINTS[recurso]}?${q}`, {
                method: "GET",
                credentials: "include"
            });
            loading.style.display = 'none';
            return await promise.json();
        },
        plotarMapas() {
            let chuvaSem1 = {};
            let chuvaSem2 = {};
            let chuvaSem3 = {};
            let loading = document.getElementById('loading')
            loading.style.display = 'block';
            this.loadings.push('')
            axios.post(`${API_ENDPOINTS.previsaoChuva['subbacia']}`,
                this.queryObj
            )
                .then(response => {
                    this.loadings.pop();
                    if (this.loadings.length == 0) {
                        loading.style.display = 'none';
                    }
                    response.data.forEach((prev) => {
                        if (prev['semana'] == 1) {
                            chuvaSem1[prev['id']] = chuvaSem1[prev['id']] == undefined ? { 'vl_chuva': prev['vl_chuva'] } : { 'vl_chuva': chuvaSem1[prev['id']]['vl_chuva'] + prev['vl_chuva'] };

                        } else if (prev['semana'] == 2) {
                            chuvaSem2[prev['id']] = chuvaSem2[prev['id']] == undefined ? { 'vl_chuva': prev['vl_chuva'] } : { 'vl_chuva': chuvaSem2[prev['id']]['vl_chuva'] + prev['vl_chuva'] };

                        } else if (prev['semana'] == 3) {
                            chuvaSem3[prev['id']] = chuvaSem3[prev['id']] == undefined ? { 'vl_chuva': prev['vl_chuva'] } : { 'vl_chuva': chuvaSem3[prev['id']]['vl_chuva'] + prev['vl_chuva'] };

                        }


                        // this.resultadoChuva[prev['id']] === undefined ? this.resultadoChuva[prev['id']] = [{'vl_chuva':prev['vl_chuva']}] : this.resultadoChuva[prev['id']].push({'vl_chuva':prev['vl_chuva']})
                    });
                    this.$refs.sem1.carregarBacias(chuvaSem1)
                    this.$refs.sem2.carregarBacias(chuvaSem2)
                    this.$refs.sem3.carregarBacias(chuvaSem3)

                });
            // this.$refs.sem3.carregarBacias(this.chuvaSem3)
        }

    },




};
class PesquisaPrevisaoChuva {
    id;
    dt_inicio;
    dt_fim;
    constructor(id, dt_inicio, dt_fim) {
        this.id = id;
        this.dt_inicio = dt_inicio;
        this.dt_fim = dt_fim;
    }
}
</script>

<style>
.multiselect {
    width: 50% !important;
}

input[type=number] {
    width: 60px !important;
}

input[type=date] {
    width: 24% !important;
}

table,
td,
th {
    font-family: 'Consolas' !important;
    border: 2px solid #ffffff !important;
}

td,
th {
    padding: 2px !important;
    min-width: 30px !important;
    white-space: nowrap;

}

tr:nth-child(even) {
    background-color: #f3f3f3 !important;
}

td[color_code='1'] {
    background-color: #ff0000 !important
}

table {
    border-collapse: collapse !important;
}

.filtro {
    border: 1px solid #0001;
    padding: 20px;
}

.form-filtro * {
    margin: 5px;
}

.form-control {
    display: inline !important;
}

.d-mapa {
    border: 1px solid #E1E1E100
}

.c1 {
    background-color: #ffffff
}

.c2 {
    background-color: #e1ffff
}

.c3 {
    background-color: #b3f0fb
}

.c4 {
    background-color: #95d2f9
}

.c5 {
    background-color: #2585f0;
    color: white
}

.c6 {
    background-color: #0c68ce;
    color: white
}

.c7 {
    background-color: #73fd8b
}

.c8 {
    background-color: #39d52b;
    color: white
}

.c9 {
    background-color: #3ba933;
    color: white
}

.c10 {
    background-color: #ffe67b
}

.c11 {
    background-color: #FFBD4A
}

.c12 {
    background-color: #fd5c22;
    color: white
}

.c13 {
    background-color: #B91D22;
    color: white
}

.c14 {
    background-color: #F7596F
}

.c15 {
    background-color: #a9a9a9
}
</style>
