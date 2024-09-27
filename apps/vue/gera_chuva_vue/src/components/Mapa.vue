<template>
    <div id="container">
        <div :id="idMapa" class="mapas"></div>
    </div>
</template>

<script>
import { ref, onMounted } from 'vue';
import "leaflet/dist/leaflet.css";
import L from "leaflet";


export default {
    name: "Map",
    props: {
        idMapa: String,
        chuva: Object
    },

    data() {
        return {
            mapa: undefined,
            geoJsonLayer: undefined,
            subbacias: undefined,
            coresIndicadores: ["#ffffff", "#e1ffff", "#b3f0fb", "#95d2f9", "#2585f0", "#0c68ce", "#73fd8b", "#39d52b", "#3ba933", "#ffe67b", "#FFBD4A", "#fd5c22", "#B91D22", "#F7596F", "#a9a9a9"]

        }
    },
    methods: {
        setupLeafletMap() {
            let id = this.idMapa;
            let zoomSetting = 3;

            this.mapa = L.map(id, {
                center: [-14.24, -60.18],
                zoom: zoomSetting,
                worldCopyJump: true,
                zoomControl: false,
                attributionControl: false,
            });
            L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
                maxZoom: 18,
                attribution: '&copy; <a href="http://www.openstreetmap.org/copyright">OpenStreetMap</a>'
            }).addTo(this.mapa);

        },
        carregarBacias(chuva) {
            fetch("/static/js/subbacias2.json").then((promise) => {
                promise.json().then((response) => {
                    this.subbacias = response;
                    for (let feature in this.subbacias["features"]) {
                        this.subbacias['features'][feature]['properties']['vl_chuva'] = chuva[this.subbacias['features'][feature]['id']]['vl_chuva']
                    }
                    console.log(this.subbacias)

                    this.plotarMapa(this.subbacias)
                })
            })
        },
        plotarMapa(data) {
            this.geoJsonLayer = L.geoJSON(data, {
                onEachFeature: this.onEachFeature,
                style: this.style
            }).addTo(this.mapa);
        },
        getColor(mm_chuva) {
            return mm_chuva > 200 ? this.coresIndicadores[14] :
                mm_chuva > 150 ? this.coresIndicadores[13] :
                    mm_chuva > 100 ? this.coresIndicadores[12] :
                        mm_chuva > 75 ? this.coresIndicadores[11] :
                            mm_chuva > 50 ? this.coresIndicadores[10] :
                                mm_chuva > 40 ? this.coresIndicadores[9] :
                                    mm_chuva > 30 ? this.coresIndicadores[8] :
                                        mm_chuva > 25 ? this.coresIndicadores[7] :
                                            mm_chuva > 20 ? this.coresIndicadores[6] :
                                                mm_chuva > 15 ? this.coresIndicadores[5] :
                                                    mm_chuva > 10 ? this.coresIndicadores[4] :
                                                        mm_chuva > 5 ? this.coresIndicadores[3] :
                                                            mm_chuva > 1 ? this.coresIndicadores[2] :
                                                                mm_chuva > 0 ? this.coresIndicadores[1] :
                                                                    this.coresIndicadores[0];
        },
        onEachFeature(feature, layer) {
            layer.on('mouseover', (e) => {
                layer.bindTooltip(`${this.formatarDadosComoTabela(feature.properties)}`).openTooltip();
            });
        },
        style(feature) {
            return {
                fillColor: this.getColor(feature['properties']['vl_chuva']),
                weight: "1",
                opacity: 1,
                color: "black",
                dashArray: "1",
                fillOpacity: 0.7
            }
        },
        formatarDadosComoTabela(dados) {
            let tabela = '<table>';
            for (let chave in dados) {
                tabela += '<tr><td>' + chave + '</td><td>' + dados[chave] + '</td></tr>';
            }
            tabela += '</table>';
            return tabela;
        }
    },

    mounted() {


    },
};
</script>

<style scoped>
.mapas {
    display: flex;
    width: 25vw;
    height: 65vh;
}
</style>