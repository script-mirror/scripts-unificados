// import './assets/main.css'

import { createApp } from 'vue'
import App from './App.vue'

import "leaflet/dist/leaflet.css";

import 'bootstrap/dist/css/bootstrap.min.css';

import 'bootstrap/dist/js/bootstrap.bundle.min.js';
import 'vue-multiselect/dist/vue-multiselect.css';

import $ from 'jquery';
window.$ = $;

createApp(App).mount('#app')
