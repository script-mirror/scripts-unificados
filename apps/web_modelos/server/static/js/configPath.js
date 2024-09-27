
const { createApp, ref } = Vue

const data = {
        // path_home: "",
        // path_dropbox: "",
        // path_wx2: "",
        path_arquivo: "",
    }

createApp({
  data() {
    return data
  }, 
    methods:{
    
    },
    computed: {
        path_windows: function () {
            return this.path_arquivo.replace(/\\\\/g, "\\")
        },
        path_linux: function () {
            return this.path_windows.replace(/\\/g, "/").replace(/C:/g, "").replace(/\/Users\/cs[0-9]{6}/g, '/home/admin')
        }
    },
    mounted() {
    },
    delimiters: ['[[',']]']
}).mount('#app')
