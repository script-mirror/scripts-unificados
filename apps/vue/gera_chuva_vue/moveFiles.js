const fs = require('fs-extra');
const path = require('path');

const rootPath = path.dirname(__dirname)
const webModelosPath = path.join(rootPath, '../web_modelos/server/');

const sourceDir = path.join(__dirname, 'dist');

const assetsDir = path.join(sourceDir, 'assets');
console.log(webModelosPath);
// Move CSS
fs.moveSync(assetsDir, path.join(webModelosPath,'static/assets/gera_chuva'), { overwrite: true });
// Move HTML
fs.moveSync(path.join(sourceDir, 'index.html'), path.join(webModelosPath,'templates/gera_chuva','index.html'), { overwrite: true });

console.log('Arquivos movidos com sucesso!');
