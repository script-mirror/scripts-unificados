const qrcode = require('qrcode-terminal');
const fs = require('fs');
const util = require('util');
const winston = require('winston');

const { format, transports } = winston;
transports.DailyRotateFile = require('winston-daily-rotate-file');

const timestampFormat = format.combine(
    format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
    format.printf(({ timestamp, level, message }) => {
        return `[${timestamp}] ${level}: ${message}`;
    })
);

// Cria um logger personalizado
const logger = winston.createLogger({
    level: 'info', // Nível de log (pode ser 'error', 'warn', 'info', etc.)
    format: timestampFormat,
    transports: [
        new winston.transports.Console(), // Exibe no console
        new transports.DailyRotateFile({
            filename: 'logs/application-%DATE%.log',
            datePattern: 'YYYY-MM-DD',
            zippedArchive: true,
            // maxSize: '20m', // Tamanho máximo do arquivo (opcional)
            maxFiles: '7d', // Manter logs dos últimos 7 dias (opcional)
        }),
    ]
});

// logger.info('Mensagem de informação');
// logger.error('Mensagem de erro');
// logger.warn('Mensagem de aviso');

try {

    const { Client, LocalAuth, MessageMedia } = require('whatsapp-web.js');
    const client = new Client(
        { 
            authStrategy: new LocalAuth(),
            
            puppeteer: { headless: true,
                args: ['--disable-gpu', '--disable-setuid-sandbox', '--no-sandbox','--no-zygote'] 
            },

        }
    );

    client.on('qr', qr => {
        qrcode.generate(qr, {small: true});
    });


    client.on('ready', () => {
        logger.info('Client is ready!');
        read_sqlite_db_after_30_seconds();
    });

    client.on('message', async message => {
        let chat = await message.getChat();

        logger.info('Mensagem de informação');
        if (message.body == '!ping') {
            message.reply('pong');
        }

        if (message.body.startsWith('!sendtogroup ')) {
            let grupo = message.body.split(' ')[1];
            grupo = grupo.includes('@g.us') ? grupo : `${grupo}@g.us`;
            let messageIndex = message.body.indexOf(grupo) + grupo.length;
            let message_txt = message.body.slice(messageIndex, message.body.length);
            client.sendMessage(grupo, message_txt);
        }

        if (message.body.startsWith('!getGroupId')) {
            if (chat.isGroup) {
                logger.info('*Group Details*')
                logger.info(`Name: ${chat.name}`)
                logger.info(`ID: ${chat.id._serialized}`)
                logger.info(`Participant count: ${chat.participants.length}`)
            }
            else {
                logger.info('This is not a group');
            }

        }

        if (message.body.startsWith('!msgTeste')) {
            file_path = "/WX/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/gerarProdutos/arquivos/tmp/2022-04_20220621_MATRIZ_NE-44_N-113_Id10928.PNG"
            cel_number = "120363042265251575@g.us"
            msg = "2022-04_20220621_MATRIZ_NE-44_N-113_Id10928.PNG"
            const media = MessageMedia.fromFilePath(file_path);
            // var media = await new MessageMedia("image/jpg", _base64.data, "myimage.jpg")
            await client.sendMessage(cel_number, media, {caption: msg})
        }

        if (message.body.startsWith('!msgBreak')) {
                // msg = "```Group Details\nName: s\nDescription: s\nCreated At: s```"
                msg = "```Group Details\nName: s\nDescription: s\nCreated At: s```"
                chat_id = '5519987688230@c.us'
                logger.info('enviando')
                await client.sendMessage(chat_id, msg)
        }

    });

    // Para pegar o id do grupo, abra o whatsweb e clique no Grupo desejado e procure no código fonte por @g.us
    // Para pegar o id do usuario, abra o whatsweb e clique no Grupo desejado e procure no código fonte por @c.us

    async function sendFilemsg(file_path,destinatario,msg){

        logger.info(`Nova Mensagem: Dst: ${destinatario} | Msg: ${msg} | Arq: ${file_path}`);

        if (isNaN(Number(destinatario))){
            group_name = destinatario
            if (group_name == 'PMO'){
                chat_id = "5511970643986-1448478805@g.us"
            }
            else if (group_name == 'WX - Meteorologia'){
                chat_id = "5511962984696-1599677203@g.us"
            }
            else if (group_name == 'Modelos'){
                chat_id = "5519992222894-1609250022@g.us"
            }
            else{
                chat_id = "120363029756985613@g.us"
            }
        }
        else{
            chat_id = `55${destinatario}@c.us`
        }
    
        if (file_path == null){
            file_path = ''
        }
        
        // Resolvendo o problema de quebra de linha (/n) na formatacao do texto
        msg = eval('`'+msg.replace(/`/g,'\\`')+'`')
        if (file_path != ''){
            const media = MessageMedia.fromFilePath(file_path);
            await client.sendMessage(chat_id, media, {caption: msg})
        }
        else{
            await client.sendMessage(chat_id, msg)
        }

    }


    client.initialize();



    const sqlite3 = require('sqlite3').verbose();
    const db = new sqlite3.Database('./lib/db_whats.db');

    function read_sqlite_db_after_30_seconds() {
        setTimeout(function() {
            
            db.serialize(() => {

                db.each("SELECT * FROM TB_MENSAGENS", (err, row) => {
                    
                    var filePath = row['TX_ARQUIVO'];
                    if (!fs.existsSync(filePath)) {
                        logger.error(`Arquivo "${filePath}" não encontrado`);
                        filePath = ""
                    }
                    sendFilemsg(filePath,row['TX_DESTINATARIO'],row['TX_MENSAGEM'])
                        db.run(`DELETE FROM TB_MENSAGENS WHERE ID=?`, row['ID'], function(err) {
                        if (err) {
                            logger.error(err.message);
                            return console.error(err.message);
                        }
                    });
                });
            });
            read_sqlite_db_after_30_seconds();
        }, 30000);

    }
} catch (erro) {
    // Código para tratar a exceção
    logger.error("Ocorreu um erro:", erro.message);
  }
  