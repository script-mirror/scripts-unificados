# Sistema de Notificações WhatsApp para Airflow

Este documento explica como usar o novo sistema de notificações WhatsApp implementado para o Airflow, baseado nas implementações existentes dos sistemas `gerarProdutos` e `rz_processar_produtos`.

## Visão Geral

O sistema foi criado para facilitar o envio de mensagens e arquivos via WhatsApp a partir das tasks do Airflow, mantendo compatibilidade com as implementações existentes.

## Componentes Principais

### 1. WhatsAppSender (`utils/whatsapp_sender.py`)

Classe principal para envio de mensagens e arquivos via WhatsApp.

### 2. AirflowNotificationHelper (`utils/airflow_notifications.py`)

Helper class que simplifica o uso do WhatsAppSender para casos comuns do Airflow.

### 3. Repository atualizado (`utils/repository_webhook.py`)

Repository atualizado com suporte a envio de arquivos.

## Como Usar

### Envio Simples de Mensagem

```python
from utils.whatsapp_sender import WhatsAppSender

sender = WhatsAppSender()
sender.send_message("PMO", "Mensagem de teste")
```

### Envio de Arquivo/Imagem

```python
sender = WhatsAppSender()
sender.send_image(
    destinatario="PMO",
    image_path="/tmp/tabela.png",
    caption="Tabela de diferença de cargas"
)
```

### Uso no Service (Exemplo do NEWAVE)

```python
# No método enviar_tabela_whatsapp_email
from utils.whatsapp_sender import WhatsAppSender

whatsapp_sender = WhatsAppSender()
success = whatsapp_sender.send_table_notification(
    table_type="Diferença de Cargas NEWAVE",
    product_datetime=product_datetime_str,
    image_path=image_path,
    destinatario="PMO"
)
```

### Notificações Padronizadas

```python
from utils.airflow_notifications import AirflowNotificationHelper

helper = AirflowNotificationHelper()

# Notificação de sucesso
helper.notify_task_success(
    task_name="processar_deck_newave",
    details="Deck processado com 150 registros",
    attach_file="/tmp/resultado.png"
)

# Notificação de erro
helper.notify_task_error(
    task_name="download_arquivos",
    error_msg="Arquivo não encontrado no webhook"
)
```

### Callbacks do Airflow

```python
# No DAG definition
from utils.airflow_notifications import send_whatsapp_success, send_whatsapp_failure

task = PythonOperator(
    task_id='minha_task',
    python_callable=minha_funcao,
    on_success_callback=send_whatsapp_success,
    on_failure_callback=send_whatsapp_failure,
    dag=dag
)
```

## Destinatários Disponíveis

Com base nos exemplos existentes, os destinatários comuns são:

- `"PMO"` - Equipe PMO
- `"FSARH"` - Notificações FSARH
- `"WX - Meteorologia"` - Equipe de Meteorologia
- `"Condicao Hidrica"` - Condições hídricas
- `"bbce"` - BBCE

## Tipos de Mensagens Suportadas

### 1. Mensagens de Texto

```python
sender.send_message("PMO", "Processamento concluído")
```

### 2. Mensagens com Imagem

```python
sender.send_image("PMO", "/path/image.png", "Legenda da imagem")
```

### 3. Mensagens com Documento

```python
sender.send_document("PMO", "/path/file.pdf", "Relatório gerado")
```

### 4. Notificações de Tabela (Formatadas)

```python
sender.send_table_notification(
    table_type="Diferença de Cargas",
    product_datetime="06/2025",
    image_path="/tmp/tabela.png",
    destinatario="PMO"
)
```

## Configuração

### Variáveis de Ambiente Necessárias

No arquivo `.env`:

```bash
WHATSAPP_API=https://tradingenergiarz.com/bot-whatsapp/whatsapp-message
URL_COGNITO=https://us-east-1rukqfgd2h.auth.us-east-1.amazoncognito.com/oauth2/token
CONFIG_COGNITO=grant_type=client_credentials&client_id=...&client_secret=...
```

### Estrutura de Diretórios

```
dags/
├── utils/
│   ├── whatsapp_sender.py          # Classe principal
│   ├── airflow_notifications.py    # Helper para Airflow
│   ├── repository_webhook.py       # Repository atualizado
│   └── whatsapp_message.py         # Callbacks atualizados
└── webhook/
    └── deck_preliminar_newave/
        └── service_deck_preliminar_newave.py
```

## Exemplo Completo - Processamento de Tabela

```python
@staticmethod
def processar_e_enviar_tabela(**kwargs):
    try:
        # 1. Processar dados e gerar tabela
        tabela_html = gerar_tabela_html(dados)

        # 2. Converter para imagem
        image_path = converter_html_para_imagem(tabela_html)

        # 3. Enviar via WhatsApp
        from utils.whatsapp_sender import WhatsAppSender

        sender = WhatsAppSender()
        success = sender.send_table_notification(
            table_type="Minha Tabela",
            product_datetime="26/06/2025",
            image_path=image_path,
            destinatario="PMO"
        )

        return {
            'success': True,
            'whatsapp_sent': success,
            'image_path': image_path
        }

    except Exception as e:
        # Enviar notificação de erro
        from utils.airflow_notifications import AirflowNotificationHelper
        helper = AirflowNotificationHelper()
        helper.notify_task_error("processar_tabela", str(e))
        raise
```

## Tratamento de Erros

O sistema inclui tratamento de erros robusto:

1. **Erros de autenticação**: Logs detalhados para debug
2. **Arquivos não encontrados**: Validação antes do envio
3. **Falhas de API**: Retry automático (onde aplicável)
4. **Erros de rede**: Timeouts configuráveis

## Compatibilidade

Este sistema mantém compatibilidade com:

- Implementações existentes do `gerarProdutos2.py`
- Funções do `rz_processar_produtos.py`
- APIs e tokens já configurados
- Estrutura de destinatários existente

## Logs e Debug

Todos os envios geram logs detalhados:

```
WhatsApp Status Code: 200
Mensagem enviada com sucesso para PMO
Anexando arquivo: /tmp/tabela.png
```

Para debug, verificar:

1. Variáveis de ambiente carregadas
2. Autenticação com API
3. Existência dos arquivos
4. Logs do Airflow

## Próximos Passos

Para implementar em novos services:

1. Importar o `WhatsAppSender`
2. Configurar destinatário apropriado
3. Usar método adequado (mensagem, imagem, documento)
4. Adicionar tratamento de erro
5. Configurar callbacks se necessário
