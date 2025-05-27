from datetime import datetime
from typing import List, Dict, Any

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__))) #NEVER REMOVE THIS LINE
from ....utils.webhook_libs.repository_webhook import SharedRepository # type: ignore # Importando o repositório compartilhado

class WhatsappMessageSender:
    def __init__(self, webhook_url):
        self.webhook_url = webhook_url

    @staticmethod
    def enviar_whatsapp_sucesso(context):
        """Envia mensagem de sucesso via WhatsApp"""
        try:
            repository = SharedRepository()
            message = "✅ Deck Preliminar Decomp processado com sucesso!"
            
            repository.send_whatsapp_message(message)
            print("Mensagem de sucesso enviada via WhatsApp")
            
        except Exception as e:
            print(f"Erro ao enviar WhatsApp de sucesso: {str(e)}")

    @staticmethod
    def enviar_whatsapp_erro(context):
        """Envia mensagem de erro via WhatsApp"""
        try:
            repository = SharedRepository()
            # Obter detalhes do erro do context se disponível
            error_details = context.get('exception', 'Erro não especificado')
            message = f"❌ Erro no processamento do Deck Preliminar Decomp: {error_details}"
            
            repository.send_whatsapp_message(message)
            print("Mensagem de erro enviada via WhatsApp")
            
        except Exception as e:
            print(f"Erro ao enviar WhatsApp de erro: {str(e)}")
    
    @staticmethod
    def enviar_notificacao_evento(self, event_type: str, status: str, details: Dict[str, Any] = None):
        """Envia notificação de evento para API de eventos"""
        try:
            event_data = {
                'event_type': event_type,
                'status': status,
                'timestamp': datetime.now().isoformat(),
                'service': 'deck_preliminar_decomp',
                'details': details or {}
            }
            
            self.repository.send_event_notification(event_data)
            print(f"Evento {event_type} enviado com status {status}")
            
        except Exception as e:
            print(f"Erro ao enviar notificação de evento: {str(e)}")