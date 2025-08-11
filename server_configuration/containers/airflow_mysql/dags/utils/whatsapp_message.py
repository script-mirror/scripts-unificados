from datetime import datetime
from typing import List, Dict, Any
import os
import sys

utils_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'utils')
sys.path.insert(0, utils_path)
from utils.whatsapp_sender import WhatsAppSender

class WhatsappMessageSender:
    def __init__(self, webhook_url=None):
        self.webhook_url = webhook_url
        self.whatsapp_sender = WhatsAppSender()

    @staticmethod
    def enviar_whatsapp_sucesso(context):
        """Envia mensagem de sucesso via WhatsApp"""
        try:
            task_instance = context['task_instance']
            task_name = task_instance.task_id
            dag_name = task_instance.dag_id
            
            sender = WhatsAppSender()
            message = f"✅ {dag_name}.{task_name} processado com sucesso!"
            
            sender.send_message("Debug", message)
            print("Mensagem de sucesso enviada via WhatsApp")
            
        except Exception as e:
            print(f"Erro ao enviar WhatsApp de sucesso: {str(e)}")

    @staticmethod
    def enviar_whatsapp_erro(context):
        """Envia mensagem de erro via WhatsApp"""
        try:
            task_instance = context['task_instance']
            task_name = task_instance.task_id
            dag_name = task_instance.dag_id
            exception = context.get('exception', 'Erro não especificado')
            
            sender = WhatsAppSender()
            message = f"❌ Erro em {dag_name}.{task_name}: {str(exception)}"
            
            sender.send_message("Debug", message)
            print("Mensagem de erro enviada via WhatsApp")
            
        except Exception as e:
            print(f"Erro ao enviar WhatsApp de erro: {str(e)}")
    
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
            
            # Usar o repository do WhatsAppSender para enviar evento
            self.whatsapp_sender.repository.send_event_notification(event_data)
            print(f"Evento {event_type} enviado com status {status}")
            
        except Exception as e:
            print(f"Erro ao enviar notificação de evento: {str(e)}")
            
    def enviar_arquivo_whatsapp(self, destinatario: str, mensagem: str, arquivo: str):
        """Envia arquivo via WhatsApp"""
        try:
            return self.whatsapp_sender.send_message(destinatario, mensagem, arquivo)
        except Exception as e:
            print(f"Erro ao enviar arquivo via WhatsApp: {str(e)}")
            return False