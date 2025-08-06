# -*- coding: utf-8 -*-
"""
Módulo de utilitários para notificações do Airflow
Centraliza as funções de envio de notificações para WhatsApp e outras plataformas
"""

import os
import sys
from datetime import datetime
from typing import Optional, Dict, Any

# Adiciona o path dos utils para imports
utils_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'utils')
sys.path.insert(0, utils_path)
from utils.whatsapp_sender import WhatsAppSender


class AirflowNotificationHelper:
    """
    Helper class para notificações no Airflow
    Simplifica o envio de notificações para diferentes serviços
    """
    
    def __init__(self):
        self.whatsapp_sender = WhatsAppSender()
    
    def notify_task_success(self, task_name: str, details: str = "", 
                          destinatario: str = "PMO", attach_file: Optional[str] = None) -> bool:
        """
        Notifica sucesso de uma task
        
        Args:
            task_name: Nome da task
            details: Detalhes adicionais
            destinatario: Destinatário da notificação
            attach_file: Arquivo a ser anexado (opcional)
            
        Returns:
            True se enviado com sucesso
        """
        timestamp = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
        
        message = f"""✅ {task_name}
            ⏰ {timestamp}
            {details}
            ✅ Processamento concluído com sucesso!"""
        
        return self.whatsapp_sender.send_message(destinatario, message, attach_file)
    
    def notify_task_error(self, task_name: str, error_msg: str, 
                         destinatario: str = "PMO") -> bool:
        """
        Notifica erro de uma task
        
        Args:
            task_name: Nome da task
            error_msg: Mensagem de erro
            destinatario: Destinatário da notificação
            
        Returns:
            True se enviado com sucesso
        """
        timestamp = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
        
        message = f"""❌ Erro em {task_name}
            ⏰ {timestamp}

            Erro: {error_msg}

            🔍 Verificar logs do Airflow para mais detalhes."""
        
        return self.whatsapp_sender.send_message(destinatario, message)
    
    def notify_file_processed(self, file_name: str, process_type: str, 
                            image_path: Optional[str] = None, 
                            destinatario: str = "PMO") -> bool:
        """
        Notifica processamento de arquivo
        
        Args:
            file_name: Nome do arquivo processado
            process_type: Tipo de processamento
            image_path: Caminho da imagem resultante (opcional)
            destinatario: Destinatário da notificação
            
        Returns:
            True se enviado com sucesso
        """
        timestamp = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
        
        message = f"""📁 {process_type}
            📄 Arquivo: {file_name}
            ⏰ {timestamp}

            ✅ Processamento finalizado!"""
        
        if image_path and os.path.exists(image_path):
            return self.whatsapp_sender.send_message(destinatario, message, image_path)
        else:
            return self.whatsapp_sender.send_message(destinatario, message)
    
    def notify_data_update(self, data_type: str, update_details: str, 
                          table_image: Optional[str] = None,
                          destinatario: str = "PMO") -> bool:
        """
        Notifica atualização de dados
        
        Args:
            data_type: Tipo de dados atualizados
            update_details: Detalhes da atualização
            table_image: Imagem da tabela (opcional)
            destinatario: Destinatário da notificação
            
        Returns:
            True se enviado com sucesso
        """
        timestamp = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
        
        message = f"""📊 Atualização {data_type}
            ⏰ {timestamp}

            {update_details}

            ✅ Dados atualizados com sucesso!"""
        
        if table_image and os.path.exists(table_image):
            return self.whatsapp_sender.send_message(destinatario, message, table_image)
        else:
            return self.whatsapp_sender.send_message(destinatario, message)


# Funções estáticas para uso direto em tasks do Airflow
def send_whatsapp_success(**context):
    """
    Callback de sucesso para tasks do Airflow
    Usar como on_success_callback
    """
    try:
        task_instance = context['task_instance']
        task_name = task_instance.task_id
        dag_name = task_instance.dag_id
        
        helper = AirflowNotificationHelper()
        helper.notify_task_success(
            task_name=f"{dag_name}.{task_name}",
            details="Task executada com sucesso via Airflow"
        )
        
    except Exception as e:
        print(f"Erro ao enviar notificação de sucesso: {str(e)}")


def send_whatsapp_failure(**context):
    """
    Callback de falha para tasks do Airflow
    Usar como on_failure_callback
    """
    try:
        task_instance = context['task_instance']
        task_name = task_instance.task_id
        dag_name = task_instance.dag_id
        exception = context.get('exception', 'Erro não especificado')
        
        helper = AirflowNotificationHelper()
        helper.notify_task_error(
            task_name=f"{dag_name}.{task_name}",
            error_msg=str(exception)
        )
        
    except Exception as e:
        print(f"Erro ao enviar notificação de falha: {str(e)}")


def send_custom_notification(destinatario: str, titulo: str, mensagem: str, 
                           arquivo: Optional[str] = None) -> bool:
    """
    Envia notificação personalizada
    
    Args:
        destinatario: Destinatário da mensagem
        titulo: Título da notificação
        mensagem: Corpo da mensagem
        arquivo: Arquivo a ser anexado (opcional)
        
    Returns:
        True se enviado com sucesso
    """
    try:
        timestamp = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
        
        full_message = f"""📢 {titulo}
            ⏰ {timestamp}
            
            {mensagem}"""
        
        sender = WhatsAppSender()
        return sender.send_message(destinatario, full_message, arquivo)
        
    except Exception as e:
        print(f"Erro ao enviar notificação personalizada: {str(e)}")
        return False


if __name__ == "__main__":
    # Exemplos de uso
    helper = AirflowNotificationHelper()
    
    # Teste de notificação de sucesso
    # helper.notify_task_success("download_arquivos", "Arquivo baixado com sucesso")
    
    # Teste de notificação com arquivo
    # helper.notify_file_processed(
    #     "deck_newave.zip", 
    #     "Processamento NEWAVE",
    #     "/tmp/resultado.png"
    # )
    
    print("AirflowNotificationHelper configurado e pronto para uso!")
