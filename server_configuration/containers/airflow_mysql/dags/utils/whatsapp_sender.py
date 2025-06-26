# -*- coding: utf-8 -*-
"""
Módulo para envio de mensagens e arquivos via WhatsApp
Adaptado para uso no Airflow com base nas implementações existentes
"""

import os
import sys
import requests
from typing import Optional, Dict, Any
from datetime import datetime

# Adiciona o path dos utils para imports
utils_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'utils')
sys.path.insert(0, utils_path)
from utils.repository_webhook import SharedRepository


class WhatsAppSender:
    """
    Classe para envio de mensagens e arquivos via WhatsApp
    Baseada nas implementações existentes do gerarProdutos e rz_processar_produtos
    """
    
    def __init__(self):
        self.repository = SharedRepository()
        self.whatsapp_api = self.repository.WHATSAPP_API
        
    def get_auth_headers(self) -> Dict[str, str]:
        """
        Obtém headers de autenticação para API do WhatsApp
        
        Returns:
            Headers com token de autorização
        """
        return self.repository.get_auth_token()
    
    def send_message(self, destinatario: str = "Debug", mensagem: str = "", arquivo: Optional[str] = None) -> bool:
        """
        Envia mensagem via WhatsApp com ou sem arquivo
        
        Args:
            destinatario: Nome do destinatário/grupo (ex: 'PMO', 'FSARH', 'WX - Meteorologia'). 
                         Default: 'Debug' - grupo de teste
            mensagem: Texto da mensagem
            arquivo: Caminho completo do arquivo a ser enviado (opcional)
            
        Returns:
            True se enviado com sucesso, False caso contrário
            
        Raises:
            Exception: Se falhar no envio
        """
        try:
            if not self.whatsapp_api:
                raise Exception("WHATSAPP_API não configurada")
            
            # Prepara os dados da requisição
            fields = {
                "destinatario": destinatario,
                "mensagem": mensagem,
            }
            
            files = {}
            if arquivo and os.path.exists(arquivo):
                # Abre o arquivo para envio
                files = {
                    "arquivo": (os.path.basename(arquivo), open(arquivo, "rb"))
                }
                print(f"Anexando arquivo: {arquivo}")
            
            # Obtém headers de autenticação
            headers = self.get_auth_headers()
            
            # Envia a mensagem
            response = requests.post(
                self.whatsapp_api,
                data=fields,
                files=files,
                headers=headers
            )
            
            # Fecha o arquivo se foi aberto
            if files:
                files["arquivo"][1].close()
            
            print(f"WhatsApp Status Code: {response.status_code}")
            
            if response.status_code in [200, 201]:
                print(f"Mensagem enviada com sucesso para {destinatario}")
                return True
            else:
                error_msg = f"Erro ao enviar WhatsApp: {response.status_code} - {response.text}"
                print(error_msg)
                raise Exception(error_msg)
                
        except Exception as e:
            error_msg = f"Falha ao enviar mensagem WhatsApp: {str(e)}"
            print(error_msg)
            raise Exception(error_msg)
    
    def send_image(self, destinatario: str = "Debug", image_path: str = "", caption: str = "") -> bool:
        """
        Envia imagem via WhatsApp
        
        Args:
            destinatario: Nome do destinatário/grupo. Default: 'Debug'
            image_path: Caminho da imagem
            caption: Legenda da imagem
            
        Returns:
            True se enviado com sucesso
        """
        if not os.path.exists(image_path):
            raise Exception(f"Arquivo de imagem não encontrado: {image_path}")
        
        return self.send_message(
            destinatario=destinatario,
            mensagem=caption,
            arquivo=image_path
        )
    
    def send_document(self, destinatario: str = "Debug", file_path: str = "", caption: str = "") -> bool:
        """
        Envia documento via WhatsApp
        
        Args:
            destinatario: Nome do destinatário/grupo. Default: 'Debug'
            file_path: Caminho do documento
            caption: Descrição do documento
            
        Returns:
            True se enviado com sucesso
        """
        if not os.path.exists(file_path):
            raise Exception(f"Arquivo não encontrado: {file_path}")
        
        return self.send_message(
            destinatario=destinatario,
            mensagem=caption,
            arquivo=file_path
        )
    
    def send_table_notification(self, table_type: str, product_datetime: str, 
                              image_path: str, destinatario: str = "Debug") -> bool:
        """
        Envia notificação de tabela gerada
        
        Args:
            table_type: Tipo da tabela (ex: "Diferença de Cargas", "FSARH")
            product_datetime: Data/hora do produto
            image_path: Caminho da imagem da tabela
            destinatario: Destinatário da mensagem. Default: 'Debug'
            
        Returns:
            True se enviado com sucesso
        """
        caption = f"""{table_type} ({product_datetime})"""
        
        return self.send_image(
            destinatario=destinatario,
            image_path=image_path,
            caption=caption
        )
    
    @staticmethod
    def send_success_notification(process_name: str, details: str = "", destinatario: str = "Debug") -> bool:
        """
        Envia notificação de sucesso
        
        Args:
            process_name: Nome do processo
            details: Detalhes adicionais
            destinatario: Destinatário da mensagem. Default: 'Debug'
            
        Returns:
            True se enviado com sucesso
        """
        sender = WhatsAppSender()
        timestamp = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
        
        message = f"""✅ {process_name}
⏰ {timestamp}
{details}

Processamento concluído com sucesso!"""
        
        return sender.send_message(destinatario, message)
    
    @staticmethod
    def send_error_notification(process_name: str, error_details: str = "", destinatario: str = "Debug") -> bool:
        """
        Envia notificação de erro
        
        Args:
            process_name: Nome do processo
            error_details: Detalhes do erro
            destinatario: Destinatário da mensagem. Default: 'Debug'
            
        Returns:
            True se enviado com sucesso
        """
        sender = WhatsAppSender()
        timestamp = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
        
        message = f"""❌ Erro em {process_name}
⏰ {timestamp}

Detalhes: {error_details}

Verificar logs para mais informações."""
        
        return sender.send_message(destinatario, message)


# Funções de conveniência para compatibilidade com código existente
def send_whatsapp_message(mensagem: str, destinatario: str = "Debug", arquivo: Optional[str] = None) -> bool:
    """
    Função de conveniência compatível com implementações existentes
    
    Args:
        mensagem: Texto da mensagem
        destinatario: Nome do destinatário/grupo. Default: 'Debug'
        arquivo: Caminho do arquivo (opcional)
        
    Returns:
        True se enviado com sucesso
    """
    sender = WhatsAppSender()
    return sender.send_message(destinatario, mensagem, arquivo)


if __name__ == "__main__":
    # Exemplo de uso
    sender = WhatsAppSender()
    
    # Teste de mensagem simples para grupo Debug (padrão)
    # sender.send_message("Teste de mensagem via Airflow - Debug")
    
    # Teste de envio específico para PMO
    # sender.send_message("PMO", "Teste de mensagem para PMO")
    
    # Teste de envio de imagem para Debug (padrão)
    # sender.send_image(image_path="/path/to/image.png", caption="Teste de imagem")
    
    # Teste de notificação de tabela para Debug
    # sender.send_table_notification(
    #     table_type="Diferença de Cargas NEWAVE",
    #     product_datetime="06/2025",
    #     image_path="/tmp/deck_preliminar_newave/images/tabela_diferenca_cargas_202506.png"
    # )
    
    # Lista de grupos disponíveis baseado no getChatIdByGroupName:
    print("Grupos disponíveis:")
    grupos = [
        "PMO", "WX - Meteorologia", "Modelos", "Preco", "Condicao Hidrica",
        "Premissas Preco", "Airflow", "Airflow - Metereologia", "Airflow - Meteorologia",
        "Notificacoes Produtos", "FSARH", "Debug", "RZ - DESSEM", "bbce", "RZ - Condicoes SST"
    ]
    for grupo in grupos:
        print(f"  - {grupo}")
    
    print("\nWhatsAppSender configurado! Destinatário padrão: 'Debug'")
