import os
import requests as req
from typing import Dict, Any, Optional
from dotenv import load_dotenv  # type: ignore


class SharedRepository:
    """
    Repository layer para acesso a dados externos do Deck Preliminar Decomp
    Responsável por comunicação com APIs, autenticação e downloads
    """
    
    def __init__(self):
        # Carrega variáveis de ambiente
        load_dotenv(os.path.join(os.path.abspath(os.path.expanduser("~")), '.env'))
        self.WHATSAPP_API = os.getenv("WHATSAPP_API")
        self.EVENTS_API = os.getenv("URL_EVENTS_API")
        self.URL_COGNITO = os.getenv("URL_COGNITO")
        self.CONFIG_COGNITO = os.getenv("CONFIG_COGNITO")
        self.WEBHOOK_BASE_URL = "https://tradingenergiarz.com/new-webhook/api/webhooks"
    
    def get_auth_token(self) -> Dict[str, str]:
        """
        Obtém token de autenticação do Cognito
        
        Returns:
            Dict com headers de autorização
            
        Raises:
            Exception: Se falhar ao obter token de autenticação
        """
        try:
            response = req.post(
                self.URL_COGNITO,
                data=self.CONFIG_COGNITO,
                headers={'Content-Type': 'application/x-www-form-urlencoded'}
            )
            
            if response.status_code != 200:
                raise Exception(f"Erro na autenticação: {response.status_code} - {response.text}")
            
            token = response.json().get('access_token')
            if not token:
                raise Exception("Token de acesso não encontrado na resposta")
            
            return {
                'Authorization': f"Bearer {token}"
            }
        except Exception as e:
            raise Exception(f"Falha ao obter token de autenticação: {str(e)}")
    
    def download_webhook_file(self, webhook_id: str, filename: str, download_path: str) -> str:
        """
        Faz download de arquivo via webhook
        
        Args:
            webhook_id: ID do webhook
            filename: Nome do arquivo
            download_path: Caminho de destino do download
            
        Returns:
            Caminho completo do arquivo baixado
            
        Raises:
            Exception: Se falhar no download
        """
        try:
            # Obtém headers de autenticação
            auth_headers = self.get_auth_token()
            
            # Monta caminho completo do arquivo
            file_path = os.path.join(download_path, filename)
            
            # Cria diretório se não existir
            os.makedirs(download_path, exist_ok=True)
            
            # Solicita URL de download
            download_url_response = req.get(
                f"{self.WEBHOOK_BASE_URL}/{webhook_id}/download",
                headers=auth_headers
            )
            
            if download_url_response.status_code != 200:
                raise Exception(f"Erro ao obter URL de download: {download_url_response.text}")
            
            download_url = download_url_response.json().get('url')
            if not download_url:
                raise Exception("URL de download não encontrada na resposta")
            
            # Faz download do arquivo
            file_response = req.get(download_url)
            
            if file_response.status_code != 200:
                raise Exception(f"Erro ao baixar arquivo: {file_response.status_code}")
            
            # Salva arquivo
            with open(file_path, 'wb') as f:
                f.write(file_response.content)
            
            return file_path
            
        except Exception as e:
            raise Exception(f"Falha no download do arquivo {filename}: {str(e)}")
    
    def send_whatsapp_message(self, message: str, phone_number: Optional[str] = None) -> bool:
        """
        Envia mensagem via WhatsApp API
        
        Args:
            message: Mensagem a ser enviada
            phone_number: Número de telefone (opcional)
            
        Returns:
            True se mensagem foi enviada com sucesso
            
        Raises:
            Exception: Se falhar no envio
        """
        try:
            if not self.WHATSAPP_API:
                raise Exception("WHATSAPP_API não configurada")
            
            payload = {
                'message': message
            }
            
            if phone_number:
                payload['phone'] = phone_number
            
            response = req.post(
                self.WHATSAPP_API,
                json=payload,
                headers={'Content-Type': 'application/json'}
            )
            
            if response.status_code not in [200, 201]:
                raise Exception(f"Erro ao enviar WhatsApp: {response.status_code} - {response.text}")
            
            return True
            
        except Exception as e:
            raise Exception(f"Falha ao enviar mensagem WhatsApp: {str(e)}")
    
    def send_event_notification(self, event_data: Dict[str, Any]) -> bool:
        """
        Envia notificação para API de eventos
        
        Args:
            event_data: Dados do evento
            
        Returns:
            True se evento foi enviado com sucesso
            
        Raises:
            Exception: Se falhar no envio
        """
        try:
            if not self.EVENTS_API:
                raise Exception("EVENTS_API não configurada")
            
            auth_headers = self.get_auth_token()
            auth_headers['Content-Type'] = 'application/json'
            
            response = req.post(
                self.EVENTS_API,
                json=event_data,
                headers=auth_headers
            )
            
            if response.status_code not in [200, 201]:
                raise Exception(f"Erro ao enviar evento: {response.status_code} - {response.text}")
            
            return True
            
        except Exception as e:
            raise Exception(f"Falha ao enviar evento: {str(e)}")
    
    def download_from_url(self, url: str, destination_path: str) -> str:
        """
        Faz download direto de uma URL
        
        Args:
            url: URL do arquivo
            destination_path: Caminho de destino
            
        Returns:
            Caminho do arquivo baixado
            
        Raises:
            Exception: Se falhar no download
        """
        try:
            response = req.get(url)
            
            if response.status_code != 200:
                raise Exception(f"Erro no download: {response.status_code}")
            
            # Cria diretório se necessário
            os.makedirs(os.path.dirname(destination_path), exist_ok=True)
            
            with open(destination_path, 'wb') as f:
                f.write(response.content)
            
            return destination_path
            
        except Exception as e:
            raise Exception(f"Falha no download de {url}: {str(e)}")
    
    def validate_file_exists(self, file_path: str) -> bool:
        """
        Valida se arquivo existe e não está vazio
        
        Args:
            file_path: Caminho do arquivo
            
        Returns:
            True se arquivo existe e não está vazio
        """
        return os.path.exists(file_path) and os.path.getsize(file_path) > 0

    def extract_zip_file(self, zip_path: str, extract_to: str) -> None:
        """
        Extrai arquivos de um arquivo ZIP
        
        Args:
            zip_path: Caminho do arquivo ZIP
            extract_to: Caminho de extração
            
        Raises:
            Exception: Se falhar na extração
        """
        try:
            import zipfile
            
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(extract_to)
            
            print(f"Arquivos extraídos para {extract_to}")
        
        except Exception as e:
            raise Exception(f"Falha na extração do arquivo ZIP: {str(e)}")