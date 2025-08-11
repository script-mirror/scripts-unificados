"""
Repositório de validadores genéricos para uso em DAGs webhook.
Contém classes base e validadores reutilizáveis.
"""

from typing import List, Dict, Any, Optional
from datetime import datetime
import re


class ValidationError(Exception):
    """Exception raised for validation errors."""
    pass


class BaseValidator:
    """Base class for validators"""
    def validate(self, data):
        raise NotImplementedError("Subclasses must implement validate method")


class UrlValidator(BaseValidator):
    """Validates URLs"""
    def validate(self, url: str) -> str:
        if not isinstance(url, str) or not url.startswith(('http://', 'https://')):
            raise ValidationError(f"URL inválida: {url}")
        return url


class FilenameValidator(BaseValidator):
    """Validates filenames"""
    def __init__(self, allowed_extensions: Optional[List[str]] = None):
        self.allowed_extensions = allowed_extensions or []
    
    def validate(self, filename: str) -> str:
        if not isinstance(filename, str):
            raise ValidationError(f"Nome do arquivo deve ser string, recebido: {type(filename)}")
        
        if self.allowed_extensions:
            if not any(filename.lower().endswith(ext.lower()) for ext in self.allowed_extensions):
                raise ValidationError(f"Extensão do arquivo inválida. Permitidas: {self.allowed_extensions}. Recebido: {filename}")
        
        return filename


class ProductNameValidator(BaseValidator):
    """Validates product names"""
    def __init__(self, expected_patterns: Optional[List[str]] = None, exact_match: Optional[str] = None):
        self.expected_patterns = expected_patterns or []
        self.exact_match = exact_match
    
    def validate(self, name: str) -> str:
        if not isinstance(name, str):
            raise ValidationError(f"Nome do produto deve ser string, recebido: {type(name)}")
        
        if self.exact_match:
            if name != self.exact_match:
                raise ValidationError(f"Nome de produto deve ser exatamente '{self.exact_match}', recebido: '{name}'")
        elif self.expected_patterns:
            if not any(pattern.upper() in name.upper() for pattern in self.expected_patterns):
                raise ValidationError(f"Nome de produto deve conter um dos padrões {self.expected_patterns}, recebido: '{name}'")
        
        return name


class FunctionNameValidator(BaseValidator):
    """Validates function names"""
    def __init__(self, expected_name: Optional[str] = None):
        self.expected_name = expected_name
    
    def validate(self, name: str) -> str:
        if not isinstance(name, str):
            raise ValidationError(f"Nome da função deve ser string, recebido: {type(name)}")
        
        if self.expected_name and name != self.expected_name:
            raise ValidationError(f"Nome de função deve ser '{self.expected_name}', recebido: '{name}'")
        
        return name


class DateFormatValidator(BaseValidator):
    """Validates date formats"""
    def __init__(self, allowed_formats: Optional[List[str]] = None):
        self.allowed_formats = allowed_formats or [
            "%Y-%m-%d",
            "%d/%m/%Y", 
            "%m/%Y",
            "%Y-%m-%dT%H:%M:%S.%fZ"
        ]
    
    def validate(self, date_str: str) -> datetime:
        if not isinstance(date_str, str):
            raise ValidationError(f"Data deve ser string, recebido: {type(date_str)}")
        
        # Try ISO format with dateutil if available
        if 'T' in date_str:
            try:
                from dateutil import parser
                return parser.parse(date_str)
            except ImportError:
                # Fallback to manual parsing
                try:
                    return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                except:
                    date_part = date_str.split('T')[0]
                    return datetime.strptime(date_part, "%Y-%m-%d")
        
        # Try predefined formats
        for fmt in self.allowed_formats:
            try:
                parsed_date = datetime.strptime(date_str, fmt)
                # Special handling for MM/YYYY format
                if fmt == "%m/%Y":
                    return datetime(parsed_date.year, parsed_date.month, 1)
                return parsed_date
            except ValueError:
                continue
        
        raise ValidationError(f"Formato de data inválido: {date_str}. Formatos permitidos: {self.allowed_formats}")


class DateRangeValidator(BaseValidator):
    """Validates date ranges"""
    def validate(self, date_range: Dict[str, datetime]) -> Dict[str, datetime]:
        if not isinstance(date_range, dict) or 'start' not in date_range or 'end' not in date_range:
            raise ValidationError("Intervalo de datas deve ser um dicionário com 'start' e 'end'")
        
        if not isinstance(date_range['start'], datetime) or not isinstance(date_range['end'], datetime):
            raise ValidationError("As datas devem ser objetos datetime")
        
        if date_range['start'] > date_range['end']:
            raise ValidationError("Data inicial deve ser anterior ou igual à data final")
        
        return date_range


class BooleanValidator(BaseValidator):
    """Validates boolean values"""
    def validate(self, value: Any) -> bool:
        if not isinstance(value, bool):
            # Try to convert common representations
            if isinstance(value, str):
                if value.lower() in ['true', '1', 'yes', 'sim']:
                    return True
                elif value.lower() in ['false', '0', 'no', 'nao', 'não']:
                    return False
            elif isinstance(value, int):
                if value in [0, 1]:
                    return bool(value)
            
            raise ValidationError(f"Valor deve ser booleano, recebido: {type(value).__name__} = {value}")
        
        return value


class RequiredFieldsValidator(BaseValidator):
    """Validates required fields"""
    def __init__(self, required_fields: List[str]):
        self.required_fields = required_fields
    
    def validate(self, data: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(data, dict):
            raise ValidationError("Dados devem ser um dicionário")
        
        missing_fields = [field for field in self.required_fields if field not in data or data[field] is None]
        if missing_fields:
            raise ValidationError(f"Campos obrigatórios ausentes: {', '.join(missing_fields)}")
        
        return data


class StringValidator(BaseValidator):
    """Validates string values"""
    def __init__(self, min_length: int = 0, max_length: Optional[int] = None, pattern: Optional[str] = None):
        self.min_length = min_length
        self.max_length = max_length
        self.pattern = re.compile(pattern) if pattern else None
    
    def validate(self, value: str) -> str:
        if not isinstance(value, str):
            raise ValidationError(f"Valor deve ser string, recebido: {type(value)}")
        
        if len(value) < self.min_length:
            raise ValidationError(f"String deve ter pelo menos {self.min_length} caracteres")
        
        if self.max_length and len(value) > self.max_length:
            raise ValidationError(f"String deve ter no máximo {self.max_length} caracteres")
        
        if self.pattern and not self.pattern.match(value):
            raise ValidationError(f"String não atende ao padrão requerido: {value}")
        
        return value


class WebhookIdValidator(BaseValidator):
    """Validates webhook IDs (typically MongoDB ObjectIds)"""
    def validate(self, webhook_id: str) -> str:
        if not isinstance(webhook_id, str):
            raise ValidationError(f"Webhook ID deve ser string, recebido: {type(webhook_id)}")
        
        # Basic validation for MongoDB ObjectId format (24 hex characters)
        if len(webhook_id) != 24 or not all(c in '0123456789abcdefABCDEF' for c in webhook_id):
            raise ValidationError(f"Webhook ID deve ser um ObjectId válido (24 caracteres hexadecimais): {webhook_id}")
        
        return webhook_id


class BaseWebhookValidator:
    """
    Classe base para validadores de webhook.
    Fornece estrutura comum e métodos utilitários.
    """
    
    def __init__(self):
        self.url_validator = UrlValidator()
        self.boolean_validator = BooleanValidator()
        self.date_format_validator = DateFormatValidator()
        self.date_range_validator = DateRangeValidator()
        self.webhook_id_validator = WebhookIdValidator()
        
        # Campos obrigatórios padrão para webhooks
        self.base_required_fields = [
            'dataProduto', 'enviar', 'macroProcesso', 'nome', 
            'periodicidade', 'periodicidadeFinal', 'processo',
            'url', 's3Key', 'webhookId', 'filename'
        ]
    
    def validate_base_structure(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Valida a estrutura base dos parâmetros do webhook"""
        if not params:
            raise ValidationError("Parâmetros não encontrados")
        
        # Valida product_details
        product_details = params.get('product_details')
        if not product_details:
            raise ValidationError("product_details não encontrado")
        
        return product_details
    
    def validate_common_fields(self, product_details: Dict[str, Any], required_fields: Optional[List[str]] = None) -> None:
        """Valida campos comuns a todos os webhooks"""
        # Usar campos obrigatórios customizados ou padrão
        fields_to_validate = required_fields or self.base_required_fields
        
        # Valida campos obrigatórios
        required_validator = RequiredFieldsValidator(fields_to_validate)
        required_validator.validate(product_details)
        
        # Validações específicas de campos comuns
        if 'url' in product_details:
            self.url_validator.validate(product_details['url'])
        
        if 'enviar' in product_details:
            self.boolean_validator.validate(product_details['enviar'])
        
        if 'webhookId' in product_details:
            self.webhook_id_validator.validate(product_details['webhookId'])
        
        # Validação das datas
        if 'periodicidade' in product_details and 'periodicidadeFinal' in product_details:
            start_date = self.date_format_validator.validate(product_details['periodicidade'])
            end_date = self.date_format_validator.validate(product_details['periodicidadeFinal'])
            
            self.date_range_validator.validate({
                'start': start_date,
                'end': end_date
            })
    
    def validate(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Método principal de validação - deve ser implementado pelas subclasses"""
        raise NotImplementedError("Subclasses devem implementar o método validate")
    
    def log_validation_success(self, validated_data: Dict[str, Any]) -> None:
        """Log de sucesso da validação"""
        print("✅ Validação dos dados de entrada concluída com sucesso")
        print(f"📝 Produto validado: {validated_data.get('nome', 'N/A')}")
        print(f"📅 Período: {validated_data.get('dataProduto', 'N/A')}")
        print(f"📁 Arquivo: {validated_data.get('filename', 'N/A')}")
        print(f"🔗 URL: {validated_data.get('url', 'N/A')}")
    
    def handle_validation_error(self, error: Exception) -> None:
        """Tratamento de erros de validação"""
        if isinstance(error, ValidationError):
            error_msg = f"❌ Erro na validação dos dados: {str(error)}"
        else:
            error_msg = f"❌ Erro inesperado na validação: {str(error)}"
        
        print(error_msg)
        raise ValidationError(error_msg)
