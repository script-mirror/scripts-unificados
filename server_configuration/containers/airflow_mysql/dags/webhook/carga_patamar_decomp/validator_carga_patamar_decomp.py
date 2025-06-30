from typing import List, Dict, Any
from datetime import datetime

# Define your own validator classes here since the import is not available
class ValidationError(Exception):
    """Exception raised for validation errors."""
    pass

class BaseValidator:
    """Base class for validators"""
    def validate(self, data):
        raise NotImplementedError("Subclasses must implement validate method")

class UrlValidator(BaseValidator):
    """Validates URLs"""
    def validate(self, url):
        if not isinstance(url, str) or not url.startswith(('http://', 'https://')):
            raise ValidationError(f"URL inválida: {url}")
        return url

class FilenameValidator(BaseValidator):
    """Validates filenames"""
    def __init__(self, allowed_extensions=None):
        self.allowed_extensions = allowed_extensions or []
    
    def validate(self, filename):
        if not isinstance(filename, str) or not any(filename.endswith(ext) for ext in self.allowed_extensions):
            raise ValidationError(f"Nome de arquivo inválido: {filename}")
        return filename

class ProductNameValidator(BaseValidator):
    """Validates product names"""
    def __init__(self, expected_name=None):
        self.expected_name = expected_name
    
    def validate(self, name):
        if not isinstance(name, str) or (self.expected_name and name != self.expected_name):
            raise ValidationError(f"Nome de produto inválido: {name}")
        return name

class FunctionNameValidator(BaseValidator):
    """Validates function names"""
    def __init__(self, expected_name=None):
        self.expected_name = expected_name
    
    def validate(self, name):
        if not isinstance(name, str) or (self.expected_name and name != self.expected_name):
            raise ValidationError(f"Nome de função inválido: {name}")
        return name

class DateFormatValidator(BaseValidator):
    """Validates date formats"""
    def validate(self, date_str):
        if not isinstance(date_str, str):
            raise ValidationError(f"Data deve ser string, recebido: {type(date_str)}")
        try:
            date_str = date_str.split('T')[0]  # Remove time part if present
            return datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            raise ValidationError(f"Formato de data inválido: {date_str}")

class DateRangeValidator(BaseValidator):
    """Validates date ranges"""
    def validate(self, date_range):
        if not isinstance(date_range, dict) or 'start' not in date_range or 'end' not in date_range:
            raise ValidationError("Intervalo de datas inválido")
        if date_range['start'] > date_range['end']:
            raise ValidationError("Data inicial deve ser anterior à data final")
        return date_range

class BooleanValidator(BaseValidator):
    """Validates boolean values"""
    def validate(self, value):
        if not isinstance(value, bool):
            raise ValidationError(f"Valor deve ser booleano, recebido: {type(value)}")
        return value

class RequiredFieldsValidator(BaseValidator):
    """Validates required fields"""
    def __init__(self, required_fields=None):
        self.required_fields = required_fields or []
    
    def validate(self, data):
        if not isinstance(data, dict):
            raise ValidationError("Dados devem ser um dicionário")
        missing_fields = [field for field in self.required_fields if field not in data]
        if missing_fields:
            raise ValidationError(f"Campos obrigatórios ausentes: {', '.join(missing_fields)}")
        return data


class CargaPMOValidator(BaseValidator):
    """Validador específico para dados de Carga PMO"""
    
    def validate(self, data_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Valida uma lista de registros de carga PMO
        
        Args:
            data_list: Lista de dicionários com dados de carga
            
        Returns:
            A mesma lista validada
            
        Raises:
            ValidationError: Se algum dado não estiver em conformidade
        """
        if not isinstance(data_list, list):
            raise ValidationError(f"Dados devem ser uma lista, recebido: {type(data_list)}")
        
        if not data_list:
            raise ValidationError("Lista de dados está vazia")
        
        for index, item in enumerate(data_list):
            try:
                # Validar campos obrigatórios
                required_fields = ['carga', 'mes', 'revisao', 'subsistema', 'dt_inicio', 'tipo']
                missing_fields = [field for field in required_fields if field not in item]
                if missing_fields:
                    raise ValidationError(f"Campos obrigatórios ausentes: {', '.join(missing_fields)}")
                
                # Validar tipos de dados
                if not isinstance(item.get('carga'), (int, float)):
                    raise ValidationError(f"Campo 'carga' deve ser numérico, recebido: {type(item.get('carga'))}")
                
                if not isinstance(item.get('mes'), int):
                    raise ValidationError(f"Campo 'mes' deve ser inteiro, recebido: {type(item.get('mes'))}")
                
                if not isinstance(item.get('revisao'), str):
                    raise ValidationError(f"Campo 'revisao' deve ser string, recebido: {type(item.get('revisao'))}")
                
                if not isinstance(item.get('subsistema'), str):
                    raise ValidationError(f"Campo 'subsistema' deve ser string, recebido: {type(item.get('subsistema'))}")
                
                if not isinstance(item.get('dt_inicio'), str):
                    raise ValidationError(f"Campo 'dt_inicio' deve ser string, recebido: {type(item.get('dt_inicio'))}")
                
                if not isinstance(item.get('tipo'), str):
                    raise ValidationError(f"Campo 'tipo' deve ser string, recebido: {type(item.get('tipo'))}")
                
                # Validar valores
                if item.get('mes') < 1 or item.get('mes') > 12:
                    raise ValidationError(f"Campo 'mes' deve estar entre 1 e 12, recebido: {item.get('mes')}")
                
                if item.get('tipo') not in ['mensal', 'semanal']:
                    raise ValidationError(f"Campo 'tipo' deve ser 'mensal' ou 'semanal', recebido: {item.get('tipo')}")
                
                # Validar subsistema
                valid_subsistemas = ['NE', 'N', 'SECO', 'S', 'SIN', 'NNO', 'SECOS']
                if item.get('subsistema') not in valid_subsistemas:
                    raise ValidationError(f"Campo 'subsistema' inválido: {item.get('subsistema')}. Valores válidos: {valid_subsistemas}")
                
                # Validar formato da data
                try:
                    datetime.strptime(item.get('dt_inicio', ''), "%Y%m%d")
                except ValueError:
                    raise ValidationError(f"Formato de data inválido: {item.get('dt_inicio')}. Use o formato YYYYMMDD.")
                
                # Validar semana
                if item.get('tipo') == 'semanal':
                    if not isinstance(item.get('semana'), int):
                        raise ValidationError(f"Para tipo 'semanal', campo 'semana' deve ser inteiro, recebido: {type(item.get('semana'))}")
                    
                    if item.get('semana') < 1 or item.get('semana') > 6:
                        raise ValidationError(f"Campo 'semana' deve estar entre 1 e 6, recebido: {item.get('semana')}")
                
            except ValidationError as e:
                # Adiciona informação de qual registro falhou
                raise ValidationError(f"Erro no registro {index+1}: {str(e)}")
        
        return data_list


class CargaPatamarDecompValidator:
    """Validador principal para dados do Deck Preliminar Decomp"""
    
    def __init__(self):
        # Configuração dos validadores específicos
        self.url_validator = UrlValidator()
        self.filename_validator = FilenameValidator(['.zip'])
        self.product_name_validator = ProductNameValidator('Carga por patamar - DECOMP')
        self.date_format_validator = DateFormatValidator()
        self.date_range_validator = DateRangeValidator()
        self.boolean_validator = BooleanValidator()
        self.required_fields_validator = RequiredFieldsValidator([
            'dataProduto', 'enviar', 'macroProcesso', 'nome', 
            'periodicidade', 'periodicidadeFinal', 'processo',
            'url', 's3Key', 'webhookId', 'filename'
        ])
    
    def validate_params(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Valida os parâmetros principais"""
        if not params:
            raise ValidationError("Parâmetros não encontrados")

        # Valida product_details
        product_details = params.get('product_details')
        if not product_details:
            raise ValidationError("product_details não encontrado")
        
        return self.validate_product_details(product_details)
    
    def validate_product_details(self, product_details: Dict[str, Any]) -> Dict[str, Any]:
        """Valida os detalhes do produto"""
        # Valida campos obrigatórios
        self.required_fields_validator.validate(product_details)
        
        # Validações específicas
        self.product_name_validator.validate(product_details['nome'])
        self.url_validator.validate(product_details['url'])
        self.filename_validator.validate(product_details['filename'])
        self.boolean_validator.validate(product_details['enviar'])
        
        # Validação das datas
        start_date = self.date_format_validator.validate(product_details['periodicidade'])
        end_date = self.date_format_validator.validate(product_details['periodicidadeFinal'])
        
        self.date_range_validator.validate({
            'start': start_date,
            'end': end_date
        })
        
        return product_details
    
    def validate(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Método principal de validação"""
        try:
            validated_data = self.validate_params(data)
            
            print("✅ Validação dos dados de entrada concluída com sucesso")
            print(f"📝 Produto validado: {validated_data['nome']}")
            print(f"📅 Período: {validated_data['dataProduto']}")
            print(f"📁 Arquivo: {validated_data['filename']}")
            print(f"🔗 URL: {validated_data['url'][:50]}...")
            
            return validated_data
            
        except ValidationError as e:
            error_msg = f"❌ Erro na validação dos dados: {str(e)}"
            print(error_msg)
            raise ValidationError(error_msg)
        except Exception as e:
            error_msg = f"❌ Erro inesperado na validação: {str(e)}"
            print(error_msg)
            raise ValidationError(error_msg)