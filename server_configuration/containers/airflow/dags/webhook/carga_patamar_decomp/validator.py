"""
Validador específico para produtos de Carga por Patamar DECOMP.
"""

import os
import sys
from typing import Dict, Any

# Adicionar o caminho para utils
current_dir = os.path.dirname(os.path.abspath(__file__))
utils_path = os.path.join(os.path.dirname(os.path.dirname(current_dir)), 'utils')
sys.path.insert(0, utils_path)

from validator_repository import (
    BaseWebhookValidator, 
    ValidationError, 
    FilenameValidator, 
    ProductNameValidator
)


class CargaPatamarDecompValidator(BaseWebhookValidator):
    """Validador específico para dados de Carga por Patamar DECOMP"""
    
    def __init__(self):
        super().__init__()
        
        # Configuração dos validadores específicos para Carga Patamar DECOMP
        self.filename_validator = FilenameValidator(['.zip', '.xlsx', '.xls'])
        self.product_name_validator = ProductNameValidator(
            expected_patterns=['CARGA PATAMAR', 'DECOMP', 'PREVISÃO CARGA']
        )
    
    def validate_product_details(self, product_details: Dict[str, Any]) -> Dict[str, Any]:
        """Valida os detalhes específicos do produto Carga Patamar DECOMP"""
        
        # Validações comuns (campos obrigatórios, URL, datas, etc.)
        self.validate_common_fields(product_details)
        
        # Validações específicas do Carga Patamar DECOMP
        self.filename_validator.validate(product_details['filename'])
        self.product_name_validator.validate(product_details['nome'])
        
        # Validação específica para formato de data (pode ser DD/MM/YYYY ou MM/YYYY)
        data_produto = product_details.get('dataProduto', '')
        if not data_produto:
            raise ValidationError("Data do produto é obrigatória")
        
        # Aceitar tanto DD/MM/YYYY quanto MM/YYYY
        if '/' in data_produto:
            parts = data_produto.split('/')
            if len(parts) == 3:  # DD/MM/YYYY
                try:
                    day, month, year = map(int, parts)
                    if not (1 <= day <= 31 and 1 <= month <= 12 and 2020 <= year <= 2050):
                        raise ValidationError(f"Data inválida: {data_produto}")
                except ValueError:
                    raise ValidationError(f"Formato de data inválido: {data_produto}")
            elif len(parts) == 2:  # MM/YYYY
                try:
                    month, year = map(int, parts)
                    if not (1 <= month <= 12 and 2020 <= year <= 2050):
                        raise ValidationError(f"Data inválida: {data_produto}")
                except ValueError:
                    raise ValidationError(f"Formato de data inválido: {data_produto}")
            else:
                raise ValidationError(f"Formato de data deve ser DD/MM/YYYY ou MM/YYYY: {data_produto}")
        else:
            raise ValidationError(f"Data deve conter barras (/): {data_produto}")
        
        return product_details
    
    def validate(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Método principal de validação para Carga Patamar DECOMP"""
        try:
            # Validar estrutura base
            product_details = self.validate_base_structure(data)
            
            # Validar detalhes específicos do produto
            validated_data = self.validate_product_details(product_details)
            
            # Log de sucesso
            self.log_validation_success(validated_data)
            
            return validated_data
            
        except Exception as e:
            self.handle_validation_error(e)
            return {}
