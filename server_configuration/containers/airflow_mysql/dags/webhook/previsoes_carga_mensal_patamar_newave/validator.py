"""
Validador específico para produtos de Previsões de Carga Mensal por Patamar NEWAVE.
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


class PrevisoesCargaMensalNewaveValidator(BaseWebhookValidator):
    """Validador específico para dados de Previsões de Carga Mensal por Patamar NEWAVE"""
    
    def __init__(self):
        super().__init__()
        
        # Configuração dos validadores específicos
        self.filename_validator = FilenameValidator(['.zip'])
        self.product_name_validator = ProductNameValidator(
            expected_patterns=['PREVISÕES DE CARGA MENSAL', 'CARGA MENSAL', 'PATAMAR NEWAVE']
        )
    
    def validate_product_details(self, product_details: Dict[str, Any]) -> Dict[str, Any]:
        """Valida os detalhes específicos do produto Previsões de Carga Mensal"""
        
        # Validações comuns (campos obrigatórios, URL, datas, etc.)
        self.validate_common_fields(product_details)
        
        # Validações específicas
        self.filename_validator.validate(product_details['filename'])
        self.product_name_validator.validate(product_details['nome'])
        
        # Validação específica da data (formato MM/YYYY)
        data_produto = product_details.get('dataProduto', '')
        if '/' not in data_produto or len(data_produto.split('/')) != 2:
            raise ValidationError(f"Data do produto deve estar no formato MM/YYYY: {data_produto}")
        
        try:
            month, year = data_produto.split('/')
            month_int = int(month)
            year_int = int(year)
            
            if not (1 <= month_int <= 12):
                raise ValidationError(f"Mês deve estar entre 1 e 12: {month_int}")
            
            if not (2020 <= year_int <= 2050):
                raise ValidationError(f"Ano deve estar entre 2020 e 2050: {year_int}")
                
        except ValueError:
            raise ValidationError(f"Formato de data inválido (deve ser MM/YYYY): {data_produto}")
        
        return product_details
    
    def validate(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Método principal de validação para Previsões de Carga Mensal"""
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
