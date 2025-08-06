"""
Validador específico para produtos Deck NEWAVE (Preliminar e Definitivo).
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

class DecksNewaveValidator(BaseWebhookValidator):
    """Validador específico para dados do Deck NEWAVE (Preliminar e Definitivo)"""
    
    def __init__(self):
        super().__init__()
        
        # Configuração dos validadores específicos para Deck NEWAVE
        self.filename_validator = FilenameValidator(['.zip'])
        self.product_name_validator = ProductNameValidator(
            expected_patterns=['Deck NEWAVE Preliminar', 'DECK NEWAVE DEFINITIVO']
        )
    
    def validate_product_details(self, product_details: Dict[str, Any]) -> Dict[str, Any]:
        """Valida os detalhes específicos do produto Deck NEWAVE"""
        
        # Validações comuns (campos obrigatórios, URL, datas, etc.)
        self.validate_common_fields(product_details)
        
        # Validações específicas do Deck NEWAVE
        self.filename_validator.validate(product_details['filename'])
        self.product_name_validator.validate(product_details['nome'])
        
        return product_details
    
    def validate(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Método principal de validação para Deck NEWAVE"""
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