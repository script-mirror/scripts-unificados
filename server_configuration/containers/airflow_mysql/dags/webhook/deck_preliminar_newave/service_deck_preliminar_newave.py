import re
import os
import sys
import pandas as pd  # Adicionando pandas para processamento de Excel
from datetime import datetime
from typing import Dict, Any, Optional, List, Tuple



utils_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'utils')
sys.path.insert(0, utils_path)
from utils.repository_webhook import SharedRepository

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from validator_deck_preliminar_newave import DeckPreliminarNewaveValidator

class DeckPreliminarNewaveService:
    def __init__(self):
        self.repository = SharedRepository()
        self.validator = DeckPreliminarNewaveValidator()

    def importar_deck_preliminar_newave(**kwargs):
        return ''