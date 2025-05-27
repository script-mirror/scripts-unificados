import re
import os
import sys
import pandas as pd  # Adicionando pandas para processamento de Excel
from datetime import datetime
from typing import Dict, Any, Optional, List, Tuple

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__))) #NEVER REMOVE THIS LINE
from validator_deck_preliminar_newave import DeckPreliminarNewaveValidator # type: ignore
from ....utils.repository_webhook import SharedRepository # type: ignore

class DeckPreliminarNewaveService:
    def __init__(self):
        self.repository = SharedRepository()
        self.validator = DeckPreliminarNewaveValidator()

    def importar_deck_preliminar_newave(**kwargs):
        return ''