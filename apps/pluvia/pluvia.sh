#!/bin/bash

# export DISPLAY=:61.0

# export PYTHONPATH="/WX2TB/Documentos/fontes/bibliotecas"

. /WX2TB/Documentos/fontes/bibliotecas/env_shar/bin/activate
cd /WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/pluvia

python pluvia.py atualizacaoDiaria preliminar $2 


