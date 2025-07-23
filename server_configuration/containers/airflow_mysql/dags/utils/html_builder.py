from datetime import datetime

class HtmlBuilder:
    def __init__(self):
        self.table_generators = {
            'unsi': self._gerar_tabela_unsi,
            'mmgd': self._gerar_tabela_mmgd,
            'mmgd_total': self._gerar_tabela_mmgd_total,
            'cargaglobal': self._gerar_tabela_carga_global,
            'cargaliquida': self._gerar_tabela_carga_liquida,
            'ande': self._gerar_tabela_ande,
            'diferenca_cargas': self._gerar_tabela_diferenca
        }
        
    def gerar_html(self, tabela, dados, **kwargs):
        """
        Gera HTML para o tipo de tabela especificado
        
        Args:
            tabela (str): O tipo de tabela a ser gerada
            dados (dict): Dados necessários para gerar a tabela
            **kwargs: Argumentos adicionais específicos para cada tipo de tabela
            
        Returns:
            str: HTML da tabela gerada
        
        Raises:
            ValueError: Se o tipo de tabela não for suportado
        """
        # Verifica se o tipo de tabela é suportado
        if tabela not in self.table_generators:
            raise ValueError(f"Geração de tabela não mapeado!: {tabela}. Opções disponíveis: {list(self.table_generators.keys())}")
        
        # Chama o gerador de tabela apropriado
        return self.table_generators[tabela](dados, **kwargs)
    
    def _gerar_tabela_unsi(self, dados, **kwargs):
        """
        Gera tabela HTML para dados de geração de usinas não simuladas
        
        Args:
            dados (dict): Dados contendo informações de decks antigo e novo
            
        Returns:
            str: HTML da tabela de diferença de geração UNSI
        """
        if not dados or len(dados) < 2:
            return "<p>Dados insuficientes para gerar a tabela de diferença.</p>"
        
        # Ordenar os dados pelo dt_deck (mais antigo primeiro)
        dados_ordenados = sorted(dados, key=lambda x: x['dt_deck'])
        
        # Extrair dados do deck antigo e novo
        deck_antigo = dados_ordenados[0]
        deck_novo = dados_ordenados[-1]
        
        # Converter os dados para um formato mais fácil de manipular
        dados_antigo = {(item['vl_ano'], item['vl_mes']): item['vl_deck_unsi'] for item in deck_antigo['data']}
        dados_novo = {(item['vl_ano'], item['vl_mes']): item['vl_deck_unsi'] for item in deck_novo['data']}
        
        # Encontrar todos os anos e meses únicos
        anos = sorted(set([item['vl_ano'] for item in deck_antigo['data'] + deck_novo['data']]))
        meses = list(range(1, 13))  # 1 a 12
        
        # Construir a tabela HTML
        html = f"""
        <style>
            table.deck-diff {{
                border-collapse: collapse;
                width: 100%;
                font-family: Arial, sans-serif;
                font-size: 12px;
            }}
            
            table.deck-diff th, table.deck-diff td {{
                border: 1px solid #ddd;
                padding: 6px;
                text-align: center;
            }}
            
            table.deck-diff th {{
                background-color: #f2f2f2;
                font-weight: bold;
            }}
            
            table.deck-diff tr:nth-child(even) {{
                background-color: #f9f9f9;
            }}
            
            .positive {{
                color: green;
            }}
            
            .negative {{
                color: red;
            }}
            
            .caption {{
                font-weight: bold;
                margin-bottom: 8px;
                font-size: 14px;
            }}
        </style>
        
        <table class="deck-diff">
            <thead>
                <tr>
                    <th>Ano</th>
                    <th>Jan</th>
                    <th>Fev</th>
                    <th>Mar</th>
                    <th>Abr</th>
                    <th>Mai</th>
                    <th>Jun</th>
                    <th>Jul</th>
                    <th>Ago</th>
                    <th>Set</th>
                    <th>Out</th>
                    <th>Nov</th>
                    <th>Dez</th>
                </tr>
            </thead>
            <tbody>
        """
        
        # Preencher os dados na tabela
        for ano in anos:
            html += f"<tr><td>{ano}</td>"
            
            for mes in meses:
                valor_antigo = dados_antigo.get((ano, mes), None)
                valor_novo = dados_novo.get((ano, mes), None)
                
                if valor_antigo is not None and valor_novo is not None:
                    # Regra: se o valor do deck novo for zero ou menor, a diferença também será zero
                    if valor_novo <= 0:
                        diferenca = 0
                    else:
                        diferenca = valor_novo - valor_antigo
                    
                    classe_css = "positive" if diferenca >= 0 else "negative"
                    
                    html += f'<td class="{classe_css}">{diferenca:.0f}</td>'
                else:
                    html += "<td>-</td>"
            
            html += "</tr>"
        
        html += """
            </tbody>
        </table>
        """
        
        return html
    
    def _gerar_tabela_mmgd(self, dados, **kwargs):
        """
        Gera tabela HTML para dados de geração MMGD
        
        Args:
            dados (dict): Dados contendo informações de decks antigo e novo
            
        Returns:
            str: HTML da tabela de diferença de geração MMGD
        """
        if not dados or len(dados) < 2:
            return "<p>Dados insuficientes para gerar a tabela de diferença MMGD.</p>"
        
        # Ordenar os dados pelo dt_deck (mais antigo primeiro)
        dados_ordenados = sorted(dados, key=lambda x: x['dt_deck'])
        
        # Extrair dados do deck antigo e novo
        deck_antigo = dados_ordenados[0]
        deck_novo = dados_ordenados[-1]
        
        # Converter os dados para um formato mais fácil de manipular
        dados_antigo = {(item['vl_ano'], item['vl_mes']): item['vl_deck_mmgd'] for item in deck_antigo['data']}
        dados_novo = {(item['vl_ano'], item['vl_mes']): item['vl_deck_mmgd'] for item in deck_novo['data']}
        
        # Encontrar todos os anos e meses únicos
        anos = sorted(set([item['vl_ano'] for item in deck_antigo['data'] + deck_novo['data']]))
        meses = list(range(1, 13))  # 1 a 12
        
        # Construir a tabela HTML
        html = f"""
        <style>
            table.deck-diff {{
                border-collapse: collapse;
                width: 100%;
                font-family: Arial, sans-serif;
                font-size: 12px;
            }}
            
            table.deck-diff th, table.deck-diff td {{
                border: 1px solid #ddd;
                padding: 6px;
                text-align: center;
            }}
            
            table.deck-diff th {{
                background-color: #f2f2f2;
                font-weight: bold;
            }}
            
            table.deck-diff tr:nth-child(even) {{
                background-color: #f9f9f9;
            }}
            
            .positive {{
                color: green;
            }}
            
            .negative {{
                color: red;
            }}
            
            .caption {{
                font-weight: bold;
                margin-bottom: 8px;
                font-size: 14px;
            }}
        </style>
        
        <table class="deck-diff">
            <thead>
                <tr>
                    <th>Ano</th>
                    <th>Jan</th>
                    <th>Fev</th>
                    <th>Mar</th>
                    <th>Abr</th>
                    <th>Mai</th>
                    <th>Jun</th>
                    <th>Jul</th>
                    <th>Ago</th>
                    <th>Set</th>
                    <th>Out</th>
                    <th>Nov</th>
                    <th>Dez</th>
                </tr>
            </thead>
            <tbody>
        """
        
        # Preencher os dados na tabela
        for ano in anos:
            html += f"<tr><td>{ano}</td>"
            
            for mes in meses:
                valor_antigo = dados_antigo.get((ano, mes), None)
                valor_novo = dados_novo.get((ano, mes), None)
                
                if valor_antigo is not None and valor_novo is not None:
                    # Regra: se o valor do deck novo for zero ou menor, a diferença também será zero
                    if valor_novo <= 0:
                        diferenca = 0
                    else:
                        diferenca = valor_novo - valor_antigo
                    
                    classe_css = "positive" if diferenca >= 0 else "negative"
                    
                    html += f'<td class="{classe_css}">{diferenca:.0f}</td>'
                else:
                    html += "<td>-</td>"
            
            html += "</tr>"
        
        html += """
            </tbody>
        </table>
        """
        
        return html
    
    def _gerar_tabela_mmgd_total(self, dados, **kwargs):
        """
        Gera tabela HTML para dados de geração MMGD Total (Base + Expansão)
        
        Args:
            dados (dict): Dados contendo informações de decks antigo e novo
            
        Returns:
            str: HTML da tabela de diferença de geração MMGD Total
        """
        if not dados or len(dados) < 2:
            return "<p>Dados insuficientes para gerar a tabela de diferença MMGD Total.</p>"
        
        # Ordenar os dados pelo dt_deck (mais antigo primeiro)
        dados_ordenados = sorted(dados, key=lambda x: x['dt_deck'])
        
        # Extrair dados do deck antigo e novo
        deck_antigo = dados_ordenados[0]
        deck_novo = dados_ordenados[-1]
        
        # Converter os dados para um formato mais fácil de manipular
        dados_antigo = {(item['vl_ano'], item['vl_mes']): item['vl_deck_mmgd_total'] for item in deck_antigo['data']}
        dados_novo = {(item['vl_ano'], item['vl_mes']): item['vl_deck_mmgd_total'] for item in deck_novo['data']}
        
        # Encontrar todos os anos e meses únicos
        anos = sorted(set([item['vl_ano'] for item in deck_antigo['data'] + deck_novo['data']]))
        meses = list(range(1, 13))  # 1 a 12
        
        # Construir a tabela HTML
        html = f"""
        <style>
            table.deck-diff {{
                border-collapse: collapse;
                width: 100%;
                font-family: Arial, sans-serif;
                font-size: 12px;
            }}
            
            table.deck-diff th, table.deck-diff td {{
                border: 1px solid #ddd;
                padding: 6px;
                text-align: center;
            }}
            
            table.deck-diff th {{
                background-color: #f2f2f2;
                font-weight: bold;
            }}
            
            table.deck-diff tr:nth-child(even) {{
                background-color: #f9f9f9;
            }}
            
            .positive {{
                color: green;
            }}
            
            .negative {{
                color: red;
            }}
            
            .caption {{
                font-weight: bold;
                margin-bottom: 8px;
                font-size: 14px;
            }}
        </style>
        
        <table class="deck-diff">
            <thead>
                <tr>
                    <th>Ano</th>
                    <th>Jan</th>
                    <th>Fev</th>
                    <th>Mar</th>
                    <th>Abr</th>
                    <th>Mai</th>
                    <th>Jun</th>
                    <th>Jul</th>
                    <th>Ago</th>
                    <th>Set</th>
                    <th>Out</th>
                    <th>Nov</th>
                    <th>Dez</th>
                </tr>
            </thead>
            <tbody>
        """
        
        # Preencher os dados na tabela
        for ano in anos:
            html += f"<tr><td>{ano}</td>"
            
            for mes in meses:
                valor_antigo = dados_antigo.get((ano, mes), None)
                valor_novo = dados_novo.get((ano, mes), None)
                
                if valor_antigo is not None and valor_novo is not None:
                    # Regra: se o valor do deck novo for zero ou menor, a diferença também será zero
                    if valor_novo <= 0:
                        diferenca = 0
                    else:
                        diferenca = valor_novo - valor_antigo
                    
                    classe_css = "positive" if diferenca >= 0 else "negative"
                    
                    html += f'<td class="{classe_css}">{diferenca:.0f}</td>'
                else:
                    html += "<td>-</td>"
            
            html += "</tr>"
        
        html += """
            </tbody>
        </table>
        """
        
        return html
    
    def _gerar_tabela_carga_global(self, dados, **kwargs):
        """
        Gera tabela HTML para dados de carga global
        
        Args:
            dados (dict): Dados contendo informações de decks antigo e novo
            
        Returns:
            str: HTML da tabela de diferença de carga global
        """
        if not dados or len(dados) < 2:
            return "<p>Dados insuficientes para gerar a tabela de diferença de carga global.</p>"
        
        # Ordenar os dados pelo dt_deck (mais antigo primeiro)
        dados_ordenados = sorted(dados, key=lambda x: x['dt_deck'])
        
        # Extrair dados do deck antigo e novo
        deck_antigo = dados_ordenados[0]
        deck_novo = dados_ordenados[-1]
        
        # Converter os dados para um formato mais fácil de manipular
        dados_antigo = {(item['vl_ano'], item['vl_mes']): item['vl_deck_carga_global'] for item in deck_antigo['data']}
        dados_novo = {(item['vl_ano'], item['vl_mes']): item['vl_deck_carga_global'] for item in deck_novo['data']}
        
        # Encontrar todos os anos e meses únicos
        anos = sorted(set([item['vl_ano'] for item in deck_antigo['data'] + deck_novo['data']]))
        meses = list(range(1, 13))  # 1 a 12
        
        # Construir a tabela HTML
        html = f"""
        <style>
            table.deck-diff {{
                border-collapse: collapse;
                width: 100%;
                font-family: Arial, sans-serif;
                font-size: 12px;
            }}
            
            table.deck-diff th, table.deck-diff td {{
                border: 1px solid #ddd;
                padding: 6px;
                text-align: center;
            }}
            
            table.deck-diff th {{
                background-color: #f2f2f2;
                font-weight: bold;
            }}
            
            table.deck-diff tr:nth-child(even) {{
                background-color: #f9f9f9;
            }}
            
            .positive {{
                color: green;
            }}
            
            .negative {{
                color: red;
            }}
            
            .caption {{
                font-weight: bold;
                margin-bottom: 8px;
                font-size: 14px;
            }}
        </style>
        
        <table class="deck-diff">
            <thead>
                <tr>
                    <th>Ano</th>
                    <th>Jan</th>
                    <th>Fev</th>
                    <th>Mar</th>
                    <th>Abr</th>
                    <th>Mai</th>
                    <th>Jun</th>
                    <th>Jul</th>
                    <th>Ago</th>
                    <th>Set</th>
                    <th>Out</th>
                    <th>Nov</th>
                    <th>Dez</th>
                </tr>
            </thead>
            <tbody>
        """
        
        # Preencher os dados na tabela
        for ano in anos:
            html += f"<tr><td>{ano}</td>"
            
            for mes in meses:
                valor_antigo = dados_antigo.get((ano, mes), None)
                valor_novo = dados_novo.get((ano, mes), None)
                
                if valor_antigo is not None and valor_novo is not None:
                    # Regra: se o valor do deck novo for zero ou menor, a diferença também será zero
                    if valor_novo <= 0:
                        diferenca = 0
                    else:
                        diferenca = valor_novo - valor_antigo
                    
                    classe_css = "positive" if diferenca >= 0 else "negative"
                    
                    html += f'<td class="{classe_css}">{diferenca:.0f}</td>'
                else:
                    html += "<td>-</td>"
            
            html += "</tr>"
        
        html += """
            </tbody>
        </table>
        """
        
        return html
    
    def _gerar_tabela_carga_liquida(self, dados, **kwargs):
        """
        Gera tabela HTML para dados de carga líquida
        
        Args:
            dados (dict): Dados contendo informações de decks antigo e novo
            
        Returns:
            str: HTML da tabela de diferença de carga líquida
        """
        if not dados or len(dados) < 2:
            return "<p>Dados insuficientes para gerar a tabela de diferença de carga líquida.</p>"
        
        # Ordenar os dados pelo dt_deck (mais antigo primeiro)
        dados_ordenados = sorted(dados, key=lambda x: x['dt_deck'])
        
        # Extrair dados do deck antigo e novo
        deck_antigo = dados_ordenados[0]
        deck_novo = dados_ordenados[-1]
        
        # Converter os dados para um formato mais fácil de manipular
        dados_antigo = {(item['vl_ano'], item['vl_mes']): item['vl_deck_carga_liquida'] for item in deck_antigo['data']}
        dados_novo = {(item['vl_ano'], item['vl_mes']): item['vl_deck_carga_liquida'] for item in deck_novo['data']}
        
        # Encontrar todos os anos e meses únicos
        anos = sorted(set([item['vl_ano'] for item in deck_antigo['data'] + deck_novo['data']]))
        meses = list(range(1, 13))  # 1 a 12
        
        # Construir a tabela HTML
        html = f"""
        <style>
            table.deck-diff {{
                border-collapse: collapse;
                width: 100%;
                font-family: Arial, sans-serif;
                font-size: 12px;
            }}
            
            table.deck-diff th, table.deck-diff td {{
                border: 1px solid #ddd;
                padding: 6px;
                text-align: center;
            }}
            
            table.deck-diff th {{
                background-color: #f2f2f2;
                font-weight: bold;
            }}
            
            table.deck-diff tr:nth-child(even) {{
                background-color: #f9f9f9;
            }}
            
            .positive {{
                color: green;
            }}
            
            .negative {{
                color: red;
            }}
            
            .caption {{
                font-weight: bold;
                margin-bottom: 8px;
                font-size: 14px;
            }}
        </style>
        
        <table class="deck-diff">
            <thead>
                <tr>
                    <th>Ano</th>
                    <th>Jan</th>
                    <th>Fev</th>
                    <th>Mar</th>
                    <th>Abr</th>
                    <th>Mai</th>
                    <th>Jun</th>
                    <th>Jul</th>
                    <th>Ago</th>
                    <th>Set</th>
                    <th>Out</th>
                    <th>Nov</th>
                    <th>Dez</th>
                </tr>
            </thead>
            <tbody>
        """
        
        # Preencher os dados na tabela
        for ano in anos:
            html += f"<tr><td>{ano}</td>"
            
            for mes in meses:
                valor_antigo = dados_antigo.get((ano, mes), None)
                valor_novo = dados_novo.get((ano, mes), None)
                
                if valor_antigo is not None and valor_novo is not None:
                    # Regra: se o valor do deck novo for zero ou menor, a diferença também será zero
                    if valor_novo <= 0:
                        diferenca = 0
                    else:
                        diferenca = valor_novo - valor_antigo
                    
                    classe_css = "positive" if diferenca >= 0 else "negative"
                    
                    html += f'<td class="{classe_css}">{diferenca:.0f}</td>'
                else:
                    html += "<td>-</td>"
            
            html += "</tr>"
        
        html += """
            </tbody>
        </table>
        """
        
        return html
    
    def _gerar_tabela_ande(self, dados, **kwargs):
        """
        Gera tabela HTML para dados de carga do ANDE
        
        Args:
            dados (dict): Dados contendo informações de decks antigo e novo
            
        Returns:
            str: HTML da tabela de diferença de carga ANDE
        """
        if not dados or len(dados) < 2:
            return "<p>Dados insuficientes para gerar a tabela de diferença de carga ANDE.</p>"
        
        # Ordenar os dados pelo dt_deck (mais antigo primeiro)
        dados_ordenados = sorted(dados, key=lambda x: x['dt_deck'])
        
        # Extrair dados do deck antigo e novo
        deck_antigo = dados_ordenados[0]
        deck_novo = dados_ordenados[-1]
        
        # Verificar se os dados contêm a chave necessária
        if not deck_antigo.get('data') or not deck_novo.get('data'):
            return "<p>Estrutura de dados inválida para carga ANDE.</p>"
        
        # Verificar se pelo menos um item tem a chave vl_ande_total
        sample_item_antigo = deck_antigo['data'][0] if deck_antigo['data'] else {}
        sample_item_novo = deck_novo['data'][0] if deck_novo['data'] else {}
        
        if 'vl_ande_total' not in sample_item_antigo and 'vl_ande_total' not in sample_item_novo:
            return "<p>Dados de carga ANDE não encontrados (campo 'vl_ande_total' ausente).</p>"
        
        # Converter os dados para um formato mais fácil de manipular
        dados_antigo = {(item['vl_ano'], item['vl_mes']): item.get('vl_ande_total', 0) for item in deck_antigo['data']}
        dados_novo = {(item['vl_ano'], item['vl_mes']): item.get('vl_ande_total', 0) for item in deck_novo['data']}
        
        # Encontrar todos os anos e meses únicos
        anos = sorted(set([item['vl_ano'] for item in deck_antigo['data'] + deck_novo['data']]))
        meses = list(range(1, 13))  # 1 a 12
        
        # Construir a tabela HTML
        html = f"""
        <style>
            table.deck-diff {{
                border-collapse: collapse;
                width: 100%;
                font-family: Arial, sans-serif;
                font-size: 12px;
            }}
            
            table.deck-diff th, table.deck-diff td {{
                border: 1px solid #ddd;
                padding: 6px;
                text-align: center;
            }}
            
            table.deck-diff th {{
                background-color: #f2f2f2;
                font-weight: bold;
            }}
            
            table.deck-diff tr:nth-child(even) {{
                background-color: #f9f9f9;
            }}
            
            .positive {{
                color: green;
            }}
            
            .negative {{
                color: red;
            }}
            
            .caption {{
                font-weight: bold;
                margin-bottom: 8px;
                font-size: 14px;
            }}
        </style>
        
        <table class="deck-diff">
            <thead>
                <tr>
                    <th>Ano</th>
                    <th>Jan</th>
                    <th>Fev</th>
                    <th>Mar</th>
                    <th>Abr</th>
                    <th>Mai</th>
                    <th>Jun</th>
                    <th>Jul</th>
                    <th>Ago</th>
                    <th>Set</th>
                    <th>Out</th>
                    <th>Nov</th>
                    <th>Dez</th>
                </tr>
            </thead>
            <tbody>
        """
        
        # Preencher os dados na tabela
        for ano in anos:
            html += f"<tr><td>{ano}</td>"
            
            for mes in meses:
                valor_antigo = dados_antigo.get((ano, mes), None)
                valor_novo = dados_novo.get((ano, mes), None)
                
                if valor_antigo is not None and valor_novo is not None:
                    # Regra: se o valor do deck novo for zero ou menor, a diferença também será zero
                    if valor_novo <= 0:
                        diferenca = 0
                    else:
                        diferenca = valor_novo - valor_antigo
                    
                    classe_css = "positive" if diferenca >= 0 else "negative"
                    
                    html += f'<td class="{classe_css}">{diferenca:.0f}</td>'
                else:
                    html += "<td>-</td>"
            
            html += "</tr>"
        
        html += """
            </tbody>
        </table>
        """
        
        return html
    
    def _gerar_tabela_diferenca(self, dados=None, dados_unsi=None, dados_ande=None, dados_mmgd_total=None, dados_carga_global=None, dados_carga_liquida=None):
        """
        Gera um documento HTML completo contendo todas as tabelas de diferenças disponíveis

        Args:
            dados: Parâmetro ignorado (mantido por compatibilidade)
            dados_unsi (list): Dados para tabela de usinas não simuladas
            dados_ande (list): Dados para tabela de carga ANDE
            dados_mmgd_total (list): Dados para tabela de MMGD Total (Base + Expansão)
            dados_carga_global (list): Dados para tabela de carga global
            dados_carga_liquida (list): Dados para tabela de carga líquida

        Returns:
            str: HTML completo com todas as tabelas
        """

        # Gera as tabelas individuais
        html_tabela_diff_unsi = self._gerar_tabela_unsi(dados_unsi) if dados_unsi else "<p>Dados não disponíveis para usinas não simuladas.</p>"
        html_tabela_diff_ande = self._gerar_tabela_ande(dados_ande) if dados_ande else "<p>Dados não disponíveis para carga ANDE.</p>"
        html_tabela_diff_mmgd_total = self._gerar_tabela_mmgd_total(dados_mmgd_total) if dados_mmgd_total else "<p>Dados não disponíveis para MMGD Total.</p>"
        html_tabela_diff_carga_global = self._gerar_tabela_carga_global(dados_carga_global) if dados_carga_global else "<p>Dados não disponíveis para carga global.</p>"
        html_tabela_diff_carga_liquida = self._gerar_tabela_carga_liquida(dados_carga_liquida) if dados_carga_liquida else "<p>Dados não disponíveis para carga líquida.</p>"

        # Combinar todas as tabelas em um documento HTML único
        html_completo = f"""
        <!DOCTYPE html>
        <html lang="pt-BR">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Relatório de Diferenças - Deck Preliminar Newave</title>
            <style>
                body {{
                    font-family: Arial, sans-serif;
                    margin: 20px;
                    background-color: #f5f5f5;
                }}

                .container {{
                    max-width: 1200px;
                    margin: 0 auto;
                    background-color: white;
                    padding: 20px;
                    border-radius: 8px;
                    box-shadow: 0 2px 10px rgba(0,0,0,0.1);
                }}

                .header {{
                    text-align: center;
                    margin-bottom: 30px;
                    padding-bottom: 20px;
                    border-bottom: 2px solid #e0e0e0;
                }}

                .header h1 {{
                    color: #333;
                    margin-bottom: 10px;
                    font-size: 24px;
                }}

                .header p {{
                    color: #666;
                    font-size: 14px;
                    margin: 5px 0;
                }}

                .table-section {{
                    margin-bottom: 40px;
                }}

                .table-section h2 {{
                    color: #2c3e50;
                    border-left: 4px solid #3498db;
                    padding-left: 15px;
                    margin-bottom: 15px;
                    font-size: 18px;
                }}

                .no-data {{
                    text-align: center;
                    color: #888;
                    font-style: italic;
                    padding: 20px;
                    background-color: #f9f9f9;
                    border-radius: 4px;
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="table-section">
                    <h2>Usinas Não Simuladas (UNSI)</h2>
                    {html_tabela_diff_unsi}
                </div>
                
                <div class="table-section">
                    <h2>Carga ANDE</h2>
                    {html_tabela_diff_ande}
                </div>

                <div class="table-section">
                    <h2>MMGD Total (Base + Expansão)</h2>
                    {html_tabela_diff_mmgd_total}
                </div>

                <div class="table-section">
                    <h2>Carga Global</h2>
                    {html_tabela_diff_carga_global}
                </div>

                <div class="table-section">
                    <h2>Carga Líquida</h2>
                    {html_tabela_diff_carga_liquida}
                </div>
            </div>
        </body>
        </html>
        """

        return html_completo