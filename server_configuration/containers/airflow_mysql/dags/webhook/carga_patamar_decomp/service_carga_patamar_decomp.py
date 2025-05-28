import re
import os
import sys
import pandas as pd  # Adicionando pandas para processamento de Excel
from datetime import datetime
from typing import Dict, Any, Optional, List, Tuple

utils_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'utils')
sys.path.insert(0, utils_path)
from utils.repository_webhook import SharedRepository

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__))) #NEVER REMOVE THIS LINE
from validator_carga_patamar_decomp import CargaPatamarDecompValidator, ValidationError, CargaPMOValidator # type: ignore


class CargaPatamarDecompService:
    """
    Service layer para processamento do Carga Patamar Decomp
    Contém toda a lógica de negócio separada da DAG
    """
    
    def __init__(self):
        self.repository = SharedRepository()
        self.validator = CargaPatamarDecompValidator()

    @staticmethod
    def validar_dados_entrada(**kwargs):
        """Valida os dados de entrada usando o validador"""
        validator = CargaPatamarDecompValidator()
        params = kwargs.get('params', {})
        return validator.validate(params)

    @staticmethod
    def download_arquivos(**kwargs):
        """Faz o download dos arquivos necessários"""
        repository = SharedRepository()
        params = kwargs.get('params', {})
        print("Parâmetros recebidos para download:", params)
        
        try:
            product_details = params.get('product_details', {})
            webhook_id = product_details.get('webhookId')
            filename = product_details.get('filename')
            
            if not webhook_id or not filename:
                raise ValueError("webhookId e filename são obrigatórios")
            
            # Define caminho de download
            download_path = "/tmp/carga_patamar_decomp"
            
            # Faz download do arquivo via repository
            file_path = repository.download_webhook_file(
                webhook_id=webhook_id,
                filename=filename,
                download_path=download_path
            )
            
            # Valida se arquivo foi baixado corretamente
            if not repository.validate_file_exists(file_path):
                raise Exception(f"Arquivo {filename} não foi baixado corretamente")
            
            print(f"Arquivo baixado com sucesso: {file_path}")
            
            return {
                'file_path': file_path,
                'success': True,
                'message': f'Arquivo {filename} baixado com sucesso'
            }
            
        except Exception as e:
            error_msg = f"Erro ao baixar arquivos: {str(e)}"
            print(error_msg)
            return {
                'success': False,
                'error': error_msg
            }

    @staticmethod
    def extrair_arquivos(**kwargs):
        """Extrai arquivos usando dados da task anterior via XCom"""
        try:
            # Busca dados da task anterior via XCom
            task_instance = kwargs['task_instance']
            download_result = task_instance.xcom_pull(task_ids='download_arquivos')
            
            print(f"Dados recebidos via XCom: {download_result}")
            
            if not download_result or not download_result.get('success'):
                raise Exception(f"Falha no download anterior: {download_result.get('error', 'Erro desconhecido')}")
            
            file_path = download_result.get('file_path')
            if not file_path:
                raise Exception("Caminho do arquivo não encontrado nos dados XCom")
            
            print(f"Processando arquivo: {file_path}")
            
            # Verifica se o arquivo ainda existe
            if not os.path.exists(file_path):
                raise Exception(f"Arquivo não encontrado: {file_path}")
            
            # Aqui você pode adicionar lógica para extrair arquivos ZIP
            if file_path.endswith('.zip'):
                import zipfile
                
                # Define diretório de extração
                extract_path = os.path.dirname(file_path)
                
                with zipfile.ZipFile(file_path, 'r') as zip_ref:
                    zip_ref.extractall(extract_path)
                    extracted_files = zip_ref.namelist()
                
                print(f"Arquivos extraídos: {extracted_files}")
                
                return {
                    'success': True,
                    'original_file': file_path,
                    'extract_path': extract_path,
                    'extracted_files': extracted_files,
                    'message': f'Arquivos extraídos com sucesso de {os.path.basename(file_path)}'
                }
            else:
                # Se não for ZIP, apenas retorna o arquivo original
                return {
                    'success': True,
                    'file_path': file_path,
                    'message': f'Arquivo {os.path.basename(file_path)} pronto para processamento'
                }
                
        except Exception as e:
            error_msg = f"Erro ao extrair arquivos: {str(e)}"
            print(error_msg)
            return {
                'success': False,
                'error': error_msg
            }

    @staticmethod
    def processar_carga_patamar_decomp(**kwargs):
        """Processa o carga patamar decomp usando dados via XCom"""
        try:
            # Busca dados da task anterior via XCom
            task_instance = kwargs['task_instance']
            extract_result = task_instance.xcom_pull(task_ids='extrair_arquivos')
            
            print(f"Dados de extração recebidos via XCom: {extract_result}")
            
            if not extract_result or not extract_result.get('success'):
                raise Exception(f"Falha na extração anterior: {extract_result.get('error', 'Erro desconhecido')}")
            
            # Obtém informações dos arquivos extraídos
            extracted_files = extract_result.get('extracted_files', [])
            extract_path = extract_result.get('extract_path')
            file_path = extract_result.get('file_path')
            
            print(f"Processando arquivos extraídos: {extracted_files}")
            print(f"Diretório de extração: {extract_path}")
            
            # Processa o Carga Patamar Decomp
            print("Processando Carga Patamar Decomp...")
            
            # Busca o arquivo de carga que começa com "Carga" e termina com "xlsx"
            carga_file = None
            for file in extracted_files:
                if file.startswith('Carga') and file.endswith('.xlsx'):
                    carga_file = os.path.join(extract_path, file)
                    print(f"Arquivo de carga encontrado: {carga_file}")
                    break
            
            carga_data = {}
            if carga_file and os.path.exists(carga_file):
                # Processa o arquivo de carga
                product_details = kwargs.get('params', {}).get('product_details', {})
                carga_data = CargaPatamarDecompService.processar_arquivo_carga(carga_file, product_details)
                print(f"Dados extraídos do arquivo de carga: {carga_data}")
            else:
                print("Arquivo de carga não encontrado entre os arquivos extraídos")
            
            return {
                'success': True,
                'processed_files': extracted_files or [file_path] if file_path else [],
                'extract_path': extract_path,
                'carga_data': carga_data,
                'message': 'Carga Patamar Decomp processada com sucesso'
            }
            
        except Exception as e:
            error_msg = f"Erro no processamento: {str(e)}"
            print(error_msg)
            return {
                'success': False,
                'error': error_msg
            }

    @staticmethod
    def processar_arquivo_carga(carga_file_path: str, product_details) -> Dict[str, Any]:
        """
        Processa o arquivo de carga Excel extraindo os dados relevantes
        
        Args:
            carga_file_path: Caminho completo para o arquivo de carga (ex: 'Carga_PMO_Maio25(Rev 4).xlsx')
            
        Returns:
            Dicionário com os dados extraídos do arquivo
        """
        try:
            print(f"Processando arquivo de carga: {carga_file_path}")
            
            # Lê o arquivo Excel com pandas
            df = pd.read_excel(carga_file_path, sheet_name=0)  # Lê a primeira aba
            
            # Extrai informações do nome do arquivo
            filename = os.path.basename(carga_file_path)
            match = re.search(r'Carga_PMO_(\D+)(\d+)(?:\(Rev\s*(\d+)\))?', filename)
            if not match:
                raise ValueError(f"Formato de nome de arquivo inválido: {filename}. Esperado: Carga_PMO_Mes##(Rev #).xlsx")
            
            mes = match.group(1) if match else None
            ano = match.group(2) if match else None
            revisao = match.group(3) if match and match.group(3) else '0'
            
            # Mapeamento dos nomes de mês em português para número do mês
            mapeamento_mes = {
                'Janeiro': 1, 'Fevereiro': 2, 'Março': 3, 'Abril': 4,
                'Maio': 5, 'Junho': 6, 'Julho': 7, 'Agosto': 8,
                'Setembro': 9, 'Outubro': 10, 'Novembro': 11, 'Dezembro': 12
            }
            if mes not in mapeamento_mes:
                raise ValueError("Mês inválido") 
            mes_numero = mapeamento_mes.get(mes)

            # Seleciona as primeiras linhas para extrair a data de início (linha do cabeçalho)
            cabecalho = df.iloc[0:10].reset_index(drop=True)
            print(f"Cabecalho do arquivo de carga:\n{cabecalho.to_string(index=False)}")
            
            # Extrai as datas de início de todas as semanas
            datas_semanas = {}  # Mapeamento coluna -> data de início
            colunas_semanas = {
                1: 'Unnamed: 7',  # 1ª Semana
                2: 'Unnamed: 8',  # 2ª Semana
                3: 'Unnamed: 9',  # 3ª Semana
                4: 'Unnamed: 10', # 4ª Semana
                5: 'Unnamed: 11', # 5ª Semana
                6: 'Unnamed: 12'  # 6ª Semana
            }
            
            for idx, row in cabecalho.iterrows():
                # Procura na linha 10 (índice 9) que contém as datas das semanas
                if idx == 9:  # A linha que contém as datas
                    for num_semana, coluna in colunas_semanas.items():
                        if coluna in row and not pd.isna(row[coluna]):
                            data_str = str(row[coluna])
                            # Formato esperado: "26/04 a 02/05"
                            match_data = re.search(r'(\d{2})/(\d{2})', data_str)
                            if match_data:
                                dia = match_data.group(1)
                                mes_data = match_data.group(2)
                                # Formata como YYYYMMDD
                                data_inicio_semana = f"20{ano}{mes_data}{dia}"
                                datas_semanas[coluna] = data_inicio_semana
                                print(f"Data da {num_semana}ª semana (coluna {coluna}): {data_inicio_semana}")
                    break
            
            # Define data_inicio como a data da primeira semana para compatibilidade
            data_inicio = datas_semanas.get('Unnamed: 7')
            
            # Se não conseguiu extrair pelo menos a primeira data, lança uma exceção
            if not data_inicio:
                raise ValueError("Não foi possível extrair a data de início do arquivo de carga. Formato de dados inválido ou inesperado.")
            
            # Seleciona especificamente as linhas entre 10 e 20 (índices 9 a 19 em base 0)
            dados_originais = df.iloc[8:20].reset_index(drop=True)
            
            # Imprime os dados originais para verificação
            print("=" * 50)
            print(f"Dados brutos das linhas 10 a 20 do arquivo {filename}:")
            print(dados_originais.to_string(index=False))
            print("=" * 50)
            
            # Extrair apenas os dados relevantes (as linhas com dados dos subsistemas e sistemas)
            # Remove linhas vazias (onde a coluna 'Unnamed: 2' é NaN)
            dados_limpos = dados_originais[dados_originais['Unnamed: 2'].notna()].reset_index(drop=True)
            
            # Mapeamento dos subsistemas conforme a tabela desejada
            mapeamento_subsistemas = {
                'Subsistema Nordeste': 'NO',
                'Subsistema Norte': 'N',
                'Subsistema Sudeste/C.Oeste': 'SCO',
                'Subsistema Sul': 'S',
                'Sistema Interligado Nacional': 'SIN',
                'Sistema Norte/Nordeste': 'NNO',
                'Sistema Sudeste/C.Oeste/Sul': 'SECOS'
            }
    
            # Criar DataFrame para estrutura final
            resultado_final = []
            
            # Processar dados mensais e semanais
            for _, row in dados_limpos.iterrows():
                subsistema_original = row['Unnamed: 2']
                if pd.isna(subsistema_original) or subsistema_original == '':
                    continue
                
                subsistema_codigo = mapeamento_subsistemas.get(subsistema_original)
                if not subsistema_codigo:
                    continue
                
                periodicidade_inicial = product_details.get('periodicidade')
                periodicidade_final = product_details.get('periodicidadeFinal')
                # Adicionar registro mensal (mantendo duas casas decimais)
                carga_mensal = round(float(row['Unnamed: 6']), 2)
                resultado_final.append({
                    'carga': carga_mensal,
                    'mes': mes_numero,  # Usando o número do mês em vez da abreviação
                    'revisao': revisao,
                    'subsistema': subsistema_codigo,
                    'semana': None,
                    'dt_inicio': data_inicio,
                    'tipo': 'mensal',
                    'periodicidade_inicial': periodicidade_inicial,
                    'periodicidade_final': periodicidade_final
                })
                
                # Adicionar registros para cada semana (de 1 a 6)
                # Mapeamento das colunas para cada semana
                for num_semana, coluna in colunas_semanas.items():
                    if coluna in row and not pd.isna(row[coluna]):
                        carga_semana = round(float(row[coluna]), 2)
                        # Usa a data específica da semana em vez de data_inicio genérica
                        data_semana = datas_semanas.get(coluna, data_inicio)  # Fallback para data_inicio se não encontrar
                        resultado_final.append({
                            'carga': carga_semana,
                            'mes': mes_numero,  # Usando o número do mês em vez da abreviação
                            'revisao': revisao,
                            'subsistema': subsistema_codigo,
                            'semana': int(num_semana),  # Garantindo que o número da semana seja inteiro
                            'dt_inicio': data_semana,  # Usa a data específica da semana
                            'tipo': 'semanal',
                            'periodicidade_inicial': periodicidade_inicial,
                            'periodicidade_final': periodicidade_final
                        })
            
            # Converter para DataFrame para facilitar a manipulação
            df_resultado = pd.DataFrame(resultado_final)
            
            # Imprime o dataframe formatado
            print("=" * 50)
            print("Dados formatados conforme estrutura solicitada:")
            print(df_resultado.to_string(index=False))
            print("=" * 50)
            
            # Validação dos dados conforme requisitos do sistema receptor
            for item in resultado_final:
                # Verificar se mes está entre 1 e 12
                if not (1 <= item["mes"] <= 12):
                    raise ValueError(f"Mês inválido: {item['mes']}. Deve estar entre 1 e 12.")
                
                # Verificar se tipo é 'mensal' ou 'semanal'
                if item["tipo"] not in ["mensal", "semanal"]:
                    raise ValueError(f"Tipo inválido: {item['tipo']}. Deve ser 'mensal' ou 'semanal'.")
                
                # Validar formato da data
                try:
                    datetime.strptime(item["dt_inicio"], "%Y%m%d")
                except ValueError:
                    raise ValueError(f"Formato de data inválido: {item['dt_inicio']}. Use o formato YYYYMMDD.")
            
            # Informações de log
            print(f"Dados extraídos com sucesso do arquivo de carga")
            print(f"Total de registros: {len(resultado_final)}")
            print(f"Registros mensais: {len([r for r in resultado_final if r['tipo'] == 'mensal'])}")
            print(f"Registros semanais: {len([r for r in resultado_final if r['tipo'] == 'semanal'])}")
            
            return resultado_final
            
        except Exception as e:
            error_msg = f"Erro ao processar arquivo de carga: {str(e)}"
            print(error_msg)
            return {
                'error': error_msg,
                'nome_arquivo': os.path.basename(carga_file_path) if carga_file_path else None
            }

    @staticmethod
    def enviar_whatsapp_sucesso(context):
        """Envia mensagem de sucesso via WhatsApp"""
        try:
            repository = SharedRepository()
            message = "✅ Carga Patamar Decomp processado com sucesso!"
            
            repository.send_whatsapp_message(message)
            print("Mensagem de sucesso enviada via WhatsApp")
            
        except Exception as e:
            print(f"Erro ao enviar WhatsApp de sucesso: {str(e)}")

    @staticmethod
    def enviar_whatsapp_erro(context):
        """Envia mensagem de erro via WhatsApp"""
        try:
            repository = SharedRepository()
            # Obter detalhes do erro do context se disponível
            error_details = context.get('exception', 'Erro não especificado')
            message = f"❌ Erro no processamento do Carga Patamar Decomp: {error_details}"
            
            repository.send_whatsapp_message(message)
            print("Mensagem de erro enviada via WhatsApp")
            
        except Exception as e:
            print(f"Erro ao enviar WhatsApp de erro: {str(e)}")
    
    def enviar_notificacao_evento(self, event_type: str, status: str, details: Dict[str, Any] = None):
        """Envia notificação de evento para API de eventos"""
        try:
            event_data = {
                'event_type': event_type,
                'status': status,
                'timestamp': datetime.now().isoformat(),
                'service': 'carga_patamar_decomp',
                'details': details or {}
            }
            
            self.repository.send_event_notification(event_data)
            print(f"Evento {event_type} enviado com status {status}")
            
        except Exception as e:
            print(f"Erro ao enviar notificação de evento: {str(e)}")

    @staticmethod
    def enviar_dados_para_api(**kwargs):
        """
        Envia os dados processados para a API conforme especificações do sistema receptor
        
        Recebe os dados processados via XCom da task anterior e envia para a API
        """
        try:
            # Busca dados da task anterior via XCom
            task_instance = kwargs['task_instance']
            processo_result = task_instance.xcom_pull(task_ids='processar_arquivos')
            
            if not processo_result:
                raise Exception("Não foi possível obter dados do processamento anterior")
                
            # Verifica se processo_result é um dicionário (formato esperado)
            if isinstance(processo_result, dict):
                if not processo_result.get('success'):
                    raise Exception(f"Falha no processamento anterior: {processo_result.get('error', 'Erro desconhecido')}")
                
                # Obtém os dados de carga
                carga_data = processo_result.get('carga_data', [])
            else:
                # Se processo_result não for um dicionário, assume que já é a lista de dados
                carga_data = processo_result
            
            # Se carga_data for um dicionário com erro, trata o erro
            if isinstance(carga_data, dict) and 'error' in carga_data:
                raise Exception(f"Erro nos dados de carga: {carga_data.get('error', 'Erro desconhecido')}")
            
            # Verifica se os dados estão no formato esperado (lista de dicionários)
            if not isinstance(carga_data, list):
                raise Exception(f"Formato de dados inválido. Esperado: lista de dicionários, recebido: {type(carga_data)}")
            
            if not carga_data:
                raise Exception("Nenhum dado de carga disponível para envio")
            
            print(f"Enviando {len(carga_data)} registros para a API")
            
            # Inicializa o repositório
            repository = SharedRepository()
            
            
            
            try:
                validator = CargaPMOValidator()
                carga_data = validator.validate(carga_data)
                print("✅ Validação dos dados de carga concluída com sucesso")
            except ValidationError as e:
                raise Exception(f"Erro na validação dos dados: {str(e)}")
            
            # Obtém token de autenticação
            auth_headers = repository.get_auth_token()
            
            # Define a URL da API para envio dos dados conforme especificação correta
            api_url = os.getenv("URL_API_CARGA", "https://tradingenergiarz.com/api/v2/decks/carga-pmo")
            
            # Envia os dados para a API
            import requests
            
            print(f"Enviando dados para: {api_url}")
            
            # Adiciona o header 'accept: application/json' conforme o exemplo do curl
            headers = {
                **auth_headers, 
                'Content-Type': 'application/json',
                'accept': 'application/json'
            }
            
            print(f"Headers: {headers}")
            
            response = requests.post(
                api_url,
                json=carga_data,  # Envia a lista diretamente como JSON
                headers=headers
            )
            
            # Verifica o resultado da requisição
            if response.status_code not in [200, 201]:
                raise Exception(f"Erro ao enviar dados para API: {response.status_code} - {response.text}")
            
            response_data = response.json()
            print(f"Dados enviados com sucesso para API. Resposta: {response_data}")
            
            # Prepara dados para evento de notificação no formato correto
            evento = {
                'eventType': 'product_processed',  # Campo obrigatório
                'systemOrigin': 'airflow_carga_patamar_decomp',  # Campo obrigatório
                'sessionId': 'RDPMO - Revisao Semanal PMO',  # Campo obrigatório
                'payload': {  # Todos os dados específicos devem ir dentro de payload
                    'tipo': 'envio_carga_pmo',
                    'status': 'sucesso',
                    'api_response': response_data,
                    'total_registros': len(carga_data),
                    'detalhes': {
                        'data_inicio': carga_data[0]['dt_inicio'] if carga_data else None,
                        'revisao': carga_data[0]['revisao'] if carga_data else None,
                        'subsistemas': list(set([item['subsistema'] for item in carga_data])),
                        'registros_mensais': len([r for r in carga_data if r['tipo'] == 'mensal']),
                        'registros_semanais': len([r for r in carga_data if r['tipo'] == 'semanal'])
                    }
                },
                'metadata': {
                    'data_processamento': datetime.now().isoformat()
                }
            }
            
            # Envia notificação de evento - aqui precisamos LANÇAR a exceção se falhar
            try:
                resultado_evento = repository.send_event_notification(evento)
                if not resultado_evento:
                    raise Exception("Falha ao enviar notificação de evento")
            except Exception as e:
                # Importante: estamos lançando a exceção para marcar a tarefa como falha
                # mesmo que o envio para a API tenha funcionado
                raise Exception(f"Falha ao enviar notificação de evento: {str(e)}")
            
            return {
                'success': True,
                'api_response': response_data,
                'message': f'Dados enviados com sucesso para a API. Total: {len(carga_data)} registros.',
                'details': {
                    'api_url': api_url,
                    'status_code': response.status_code
                }
            }
            
        except Exception as e:
            error_msg = f"Erro ao enviar dados para API: {str(e)}"
            print(error_msg)
            
            # Envia notificação de erro com o formato correto
            try:
                repository = SharedRepository()
                repository.send_event_notification({
                    'eventType': 'carga_pmo_erro',  # Campo obrigatório
                    'systemOrigin': 'airflow_carga_patamar_decomp',  # Campo obrigatório
                    'payload': {  # Todos os dados específicos devem ir dentro de payload
                        'tipo': 'envio_carga_pmo',
                        'status': 'erro',
                        'mensagem': str(e)
                    },
                    'metadata': {
                        'data_processamento': datetime.now().isoformat()
                    }
                })
            except Exception as notify_error:
                print(f"Erro adicional ao enviar notificação de erro: {str(notify_error)}")
            
            # IMPORTANTE: Lançar a exceção novamente para que o Airflow marque a tarefa como falha
            raise
