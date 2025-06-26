import requests
import os
import sys
import pandas as pd 
from inewave.newave import Cadic, Sistema
from datetime import datetime, date

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)
from utils.repository_webhook import SharedRepository
from utils.html_builder import HtmlBuilder
from validator_deck_preliminar_newave import DeckPreliminarNewaveValidator

class DeckPreliminarNewaveService:
    def __init__(self):
        self.repository = SharedRepository()
        self.validator = DeckPreliminarNewaveValidator()
        
    @staticmethod
    def determinar_task_inicial(**kwargs):
        """
        Determina qual task deve ser executada baseada na origem da configuração.
        Retorna 'validar_dados_entrada' se origem não existir ou for 'ons',
        caso contrário retorna 'gerar_tabela_diferenca_cargas'.
        """
        dag_run = kwargs.get('dag_run')
        if not dag_run or not hasattr(dag_run, 'conf') or not dag_run.conf:
            print("Sem configuração DAG, executando fluxo completo")
            return 'validar_dados_entrada'
        
        origem = dag_run.conf.get('origem')
        
        if origem is None or origem == 'ons':
            print(f"Origem: {origem}, executando fluxo completo")
            return 'validar_dados_entrada'
        else:
            print(f"Origem: {origem}, executando apenas geração de tabela")
            return 'gerar_tabela_diferenca_cargas'

    @staticmethod
    def validar_dados_entrada(**kwargs):
        validator = DeckPreliminarNewaveValidator()
        params = kwargs.get('params', {})
        return validator.validate(params)
    
    @staticmethod
    def download_arquivos(**kwargs):
        repository = SharedRepository()
        params = kwargs.get('params', {})
        print("Parâmetros recebidos para download:", params)
        
        
        try:
            product_details = params.get('product_details', {})
            webhook_id = product_details.get('webhookId')
            filename = product_details.get('filename')
            product_date = product_details.get('dataProduto')
            
            if not webhook_id or not filename or not product_date:
                raise ValueError("webhookId, filename e product_date são obrigatórios")
            
            if product_date:
                from datetime import datetime
                month, year = product_date.split('/')
                product_datetime = datetime(int(year), int(month), 1, 0, 0, 0)
            
            download_path = "/tmp/deck_preliminar_newave"
            
            file_path = repository.download_webhook_file(
                webhook_id=webhook_id,
                filename=filename,
                download_path=download_path
            )
            
            if not repository.validate_file_exists(file_path):
                raise Exception(f"Arquivo {filename} não foi baixado corretamente")
            
            print(f"Arquivo baixado com sucesso: {file_path}")
            
            xcom_data = {
                'file_path': file_path,
                'success': True,
                'product_datetime': product_datetime.strftime('%Y-%m-%d %H:%M:%S') if product_datetime else None,
                'message': f'Arquivo {filename} baixado com sucesso'
            }
            
            print(f"task(download_arquivos) - Retornando dados para XCom: {xcom_data}")
            
            return xcom_data
            
        except Exception as e:
            error_msg = f"Erro ao baixar arquivos: {str(e)}"
            print(error_msg)
            # Raise the exception instead of returning an error dictionary
            raise Exception(error_msg)
    
    @staticmethod
    def extrair_arquivos(**kwargs):
        try:
            task_instance = kwargs['task_instance']
            download_result = task_instance.xcom_pull(task_ids='download_arquivos')
            
            print(f"Dados recebidos via XCom: {download_result}")
            
            if not download_result or not download_result.get('success'):
                raise Exception(f"Falha no download anterior: {download_result.get('error', 'Erro desconhecido')}")
            
            file_path = download_result.get('file_path')
            if not file_path:
                raise Exception("Caminho do arquivo não encontrado nos dados XCom")
            
            print(f"Processando arquivo: {file_path}")
            
            if not os.path.exists(file_path):
                raise Exception(f"Arquivo não encontrado: {file_path}")
            
            extracted_files = []
            nested_extracted_files = []
            
            if file_path.endswith('.zip'):
                import zipfile
                
                extract_path = os.path.dirname(file_path)
                
                with zipfile.ZipFile(file_path, 'r') as zip_ref:
                    zip_ref.extractall(extract_path)
                    extracted_files = zip_ref.namelist()
                
                print(f"Arquivos extraídos: {extracted_files}")
                
                for extracted_file in extracted_files:
                    nested_zip_path = os.path.join(extract_path, extracted_file)
                    
                    if os.path.exists(nested_zip_path) and nested_zip_path.endswith('.zip'):
                        print(f"Encontrado ZIP aninhado: {nested_zip_path}")
                        
                        try:
                            with zipfile.ZipFile(nested_zip_path, 'r') as nested_zip:
                                nested_zip.extractall(extract_path)
                                nested_extracted_files = nested_zip.namelist()
                                print(f"Arquivos extraídos do ZIP aninhado: {nested_extracted_files}")
                        except Exception as zip_error:
                            print(f"Erro ao extrair ZIP aninhado: {str(zip_error)}")
            
            xcom_data = {
                    'success': True,
                    'original_file': file_path,
                    'extract_path': extract_path,
                    'extracted_files': nested_extracted_files,
                    'message': f'Arquivos extraídos com sucesso de {os.path.basename(file_path)}'
                }
        
            print(f"task(extrair_arquivos) - Retornando dados para XCom: {xcom_data}")
            
            return xcom_data
        
        except Exception as e:
            error_msg = f"Erro ao extrair arquivos: {str(e)}"
            print(error_msg)
            raise Exception(error_msg)
    
    @staticmethod
    def processar_deck_nw_cadic(**kwargs):
        try:
            task_instance = kwargs['task_instance']
            extract_result = task_instance.xcom_pull(task_ids='extrair_arquivos')
            download_result = task_instance.xcom_pull(task_ids='download_arquivos')
            
            print(f"Dados de extração recebidos via XCom: {extract_result}")
            
            if not extract_result or not extract_result.get('success'):
                raise Exception(f"Falha na extração anterior: {extract_result.get('error', 'Erro desconhecido')}")
            
            cadic_file = os.path.join(extract_result['extract_path'], 'C_ADIC.DAT')
            if not os.path.exists(cadic_file):
                raise FileNotFoundError(f"Arquivo C_ADIC.DAT não encontrado em {extract_result['extract_path']}")
            
            print(f"Processando arquivo C_ADIC.DAT: {cadic_file}")
            
            cadic_object = Cadic.read(cadic_file)
            
            nw_cadic_df:pd.DataFrame = cadic_object.cargas.copy()
            
            # Correção temporária do erro da lib inewave
            nw_cadic_df['nome_submercado'] = nw_cadic_df['nome_submercado'].replace('ORDESTE', 'NORDESTE')
            
            if nw_cadic_df is None:
                print("Aviso: Não foram encontrados dados de cargas adicionais no arquivo")
            else:
                print(f"Dados de cargas adicionais encontrados: {nw_cadic_df} ")
                print(f"Cargas adicionais carregadas com sucesso. Total de registros: {len(nw_cadic_df)}")

            nw_cadic_df['data'] = pd.to_datetime(nw_cadic_df['data'], errors='coerce')
            
            nw_cadic_df = nw_cadic_df[nw_cadic_df['data'].dt.year < 9999]
            
            nw_cadic_df['vl_ano'] = nw_cadic_df['data'].dt.year.astype(int)
            nw_cadic_df['vl_mes'] = nw_cadic_df['data'].dt.month.astype(int)
            
            nw_cadic_df = nw_cadic_df.dropna(subset=['valor'])
            
            mapeamento_razao = {
                'CONS.ITAIPU': 'vl_const_itaipu',
                'ANDE': 'vl_ande',
                'MMGD SE': 'vl_mmgd_se',
                'MMGD S': 'vl_mmgd_s',
                'MMGD NE': 'vl_mmgd_ne',
                'BOA VISTA': 'vl_boa_vista',
                'MMGD N': 'vl_mmgd_n'
            }
        
            nw_cadic_df['coluna'] = nw_cadic_df['razao'].map(mapeamento_razao)

            nw_cadic_df = nw_cadic_df.pivot_table(
                index=['vl_ano', 'vl_mes'], 
                columns='coluna',
                values='valor',
                aggfunc='first'  
            ).reset_index()
            
            product_datetime_str = download_result.get('product_datetime') if download_result else None
            dt_deck = datetime.strptime(product_datetime_str, '%Y-%m-%d %H:%M:%S')
                    
            nw_cadic_df['dt_deck'] = dt_deck
            nw_cadic_df['dt_deck'] = nw_cadic_df['dt_deck'].dt.strftime('%Y-%m-%d')  # Formata como string
            
            nw_cadic_df['fonte'] = 'ONS'


            nw_cadic_records = nw_cadic_df.to_dict('records')
            
            xcom_data = {
                'success': True,
                'cadic_path': cadic_file,
                'nw_cadic_records': nw_cadic_records,
                'product_datetime': product_datetime_str,
                'message': 'Arquivo C_ADIC.DAT processado com sucesso'
            }
            
            print(f"Retornando dados para XCom: {xcom_data}")
            
            return xcom_data
            
        except Exception as e:
            error_msg = f"Erro ao processar deck NW CADIC: {str(e)}"
            print(error_msg)
            raise Exception(error_msg)
    
    @staticmethod
    def processar_deck_nw_sist(**kwargs):
        try:
            task_instance = kwargs['task_instance']
            extract_result = task_instance.xcom_pull(task_ids='extrair_arquivos')
            download_result = task_instance.xcom_pull(task_ids='download_arquivos')
            
            print(f"Dados de extração recebidos via XCom: {extract_result}")
            
            if not extract_result or not extract_result.get('success'):
                raise Exception(f"Falha na extração anterior: {extract_result.get('error', 'Erro desconhecido')}")
            
            sistema_file = os.path.join(extract_result['extract_path'], 'SISTEMA.DAT')
            
            sistema_object = Sistema.read(sistema_file)
            
            # Manipulando dataframe de valores de mercado de energia total
            sistema_mercado_energia_df = sistema_object.mercado_energia.copy()   
            
            if sistema_mercado_energia_df is None:
                print("Aviso: Não foram encontrados dados de sistema do mercado de energia no arquivo")
            else:
                print(f"Dados de sistema do mercado de energia encontrados: {sistema_mercado_energia_df} ")
                print(f"Mercado de energia total carregado com sucesso. Total de registros: {len(sistema_mercado_energia_df)}")
            
            sistema_mercado_energia_df['data'] = pd.to_datetime(sistema_mercado_energia_df['data'], errors='coerce')

            sistema_mercado_energia_df = sistema_mercado_energia_df.dropna(subset=['data'])
            
            sistema_mercado_energia_df['vl_ano'] = sistema_mercado_energia_df['data'].dt.year.astype(int)
            sistema_mercado_energia_df['vl_mes'] = sistema_mercado_energia_df['data'].dt.month.astype(int)
            
            sistema_mercado_energia_df = sistema_mercado_energia_df.rename(columns={
                'codigo_submercado': 'cd_submercado',
                'valor': 'vl_energia_total'
            })
            
            # Manipulando dataframe de valores de geração de usinas não simuladas
            sistema_geracao_unsi_df = sistema_object.geracao_usinas_nao_simuladas
            
            sistema_geracao_unsi_df['tipo_geracao'] = sistema_geracao_unsi_df['indice_bloco'].map({
                1: 'vl_geracao_pch',
                2: 'vl_geracao_pct',
                3: 'vl_geracao_eol',
                4: 'vl_geracao_ufv',
                5: 'vl_geracao_pch_mmgd',
                6: 'vl_geracao_pct_mmgd',
                7: 'vl_geracao_eol_mmgd',
                8: 'vl_geracao_ufv_mmgd'
            })
            
            sistema_geracao_unsi_df['vl_ano'] = sistema_geracao_unsi_df['data'].dt.year
            sistema_geracao_unsi_df['vl_mes'] = sistema_geracao_unsi_df['data'].dt.month
            
            sistema_geracao_unsi_df = sistema_geracao_unsi_df.pivot_table(
                index=['codigo_submercado', 'vl_ano', 'vl_mes'], 
                columns='tipo_geracao',
                values='valor',
                aggfunc='sum'  
            ).reset_index()
            
            sistema_geracao_unsi_df = sistema_geracao_unsi_df.rename(columns={'codigo_submercado': 'cd_submercado'})
            
            # Fazendo merge entre os dataframes de geração e mercado de energia
            nw_sistema_df = pd.merge(
                sistema_geracao_unsi_df, 
                sistema_mercado_energia_df,
                on=['cd_submercado', 'vl_ano', 'vl_mes'],
                how='left'
            )
            
            product_datetime_str = download_result.get('product_datetime') if download_result else None
            dt_deck = datetime.strptime(product_datetime_str, '%Y-%m-%d %H:%M:%S')
                
            nw_sistema_df['dt_deck'] = dt_deck
            nw_sistema_df['dt_deck'] = nw_sistema_df['dt_deck'].dt.date 
            
            nw_sistema_df = nw_sistema_df[~((nw_sistema_df['vl_geracao_pch'].fillna(0) == 0) &
                     (nw_sistema_df['vl_geracao_pct'].fillna(0) == 0) &
                     (nw_sistema_df['vl_geracao_eol'].fillna(0) == 0) &
                     (nw_sistema_df['vl_geracao_ufv'].fillna(0) == 0) &
                     (nw_sistema_df['vl_geracao_pch_mmgd'].fillna(0) == 0) &
                     (nw_sistema_df['vl_geracao_pct_mmgd'].fillna(0) == 0) &
                     (nw_sistema_df['vl_geracao_eol_mmgd'].fillna(0) == 0) &
                     (nw_sistema_df['vl_geracao_ufv_mmgd'].fillna(0) == 0))
            ]
            
            nw_sistema_df['fonte'] = 'ONS'
            
            ordem_colunas = [
                'cd_submercado',
                'vl_ano',
                'vl_mes',
                'vl_energia_total',
                'vl_geracao_pch',
                'vl_geracao_pct',
                'vl_geracao_eol',
                'vl_geracao_ufv',
                'vl_geracao_pch_mmgd',
                'vl_geracao_pct_mmgd',
                'vl_geracao_eol_mmgd',
                'vl_geracao_ufv_mmgd',
                'dt_deck',
                'fonte'
            ]
            
            nw_sistema_df = nw_sistema_df.reindex(columns=ordem_colunas)
            
            nw_sistema_records = nw_sistema_df.to_dict('records')
            
            xcom_data = {
                'success': True,
                'sistema_file': sistema_file,
                'nw_sistema_records': nw_sistema_records,
                'product_datetime': product_datetime_str,
                'message': 'Arquivo SISTEMA.DAT processado com sucesso',
            }
            
            print(f"task(processar_deck_nw_sist) - Retornando dados para XCom: {xcom_data}")
            
            return xcom_data
            
        except Exception as e:
            error_msg = f"Erro ao processar deck NW SISTEMA: {str(e)}"
            print(error_msg)
            raise Exception(error_msg)
    
    @staticmethod
    def enviar_dados_para_api(**kwargs):
        try:
            task_instance = kwargs['task_instance']
            nw_sist_result = task_instance.xcom_pull(task_ids='processar_deck_nw_sist')
            nw_cadic_result = task_instance.xcom_pull(task_ids='processar_deck_nw_cadic')

            if not nw_sist_result or not nw_sist_result.get('success') or not nw_cadic_result or not nw_cadic_result.get('success'):
                raise Exception("Dados necessários não encontrados ou inválidos nos XComs")

            nw_sist_records = nw_sist_result.get('nw_sistema_records', [])
            nw_cadic_records = nw_cadic_result.get('nw_cadic_records', [])
            product_datetime_str = nw_sist_result.get('product_datetime') or nw_cadic_result.get('product_datetime')

            print(f"Preparando dados para envio à API: {len(nw_sist_records)} registros de SISTEMA e {len(nw_cadic_records)} registros CADIC")

            repository = SharedRepository()

            auth_headers = repository.get_auth_token()
            headers = {
                **auth_headers, 
                'Content-Type': 'application/json',
                'accept': 'application/json'
            }

            print(f"Headers: {headers}")

            api_url = os.getenv("URL_API_V2", "http://host.docker.internal:8000/api/v2")

            for record in nw_sist_records:
                if isinstance(record.get('dt_deck'), date):
                    record['dt_deck'] = record['dt_deck'].isoformat()

            for record in nw_cadic_records:
                if isinstance(record.get('dt_deck'), str) and 'T' in record['dt_deck']:
                    record['dt_deck'] = record['dt_deck'].split('T')[0]  

            sistema_url = f"{api_url}/decks/newave/sistema"
            cadic_url = f"{api_url}/decks/newave/cadic"

            print(f"Enviando dados para: {sistema_url}")

            request_sistema = requests.post(
                sistema_url,
                headers=headers,
                json=nw_sist_records,  # Use json parameter to properly encode the data
            )

            request_cadic = requests.post(
                cadic_url,
                headers=headers,
                json=nw_cadic_records,  # Use json parameter to properly encode the data
            )

            if request_sistema.status_code != 200:
                raise Exception(f"Erro ao enviar carga do SISTEMA para API: {request_sistema.text}")
                
            if request_cadic.status_code != 200:
                raise Exception(f"Erro ao enviar carga do CADIC para API: {request_cadic.text}")

            xcom_data = {
                'success': True,
                'message': 'Dados enviados para a API com sucesso',
                'product_datetime': product_datetime_str  # Repassa a data do produto para as próximas tasks
            }

            print(f"task(enviar_dados_para_api) - Retornando dados para XCom: {xcom_data}")

            return xcom_data
        except Exception as e:
            error_msg = f"Erro ao enviar dados para API: {str(e)}"
            print(error_msg)
            raise Exception(error_msg)
    
    @staticmethod
    def gerar_tabela_diferenca_cargas(**kwargs):
        import pdb
        
        try:
            product_datetime_str = kwargs.get('dag_run').conf.get('product_details').get('dataProduto')

            api_url = os.getenv("URL_API_V2", "http://host.docker.internal:8000/api/v2")
            image_api_url = "https://tradingenergiarz.com/html-to-img"
            
            repository = SharedRepository()
            html_builder = HtmlBuilder()

            auth_headers = repository.get_auth_token()
            headers = {
                **auth_headers, 
                'Content-Type': 'application/json',
                'accept': 'application/json'
            }
            
            
            # Pegando valores do sistema de geração de usinas não simuladas (UNSI)
            sistema_unsi_url = f"{api_url}/decks/newave/sistema/unsi"
            sistema_unsi_response = requests.get(
                sistema_unsi_url,
                headers=headers
            )
            if sistema_unsi_response.status_code != 200:
                raise Exception(f"Erro ao obter dados de geração UNSI: {sistema_unsi_response.text}")
            
            sistema_unsi_values = sistema_unsi_response.json() 
            
            
            # Pegando valores de MMGD Total 
            sistema_mmgd_total_url = f"{api_url}/decks/newave/mmgd_total"
            sistema_mmgd_total_response = requests.get(
                sistema_mmgd_total_url,
                headers=headers
            )
            if sistema_mmgd_total_response.status_code != 200:
                raise Exception(f"Erro ao obter dados de MMGD Total: {sistema_mmgd_total_response.text}")
            
            sistema_mmgd_total_values = sistema_mmgd_total_response.json()
            
            
            # Pegando valores de geração de Carga Global
            carga_global_url = f"{api_url}/decks/newave/sistema/cargas/carga_global"
            carga_global_response = requests.get(
                carga_global_url,
                headers=headers
            )
            if carga_global_response.status_code != 200:
                raise Exception(f"Erro ao obter dados de geração de carga global: {carga_global_response.text}")
            carga_global_values = carga_global_response.json()
            
            
            # Pegando valores de geração de Carga Líquida
            carga_liquida_url = f"{api_url}/decks/newave/sistema/cargas/carga_liquida"
            carga_liquida_response = requests.get(
                carga_liquida_url,
                headers=headers
            )
            if carga_liquida_response.status_code != 200:
                raise Exception(f"Erro ao obter dados de geração de carga liquida: {carga_liquida_response.text}")
            carga_liquida_values = carga_liquida_response.json()
            
            
            # Gerar o HTML usando o método gerar_html
            html_tabela_diferenca = html_builder.gerar_html(
                'diferenca_cargas', 
                None, 
                dados_unsi=sistema_unsi_values,
                dados_mmgd_total=sistema_mmgd_total_values,
                dados_carga_global=carga_global_values,
                dados_carga_liquida=carga_liquida_values
            )
            
            print(html_tabela_diferenca)
            
            api_html_payload = {
                "html": html_tabela_diferenca,
                "options": {
                  "type": "png",
                  "quality": 100,
                  "trim": True,
                  "deviceScaleFactor": 2
                }
            }
            
            html_api_endpoint = f"{image_api_url}/convert"
            
            request_html_api = requests.post(
                html_api_endpoint,
                headers=headers,
                json=api_html_payload,  
            )
            
            if request_html_api.status_code != 200:
                raise Exception(f"Erro ao converter HTML em imagem: {request_html_api.text}")
            
            # Salvar a imagem retornada pela API
            image_dir = "/tmp/deck_preliminar_newave/images"
            os.makedirs(image_dir, exist_ok=True)
            
            # Gerar nome do arquivo baseado na data do produto
            if product_datetime_str:
                # dt = datetime.strptime(product_datetime_str, '%Y-%m-%d %H:%M:%S')
                image_filename = f"tabela_diferenca_cargas_{product_datetime_str}.png"
            else:
                image_filename = f"tabela_diferenca_cargas_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
            
            image_path = os.path.join(image_dir, image_filename)
            
            with open(image_path, 'wb') as f:
                f.write(request_html_api.content)
            
            print(f"Imagem salva em: {image_path}")
            
            xcom_data = {
                'success': True,
                'image_path': image_path,
                'image_filename': image_filename,
                'message': 'Tabela de diferença de carga gerada com sucesso',
                'product_datetime': product_datetime_str  
            }
            
            print(f"task(gerar_tabela_diferenca_cargas) - Retornando dados para XCom: {xcom_data}")
            
            return xcom_data
        except Exception as e:
            error_msg = f"Erro ao gerar tabela de diferença de cargas: {str(e)}"
            print(error_msg)
            raise Exception(error_msg)
    
    @staticmethod
    def enviar_tabela_whatsapp_email(**kwargs):
        try:
            task_instance = kwargs['task_instance']
            tabela_result = task_instance.xcom_pull(task_ids='gerar_tabela_diferenca_cargas')
            params = kwargs.get('params', {})
            
            if not tabela_result or not tabela_result.get('success'):
                raise Exception("Tabela de diferença não foi gerada com sucesso")
            
            image_path = tabela_result.get('image_path')
            image_filename = tabela_result.get('image_filename')
            product_datetime_str = kwargs.get('dag_run').conf.get('product_details').get('dataProduto')
            
            if not image_path or not os.path.exists(image_path):
                raise Exception(f"Arquivo de imagem não encontrado: {image_path}")
            
            print(f"Preparando envio da imagem: {image_path}")
            
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            sys.path.insert(0, project_root)
            from utils.whatsapp_sender import WhatsAppSender
            
            whatsapp_sender = WhatsAppSender()
            
            try:
                success = whatsapp_sender.send_table_notification(
                    table_type="Diferença de Cargas NEWAVE",
                    product_datetime=product_datetime_str or "Data não informada",
                    image_path=image_path
                )
                
                if success:
                    print(f"Tabela enviada com sucesso via WhatsApp para Debug")
                else:
                    print("Falha no envio via WhatsApp")
                    
            except Exception as whatsapp_error:
                print(f"Erro no envio WhatsApp: {str(whatsapp_error)}")
              
            xcom_data = {
                'success': True,
                'message': f'Imagem {image_filename} enviada com sucesso',
                'image_sent': True,
                'product_datetime': product_datetime_str,
                'whatsapp_sent': success if 'success' in locals() else False
            }
            
            print(f"task(enviar_tabela_whatsapp_email) - Retornando dados para XCom: {xcom_data}")
            
            return xcom_data
            
        except Exception as e:
            error_msg = f"Erro ao enviar tabela por WhatsApp ou email: {str(e)}"
            print(error_msg)
            raise Exception(error_msg)
    
if __name__ == "__main__":
    service = DeckPreliminarNewaveService()
    class MockTaskInstance:
        def xcom_pull(self, task_ids, key=None):
            return {
                'success': True,
                'message': 'Dados enviados para a API com sucesso'
            }
    
    try:
        params = {
            "function_name": "WEBHOOK",
            "product_details": {
                "dataProduto": "06/2025",
                "enviar": True,
                "filename": "Deck NEWAVE Preliminar.zip",
                "macroProcesso": "Programação da Operação",
                "nome": "Deck NEWAVE Preliminar",
                "periodicidade": "2025-06-01T03:00:00.000Z",
                "periodicidadeFinal": "2025-07-01T02:59:59.000Z",
                "processo": "Médio Prazo",
                "s3Key": "webhooks/Deck NEWAVE Preliminar/68347a7abd270c7eb3fac7cb_Deck NEWAVE Preliminar.zip",
                "url": "https://apps08.ons.org.br/ONS.Sintegre.Proxy/webhook?token=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJVUkwiOiIvc2l0ZXMvOS81Mi83MS9Qcm9kdXRvcy8yODcvMjYtMDUtMjAyNV8xMTI2MDAiLCJ1c2VybmFtZSI6ImdpbHNldS5tdWhsZW5AcmFpemVuLmNvbSIsIm5vbWVQcm9kdXRvIjoiRGVjayBORVdBVkUgUHJlbGltaW5hciIsIklzRmlsZSI6IkZhbHNlIiwiaXNzIjoiaHR0cDovL2xvY2FsLm9ucy5vcmcuYnIiLCJhdWQiOiJodHRwOi8vbG9jYWwub25zLm9yZy5iciIsImV4cCI6MTc0ODM1NjMyOSwibmJmIjoxNzQ4MjY5Njg5fQ.EWzTvWRcHywDyfTpVxGbRZ4phTok-Kw9uVypsPN5sXI",
                "webhookId": "68347a7abd270c7eb3fac7cb"
            }
        }       
        result = DeckPreliminarNewaveService.gerar_tabela_diferenca_cargas(params=params, task_instance=MockTaskInstance())
    except Exception as e:
        print(f"Erro ao extrair arquivos: {str(e)}")


