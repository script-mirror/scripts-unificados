"""
Tasks específicas para processamento de Previsões de Carga Mensal por Patamar NEWAVE.
"""

import os
import sys
import datetime
import pandas as pd
import requests
import pdb
from typing import Dict, Any

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)
from utils.repository_webhook import SharedRepository
from validator import PrevisoesCargaMensalNewaveValidator

from inewave.newave import Cadic


class PrevisoesCargaMensalNewaveService:
    """Service para processamento de Previsões de Carga Mensal por Patamar NEWAVE"""
    
    def __init__(self):
        self.repository = SharedRepository()
        self.validator = PrevisoesCargaMensalNewaveValidator()
        
    @staticmethod
    def validar_dados_entrada(**kwargs) -> Dict[str, Any]:
        """Valida os dados de entrada do webhook"""
        validator = PrevisoesCargaMensalNewaveValidator()
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
            
            product_datetime = None
            if product_date:
                if '/' in product_date:
                    month, year = product_date.split('/')
                    product_datetime = datetime.datetime(int(year), int(month), 1, 0, 0, 0)
                else:
                    try:
                        product_datetime = datetime.datetime.strptime(product_date, '%Y-%m-%d')
                    except ValueError:
                        product_datetime = datetime.datetime.now()
            
            download_path = "/tmp/previsoes_carga_mensal_newave"
            
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
            
            if file_path.endswith('.zip'):
                import zipfile
                
                extract_path = os.path.dirname(file_path)
                
                # Extrair o ZIP principal primeiro
                print(f"Extraindo ZIP principal: {file_path}")
                with zipfile.ZipFile(file_path, 'r') as zip_ref:
                    zip_ref.extractall(extract_path)
                    extracted_files = zip_ref.namelist()
                
                print(f"Arquivos extraídos do ZIP principal: {extracted_files}")
                
                # Buscando pelo arquivo .xlsx
                print("Buscando arquivos .xlsx nos arquivos extraidos...")
                final_extracted_file = []
                for extracted_file in extracted_files:
                    if extracted_file.endswith('.xlsx'):
                        final_extracted_file.append(os.path.join(extract_path, extracted_file))
                        print(f"Arquivo .xlsx encontrado: {extracted_file}")
                if not final_extracted_file:
                    raise Exception("Nenhum arquivo .xlsx encontrado após a extração do ZIP")
                
            else:
                print(f"Arquivo não é ZIP: {file_path}")
                final_extracted_file = []
                            
            xcom_data = {
                'success': True,
                'original_file': file_path,
                'extract_path': extract_path,
                'extracted_file': final_extracted_file,
                'product_datetime': download_result.get('product_datetime'),
                'message': f'Arquivos extraídos com sucesso de {os.path.basename(file_path)}'
            }

            print(f"task(extrair_arquivos) - Retornando dados para XCom: {xcom_data}")
            
            return xcom_data
        
        except Exception as e:
            error_msg = f"Erro ao extrair arquivos: {str(e)}"
            print(error_msg)
            raise Exception(error_msg)
    
    @staticmethod
    def processar_carga_mensal(**kwargs):
        try:
            
            task_instance = kwargs['task_instance']
            extract_result = task_instance.xcom_pull(task_ids='extrair_arquivos')
            download_result = task_instance.xcom_pull(task_ids='download_arquivos')
            
            print(f"Dados de extração recebidos via XCom: {extract_result}")
            
            if not extract_result or not extract_result.get('success'):
                raise Exception(f"Falha na extração anterior: {extract_result.get('error', 'Erro desconhecido')}")
            
            cargas_mensais_xlsx = extract_result.get('extracted_file', [])
           
            xlsx_path = cargas_mensais_xlsx[0]
            
            print(f"Processando arquivo xlsx: {xlsx_path}")
            
            try:
                df_carga = pd.read_excel(xlsx_path)
                
                # Verifica se o DataFrame tem dados
                if df_carga.empty:
                    raise Exception("O arquivo xlsx não contém dados")
                
                df_carga.drop(columns=['WEEK','GAUGE','LOAD_cMMGD', 'Base_CGH', 'Base_EOL', 'Base_UFV', 'Base_UTE','Exp_MMGD','REVISION'], inplace=True, errors='ignore')
                df_carga = df_carga[df_carga['TYPE'] == 'MEDIUM']
                df_carga.drop(columns=['TYPE'], inplace=True, errors='ignore')
                
                df_carga['vl_ano'] = df_carga['DATE'].dt.year
                df_carga['vl_mes'] = df_carga['DATE'].dt.month
                df_carga.drop(columns=['DATE'], inplace=True, errors='ignore')
                
                df_atualizacao_sist = df_carga[['vl_ano', 'vl_mes','SOURCE', 'LOAD_sMMGD', 'Exp_CGH', 'Exp_EOL', 'Exp_UFV', 'Exp_UTE']]
                df_atualizacao_cadic = df_carga[['vl_ano', 'vl_mes','SOURCE', 'Base_MMGD']]
                
                df_atualizacao_sist.rename(columns={
                    'SOURCE': 'cd_submercado',
                    'LOAD_sMMGD': 'vl_energia_total',
                    'Exp_CGH': 'vl_geracao_pch_mmgd',
                    'Exp_EOL': 'vl_geracao_eol_mmgd',
                    'Exp_UFV': 'vl_geracao_ufv_mmgd',
                    'Exp_UTE': 'vl_geracao_pct_mmgd'
                }, inplace=True)
                
                cd_submercados = {
                    'SUDESTE': '1',
                    'SUL': '2',
                    'NORDESTE': '3',
                    'NORTE': '4'
                }
                
                df_atualizacao_sist['cd_submercado'] = df_atualizacao_sist['cd_submercado'].map(cd_submercados)
                
                df_atualizacao_sist['vl_energia_total'] = df_atualizacao_sist['vl_energia_total'].astype(int)
                
                for col in ['vl_geracao_pch_mmgd', 'vl_geracao_eol_mmgd', 'vl_geracao_ufv_mmgd', 'vl_geracao_pct_mmgd']:
                    if col in df_atualizacao_sist.columns:
                        df_atualizacao_sist[col] = df_atualizacao_sist[col].astype(float).round(2)
                
                df_atualizacao_cadic.rename(columns={
                    'SOURCE': 'cd_submercado',
                    'Base_MMGD': 'vl_mmgd'
                }, inplace=True)
                
                cd_submercados_cadic = {
                    'SUDESTE': 'se',
                    'SUL': 's',
                    'NORDESTE': 'ne',
                    'NORTE': 'n'
                }
                
                df_atualizacao_cadic.loc[:, 'cd_submercado'] = df_atualizacao_cadic['cd_submercado'].map(cd_submercados_cadic)
                
                df_atualizacao_cadic = df_atualizacao_cadic.pivot_table(
                    index=['vl_ano', 'vl_mes'],
                    columns='cd_submercado',
                    values='vl_mmgd',
                    aggfunc='first'
                ).reset_index()
                
                df_atualizacao_cadic.columns.name = None
                
                df_atualizacao_cadic = df_atualizacao_cadic.rename(columns={
                    'se': 'vl_mmgd_se',
                    's': 'vl_mmgd_s',
                    'ne': 'vl_mmgd_ne',
                    'n': 'vl_mmgd_n'
                })
                
                for col in ['vl_mmgd_se', 'vl_mmgd_s', 'vl_mmgd_ne', 'vl_mmgd_n']:
                    if col in df_atualizacao_cadic.columns:
                        df_atualizacao_cadic[col] = df_atualizacao_cadic[col].astype(float).round(0)
                        df_atualizacao_cadic[col] = df_atualizacao_cadic[col].fillna(0).astype(int)
                        
                df_atualizacao_cadic['versao'] = 'preliminar'
                df_atualizacao_sist['versao'] = 'preliminar'
                
                product_datetime_str = download_result.get('product_datetime') or extract_result.get('product_datetime')
                if product_datetime_str:
                    dt_deck = datetime.datetime.strptime(product_datetime_str, '%Y-%m-%d %H:%M:%S')
                    df_atualizacao_sist['dt_deck'] = dt_deck.strftime('%Y-%m-%d')
                    df_atualizacao_cadic['dt_deck'] = dt_deck.strftime('%Y-%m-%d')
                    
            except Exception as e:
                raise Exception(f"Erro ao ler arquivo xlsx: {str(e)}")
            
            print(f"Processamento concluído:")
            
            xcom_data = {
                'success': True,
                'atualizacao_cadic_records': df_atualizacao_cadic.to_dict(orient='records'),
                'atualizacao_sist_records': df_atualizacao_sist.to_dict(orient='records'),
                'original_file': xlsx_path,
                'product_datetime': product_datetime_str,
                'message': 'Cargas do NEWAVE processadas com sucesso'
            }
            
            print(f"Avançando para envio das cargas à API Middle")
            
            return xcom_data
            
        except Exception as e:
            error_msg = f"Erro ao processar carga mensal: {str(e)}"
            print(error_msg)
            raise Exception(error_msg)
        
    
    @staticmethod
    def atualizar_sist_cadic_com_cargas(**kwargs):
        """Envia as cargas processadas que atualizarão os valores do Sistema e Cadic para a API"""
        try:
            repository = SharedRepository()
            
            task_instance = kwargs['task_instance']
            process_result = task_instance.xcom_pull(task_ids='processar_carga_mensal')
            atualizacao_cadic_records = process_result.get('atualizacao_cadic_records', [])
            atualizacao_sist_records = process_result.get('atualizacao_sist_records', [])
            
            auth_headers = repository.get_auth_token()
            headers = {
                **auth_headers, 
                'Content-Type': 'application/json',
                'accept': 'application/json'
            }

            print(f"Headers: {headers}")

            api_url = os.getenv("DNS", "http://localhost:8000")
            api_url += "/api/v2"
            
            update_sistema_url = f"{api_url}/decks/newave/sistema/total_mmgd_expansao"
            
            request_sist = requests.put(
                update_sistema_url,
                json=atualizacao_sist_records,
                headers=headers
            )
            
            update_cadic_url = f"{api_url}/decks/newave/cadic/total_mmgd_base"
            
            request_cadic = requests.put(
                update_cadic_url,
                json=atualizacao_cadic_records,
                headers=headers
            )
            
            if request_sist.status_code not in [200, 201]:
                raise Exception(f"Erro ao atualizar sistema: {request_sist.status_code} - {request_sist.text}")
            
            if request_cadic.status_code not in [200, 201]:
                raise Exception(f"Erro ao atualizar Cadic: {request_cadic.status_code} - {request_cadic.text}")
            
            print("Atualizações enviadas com sucesso para o Sistema e Cadic")
            
            xcom_data = {
                'success': True,
                'message': 'Atualizações enviadas com sucesso para o Sistema e Cadic',
                'atualizacao_sist_response': request_sist.json(),
                'atualizacao_cadic_response': request_cadic.json()
            }   
            
            return xcom_data
            
        except Exception as e:
            error_msg = f"Erro ao enviar dados para API: {str(e)}"
            print(error_msg)
            raise Exception(error_msg)


if __name__ == "__main__":
    # Teste local
    service = PrevisoesCargaMensalNewaveService()
    
    class MockTaskInstance:
        def xcom_pull(self, task_ids, key=None):
            return {'success': True, 'original_file': '/tmp/previsoes_carga_mensal_newave/RV0_PMO_Agosto_2025_carga_mensal.zip', 'extract_path': '/tmp/previsoes_carga_mensal_newave', 'extracted_file': ['/tmp/previsoes_carga_mensal_newave/CargaMensal_PMO-Agosto2025.xlsx'], 'product_datetime': '2025-08-01 00:00:00', 'message': 'Arquivos extraídos com sucesso de RV0_PMO_Agosto_2025_carga_mensal.zip'}
        
    try:
        params = {
            "file_path": "/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/webhook/arquivos/tmp/Previsões de carga mensal e por patamar - NEWAVE/RV0_PMO_Agosto_2025_carga_mensal.zip",
            "function_name": "WEBHOOK",
            "product_details": {
                "dataProduto": "08/2025",
                "enviar": True,
                "filename": "RV0_PMO_Agosto_2025_carga_mensal.zip",
                "macroProcesso": "Programação da Operação",
                "nome": "Previsões de carga mensal e por patamar - NEWAVE",
                "periodicidade": "2025-08-01T03:00:00.000Z",
                "periodicidadeFinal": "2025-09-01T02:59:59.000Z",
                "processo": "Previsão de Carga para o PMO",
                "s3Key": "webhooks/Previsões de carga mensal e por patamar - NEWAVE/687f7056d49e380e81e2a922_RV0_PMO_Agosto_2025_carga_mensal.zip",
                "url": "https://apps08.ons.org.br/ONS.Sintegre.Proxy/webhook?token=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJVUkwiOiJodHRwczovL3NpbnRlZ3JlLm9ucy5vcmcuYnIvc2l0ZXMvOS80Ny9Qcm9kdXRvcy8yMjkvUlYwX1BNT19BZ29zdG9fMjAyNV9jYXJnYV9tZW5zYWwuemlwIiwidXNlcm5hbWUiOiJnaWxzZXUubXVobGVuQHJhaXplbi5jb20iLCJub21lUHJvZHV0byI6IlByZXZpc8O1ZXMgZGUgY2FyZ2EgbWVuc2FsIGUgcG9yIHBhdGFtYXIgLSBORVdBVkUiLCJJc0ZpbGUiOiJUcnVlIiwiaXNzIjoiaHR0cDovL2xvY2FsLm9ucy5vcmcuYnIiLCJhdWQiOiJodHRwOi8vbG9jYWwub25zLm9yZy5iciIsImV4cCI6MTc1MzI2ODkzMywibmJmIjoxNzUzMTgyMjkzfQ._J94lenMI0vFQUnWXcEbNlwRc8WG5j03zQzUUlQe16c",
                "webhookId": "687f7056d49e380e81e2a922"
            },
            "task_to_execute": "revisao_carga_nw",
            "trigger_dag_id": "PROSPEC_UPDATER"
        }
        
        result = service.processar_carga_mensal(
            task_instance=MockTaskInstance(),
            params=params
        )
        print("Service de PrevisoesCargaMensalNewave inicializado com sucesso")
    except Exception as e:
        print(f"Erro ao debugar manualmente: {str(e)}")