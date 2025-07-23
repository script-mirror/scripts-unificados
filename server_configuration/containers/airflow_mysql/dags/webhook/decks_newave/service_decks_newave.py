import requests
import os
import sys
import numpy as np
import pandas as pd 
from inewave.newave import Patamar, Cadic, Sistema
from datetime import datetime, date

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)
from utils.repository_webhook import SharedRepository
from utils.html_builder import HtmlBuilder
from validator_decks_newave import DecksNewaveValidator

class DecksNewaveService:
    def __init__(self):
        self.repository = SharedRepository()
        self.validator = DecksNewaveValidator()
    
    @staticmethod
    def determinar_versao_por_filename(filename):
        """
        Determina a versão do deck baseada no nome do arquivo.
        Retorna 'preliminar' para deck preliminar ou 'definitivo' para deck definitivo.
        """
        if not filename:
            return 'preliminar'  # Default para compatibilidade
        
        filename_upper = filename.upper()
        
        if 'DEFINITIVO' in filename_upper:
            return 'definitivo'
        elif 'PRELIMINAR' in filename_upper:
            return 'preliminar'
        else:
            # Se não encontrar nem preliminar nem definitivo, assume preliminar como padrão
            return 'preliminar'

    @staticmethod
    def validar_dados_entrada(**kwargs):
        validator = DecksNewaveValidator()
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
            
            download_path = "/tmp/decks_newave"
            
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
            final_extracted_files = []
            dat_files_found = []
            
            if file_path.endswith('.zip'):
                import zipfile
                
                extract_path = os.path.dirname(file_path)
                
                # Extrair o ZIP principal primeiro
                print(f"Extraindo ZIP principal: {file_path}")
                with zipfile.ZipFile(file_path, 'r') as zip_ref:
                    zip_ref.extractall(extract_path)
                    extracted_files = zip_ref.namelist()
                
                print(f"Arquivos extraídos do ZIP principal: {extracted_files}")
                
                # Buscar por ZIPs aninhados e extrair todos de uma vez
                print("Procurando por ZIPs aninhados...")
                for root, dirs, files in os.walk(extract_path):
                    for file in files:
                        if file.endswith('.zip'):
                            zip_path = os.path.join(root, file)
                            try:
                                if zipfile.is_zipfile(zip_path):
                                    print(f"Extraindo ZIP aninhado: {file}")
                                    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                                        zip_ref.extractall(extract_path)
                                        nested_files = zip_ref.namelist()
                                        print(f"Arquivos extraídos de {file}: {nested_files}")
                                else:
                                    print(f"Arquivo {file} não é um ZIP válido")
                            except Exception as e:
                                print(f"Erro ao extrair {file}: {str(e)}")
                
                # Agora buscar pelos arquivos .DAT uma única vez
                print("Buscando arquivos .DAT...")
                dat_files_found = []
                all_files = []
                
                for root, dirs, files in os.walk(extract_path):
                    for file in files:
                        file_path_full = os.path.join(root, file)
                        relative_path = os.path.relpath(file_path_full, extract_path)
                        all_files.append(relative_path)
                        
                        # Verificar se é um arquivo .DAT necessário
                        if file.upper().endswith('.DAT'):
                            dat_files_found.append(relative_path)
                            print(f"Arquivo .DAT encontrado: {relative_path}")
                
                # Verificar se encontramos os arquivos .DAT necessários
                required_dat_files = ['C_ADIC.DAT', 'SISTEMA.DAT', 'PATAMAR.DAT']
                found_required_files = []
                
                for required_file in required_dat_files:
                    for dat_file in dat_files_found:
                        if os.path.basename(dat_file).upper() == required_file.upper():
                            found_required_files.append(required_file)
                            print(f"Arquivo necessário encontrado: {required_file} -> {dat_file}")
                            break
                
                print(f"Arquivos .DAT necessários encontrados: {found_required_files}")
                print(f"Total de arquivos .DAT encontrados: {len(dat_files_found)}")
                print(f"Total de arquivos extraídos: {len(all_files)}")
                
                # Usar todos os arquivos extraídos
                final_extracted_files = all_files
                
            else:
                print(f"Arquivo não é ZIP: {file_path}")
                final_extracted_files = []
                dat_files_found = []
                found_required_files = []
                            
            xcom_data = {
                'success': True,
                'original_file': file_path,
                'extract_path': extract_path,
                'extracted_files': final_extracted_files,
                'dat_files_found': dat_files_found,
                'required_files_found': found_required_files,
                'total_files': len(final_extracted_files),
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
            
            # Obter informações do produto para determinar a versão
            params = kwargs.get('params', {})
            product_details = params.get('product_details', {})
            filename = product_details.get('filename', '')
            
            # Determinar versão baseada no filename
            versao = DecksNewaveService.determinar_versao_por_filename(filename)
            
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
            
            nw_cadic_df['versao'] = versao

            nw_cadic_records = nw_cadic_df.to_dict('records')
            
            print(f"Processamento concluído:")
            
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
            
            # Obter informações do produto para determinar a versão
            params = kwargs.get('params', {})
            product_details = params.get('product_details', {})
            filename = product_details.get('filename', '')
            
            # Determinar versão baseada no filename
            versao = DecksNewaveService.determinar_versao_por_filename(filename)
            
            sistema_file = os.path.join(extract_result['extract_path'], 'SISTEMA.DAT')
            
            sistema_object = Sistema.read(sistema_file)
            
            # Manipulando dataframe de valores de mercado de energia total
            sistema_mercado_energia_df = sistema_object.mercado_energia.copy()   
            
            if sistema_mercado_energia_df is None:
                error_msg = "Dados de sistema do mercado de energia não encontrados no arquivo!"
                print(error_msg)
                raise Exception(error_msg)
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
            
            nw_sistema_df['versao'] = versao
            
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
                'versao'
            ]
            
            nw_sistema_df = nw_sistema_df.reindex(columns=ordem_colunas)
            
            nw_sistema_records = nw_sistema_df.to_dict('records')
            
            print(f"Processamento concluído:")
            
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
    def processar_patamar_nw(**kwargs):
        try:
            task_instance = kwargs['task_instance']
            extract_result = task_instance.xcom_pull(task_ids='extrair_arquivos')
            download_result = task_instance.xcom_pull(task_ids='download_arquivos')

            print(f"Dados de extração recebidos via XCom: {extract_result}")

            if not extract_result or not extract_result.get('success'):
                raise Exception(f"Falha na extração anterior: {extract_result.get('error', 'Erro desconhecido')}")
            
            # Obter informações do produto para determinar a versão
            params = kwargs.get('params', {})
            product_details = params.get('product_details', {})
            filename = product_details.get('filename', '')
            
            # Determinar versão baseada no filename
            versao = DecksNewaveService.determinar_versao_por_filename(filename)
            
            patamar_file = os.path.join(extract_result['extract_path'], 'PATAMAR.DAT')

            # Corrigir a verificação de existência do arquivo
            if not os.path.exists(patamar_file):
                error_msg = f"Arquivo PATAMAR.DAT não encontrado em {extract_result['extract_path']}"
                print(error_msg)
                raise FileNotFoundError(error_msg)

            print(f"Arquivo PATAMAR.DAT encontrado: {patamar_file}")
            patamar_object = Patamar.read(patamar_file)

            patamares = {
                '1': 'Pesado',
                '2': 'Medio',
                '3': 'Leve'
            }

            indices_bloco = {
                1: 'PCH',
                2: 'PCT',
                3: 'EOL',
                4: 'UFV',
                5: 'PCH_MMGD',
                6: 'PCT_MMGD',
                7: 'EOL_MMGD',
                8: 'UFV_MMGD'
            }

            submercados = {
                '1': 'SE',
                '2': 'S',
                '3': 'NE',
                '4': 'N',
                '11': 'FC'
            }

            # Verificar se os dados foram extraídos corretamente
            carga_patamares_df = patamar_object.carga_patamares.copy() 

            if carga_patamares_df is None or carga_patamares_df.empty:
                error_msg = "Não foi possível extrair a carga dos patamares NEWAVE"
                print(error_msg)
                raise Exception(error_msg)

            print(f"Total de registros de carga: {len(carga_patamares_df)}")

            duracao_mensal_patamares_df = patamar_object.duracao_mensal_patamares.copy() 

            if duracao_mensal_patamares_df is None or duracao_mensal_patamares_df.empty:
                error_msg = "Não foi possível extrair a duração mensal dos patamares NEWAVE"
                print(error_msg)
                raise Exception(error_msg)

            print(f"Total de registros de duração mensal: {len(duracao_mensal_patamares_df)}")

            intercambio_patamares_df = patamar_object.intercambio_patamares.copy() 

            if intercambio_patamares_df is None or intercambio_patamares_df.empty:
                error_msg = "Não foi possível extrair os intercâmbios por patamares NEWAVE"
                print(error_msg)
                raise Exception(error_msg)

            print(f"Total de registros de intercâmbios: {len(intercambio_patamares_df)}")

            usinas_nao_simuladas_df = patamar_object.usinas_nao_simuladas.copy()

            if usinas_nao_simuladas_df is None or usinas_nao_simuladas_df.empty:
                error_msg = "Não foi possível extrair as usinas não simuladas do NEWAVE"
                print(error_msg)
                raise Exception(error_msg)

            print(f"Total de registros de usinas não simuladas: {len(usinas_nao_simuladas_df)}")

            # Processamento das tabelas
            # 1. Processar carga_patamares_df
            carga_df = carga_patamares_df.copy()
            carga_df['patamar_nome'] = carga_df['patamar'].astype(str).map(patamares)
            carga_df['submercado_nome'] = carga_df['codigo_submercado'].astype(str).map(submercados)
            carga_df = carga_df.rename(columns={
                'data': 'dt_referente',
                'valor': 'pu_demanda_med',
                'codigo_submercado': 'submercado'
            })
            carga_df = carga_df[['dt_referente', 'patamar', 'patamar_nome', 'submercado', 'submercado_nome', 'pu_demanda_med']]

            # 2. Processar duracao_mensal_patamares_df
            duracao_df = duracao_mensal_patamares_df.copy()
            duracao_df['patamar_nome'] = duracao_df['patamar'].astype(str).map(patamares)
            duracao_df = duracao_df.rename(columns={
                'data': 'dt_referente',
                'valor': 'duracao_mensal'
            })
            duracao_df = duracao_df[['dt_referente', 'patamar', 'patamar_nome', 'duracao_mensal']]

            # 3. Processar intercambio_patamares_df
            intercambio_df = intercambio_patamares_df.copy()
            intercambio_df['patamar_nome'] = intercambio_df['patamar'].astype(str).map(patamares)
            intercambio_df['submercado_de_nome'] = intercambio_df['submercado_de'].astype(str).map(submercados)
            intercambio_df['submercado_para_nome'] = intercambio_df['submercado_para'].astype(str).map(submercados)
            intercambio_df = intercambio_df.rename(columns={
                'data': 'dt_referente',
                'valor': 'pu_intercambio_med'
            })
            intercambio_df = intercambio_df[['dt_referente', 'patamar', 'patamar_nome', 'submercado_de', 'submercado_de_nome', 
                                           'submercado_para', 'submercado_para_nome', 'pu_intercambio_med']]

            # 4. Processar usinas_nao_simuladas_df
            usinas_df = usinas_nao_simuladas_df.copy()
            usinas_df['patamar_nome'] = usinas_df['patamar'].astype(str).map(patamares)
            usinas_df['submercado_nome'] = usinas_df['codigo_submercado'].astype(str).map(submercados)
            usinas_df['indice_bloco_nome'] = usinas_df['indice_bloco'].map(indices_bloco)
            usinas_df = usinas_df.rename(columns={
                'data': 'dt_referente',
                'codigo_submercado': 'submercado',
                'valor': 'pu_montante_med'
            })
            usinas_df = usinas_df[['dt_referente', 'patamar', 'patamar_nome', 'submercado', 'submercado_nome', 
                                 'indice_bloco', 'indice_bloco_nome', 'pu_montante_med']]

            product_datetime_str = download_result.get('product_datetime') if download_result else None
            dt_deck = datetime.strptime(product_datetime_str, '%Y-%m-%d %H:%M:%S') if product_datetime_str else datetime.now().replace(day=1)

            # TABELA 1: Preparar DataFrame de carga com indice_bloco = 'CARGA'
            carga_transformada_df = carga_df.copy()
            carga_transformada_df['indice_bloco'] = 'CARGA'
            carga_transformada_df['valor_pu'] = carga_transformada_df['pu_demanda_med']
            carga_transformada_df = carga_transformada_df.drop(columns=['pu_demanda_med'])
            
            # Preparar DataFrame de usinas não simuladas
            usinas_transformada_df = usinas_df.copy()
            usinas_transformada_df['valor_pu'] = usinas_transformada_df['pu_montante_med']
            usinas_transformada_df = usinas_transformada_df.drop(columns=['pu_montante_med'])
            
            # Concatenar os dois DataFrames
            patamar_carga_usinas_df = pd.concat([carga_transformada_df, usinas_transformada_df], ignore_index=True)

            # Adicionar duração mensal
            patamar_carga_usinas_df = pd.merge(
                patamar_carga_usinas_df,
                duracao_df,
                on=['dt_referente', 'patamar', 'patamar_nome'],
                how='left'
            )

            # Adicionar dados complementares na tabela 1
            patamar_carga_usinas_df['dt_deck'] = dt_deck.strftime('%Y-%m-%d')
            patamar_carga_usinas_df['versao'] = versao

            # Remover colunas numéricas originais e renomear as colunas de texto
            patamar_carga_usinas_df = patamar_carga_usinas_df.drop(columns=['patamar', 'submercado'])
            patamar_carga_usinas_df = patamar_carga_usinas_df.rename(columns={
                'patamar_nome': 'patamar',
                'submercado_nome': 'submercado'
            })
            
            # Para a coluna indice_bloco, usar o valor de indice_bloco_nome quando disponível, senão manter o valor atual
            patamar_carga_usinas_df['indice_bloco'] = patamar_carga_usinas_df.apply(
                lambda row: row['indice_bloco_nome'] if pd.notna(row.get('indice_bloco_nome')) else row['indice_bloco'], axis=1
            )
            
            # Remover a coluna indice_bloco_nome se existir
            if 'indice_bloco_nome' in patamar_carga_usinas_df.columns:
                patamar_carga_usinas_df = patamar_carga_usinas_df.drop(columns=['indice_bloco_nome'])

            # Selecionar colunas finais da tabela 1
            colunas_tabela1 = [
                'dt_referente', 'patamar', 'submercado', 'valor_pu',
                'duracao_mensal', 'indice_bloco', 'dt_deck', 'versao'
            ]

            patamar_carga_usinas_df['dt_referente'] = patamar_carga_usinas_df['dt_referente'].dt.strftime('%Y-%m-%d')
            patamar_carga_usinas_df['dt_deck'] = patamar_carga_usinas_df['dt_deck'].astype(str)
            patamar_carga_usinas_df = patamar_carga_usinas_df[colunas_tabela1]
            
            # TABELA 2: Intercâmbios por Patamares + Duração
            patamar_intercambio_df = pd.merge(
                intercambio_df,
                duracao_df,
                on=['dt_referente', 'patamar', 'patamar_nome'],
                how='inner'
            )

            # Adicionar dados complementares na tabela 2
            patamar_intercambio_df['dt_deck'] = dt_deck.strftime('%Y-%m-%d')
            if patamar_intercambio_df['dt_deck'].dtype == 'object':
                patamar_intercambio_df['dt_deck'] = patamar_intercambio_df['dt_deck'].astype(str)
            patamar_intercambio_df['versao'] = versao

            # Remover colunas numéricas originais e renomear as colunas de texto
            patamar_intercambio_df = patamar_intercambio_df.drop(columns=['patamar', 'submercado_de', 'submercado_para'])
            patamar_intercambio_df = patamar_intercambio_df.rename(columns={
                'patamar_nome': 'patamar',
                'submercado_de_nome': 'submercado_de',
                'submercado_para_nome': 'submercado_para'
            })

            # Selecionar colunas finais da tabela 2
            colunas_tabela2 = [
                'dt_referente', 'patamar', 'submercado_de', 'submercado_para',
                'pu_intercambio_med', 'duracao_mensal', 'dt_deck', 'versao'
            ]

            patamar_intercambio_df['dt_referente'] = patamar_intercambio_df['dt_referente'].dt.strftime('%Y-%m-%d')
            patamar_intercambio_df['dt_deck'] = patamar_intercambio_df['dt_deck'].astype(str)
            
            patamar_intercambio_df['pu_intercambio_med'] = patamar_intercambio_df['pu_intercambio_med'].round(4)
            
            patamar_intercambio_df = patamar_intercambio_df[colunas_tabela2]
            patamar_intercambio_df = patamar_intercambio_df.replace([np.inf, -np.inf], np.nan)
            
            patamar_carga_usinas_records = patamar_carga_usinas_df.to_dict('records')
            patamar_intercambio_records = patamar_intercambio_df.to_dict('records')
           
            import pickle

            temp_dir = "/tmp/deck_preliminar_newave/processed_data"
            os.makedirs(temp_dir, exist_ok=True)

            carga_usinas_file = os.path.join(temp_dir, "patamar_carga_usinas.pkl")
            intercambio_file = os.path.join(temp_dir, "patamar_intercambio.pkl")

            try:
                with open(carga_usinas_file, 'wb') as f:
                    pickle.dump(patamar_carga_usinas_records, f)
                print(f"Arquivo {carga_usinas_file} salvo com sucesso")
            except Exception as e:
                print(f"Erro ao salvar arquivo carga_usinas: {str(e)}")
                raise

            try:
                with open(intercambio_file, 'wb') as f:
                    pickle.dump(patamar_intercambio_records, f)
                print(f"Arquivo {intercambio_file} salvo com sucesso")
            except Exception as e:
                print(f"Erro ao salvar arquivo intercambio: {str(e)}")
                raise

            print(f"Dados salvos em arquivos temporários:")
            print(f"- Carga e usinas: {carga_usinas_file} ({len(patamar_carga_usinas_records)} registros)")
            print(f"- Intercâmbio: {intercambio_file} ({len(patamar_intercambio_records)} registros)")

            # Retornar apenas metadados via XCom
            xcom_data = {
                'success': True,
                'carga_usinas_file': carga_usinas_file,
                'intercambio_file': intercambio_file,
                'carga_usinas_count': len(patamar_carga_usinas_records),
                'intercambio_count': len(patamar_intercambio_records),
                'product_datetime': product_datetime_str,
                'message': f'Arquivo PATAMAR.DAT processado com sucesso - 2 tabelas geradas ({len(patamar_carga_usinas_records)} + {len(patamar_intercambio_records)} registros)'
            }

            print(f"task(processar_patamar_nw) - Processamento concluído com sucesso")

            return xcom_data
    
        except Exception as e:
            error_msg = f"Erro ao processar deck NW PATAMAR: {str(e)}"
            print(error_msg)
            raise Exception(error_msg)
    
    @staticmethod
    def enviar_dados_sistema_cadic_para_api(**kwargs):
        try:
            repository = SharedRepository()

            auth_headers = repository.get_auth_token()
            headers = {
                **auth_headers, 
                'Content-Type': 'application/json',
                'accept': 'application/json'
            }

            print(f"Headers: {headers}")

            api_url = os.getenv("DNS", "http://localhost:8000")
            api_url += "/api/v2"
            
            task_instance = kwargs['task_instance']
            nw_sist_result = task_instance.xcom_pull(task_ids='processar_deck_nw_sist')
            nw_cadic_result = task_instance.xcom_pull(task_ids='processar_deck_nw_cadic')

            if not nw_sist_result or not nw_sist_result.get('success') or not nw_cadic_result or not nw_cadic_result.get('success'):
                raise Exception("Dados necessários não encontrados ou inválidos nos XComs")

            nw_sist_records = nw_sist_result.get('nw_sistema_records', [])
            nw_cadic_records = nw_cadic_result.get('nw_cadic_records', [])
            product_datetime_str = nw_sist_result.get('product_datetime') or nw_cadic_result.get('product_datetime')

            print(f"Preparando dados para envio à API: {len(nw_sist_records)} registros de SISTEMA e {len(nw_cadic_records)} registros CADIC")

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
            
            if request_sistema.status_code != 200:
                raise Exception(f"Erro ao enviar carga do SISTEMA para API: {request_sistema.text}")


            print(f"Enviando dados para: {cadic_url}")

            request_cadic = requests.post(
                cadic_url,
                headers=headers,
                json=nw_cadic_records,  # Use json parameter to properly encode the data
            )
            
            if request_cadic.status_code != 200:
                raise Exception(f"Erro ao enviar carga do CADIC para API: {request_cadic.text}")
            
            xcom_data = {
                'success': True,
                'message': 'Dados do SISTEMA e CADIC enviados para a API com sucesso',
                'product_datetime': product_datetime_str  # Repassa a data do produto para as próximas tasks
            }

            print(f"task(enviar_dados_para_api) - Retornando dados para XCom: {xcom_data}")

            return xcom_data
        except Exception as e:
            error_msg = f"Erro ao enviar dados para API: {str(e)}"
            print(error_msg)
            raise Exception(error_msg)
    
    @staticmethod
    def enviar_dados_patamares_para_api(**kwargs):
        try:
            import pickle
            
            repository = SharedRepository()

            auth_headers = repository.get_auth_token()
            headers = {
                **auth_headers, 
                'Content-Type': 'application/json',
                'accept': 'application/json'
            }

            print(f"Headers: {headers}")

            api_url = os.getenv("DNS", "http://localhost:8000")
            
            api_url += "/api/v2"
            
            task_instance = kwargs['task_instance']
            
            patamar_nw_result = task_instance.xcom_pull(task_ids='processar_patamar_nw')
            
            carga_usinas_file = patamar_nw_result.get('carga_usinas_file')
            intercambio_file = patamar_nw_result.get('intercambio_file')
        
            if not carga_usinas_file or not intercambio_file:
                raise Exception("Arquivos de dados temporários não encontrados")
        
            with open(carga_usinas_file, 'rb') as f:
                patamar_carga_usinas_records = pickle.load(f)
        
            with open(intercambio_file, 'rb') as f:
                patamar_intercambio_records = pickle.load(f)
        
            product_datetime_str = patamar_nw_result.get('product_datetime')
            
            print(f"Dados carregados dos arquivos temporários:")
            print(f"- Carga e usinas: {len(patamar_carga_usinas_records)} registros")
            print(f"- Intercâmbio: {len(patamar_intercambio_records)} registros")
            
            patamar_carga_usinas_url = f"{api_url}/decks/newave/patamar/carga_usinas"
            patamar_intercambio_url = f"{api_url}/decks/newave/patamar/intercambio"
            
            
            print(f"Enviando dados para: {patamar_carga_usinas_url}")
            
            request_patamar_carga_usinas = requests.post(
                patamar_carga_usinas_url,
                headers=headers,
                json=patamar_carga_usinas_records,  
            )
            
            if request_patamar_carga_usinas.status_code != 200:
                raise Exception(f"Erro ao enviar patamar do newave de carga e usina para API: {request_patamar_carga_usinas.text}")
            
            
            print(f"Enviando dados para: {patamar_intercambio_url}")
            
            request_patamar_intercambio = requests.post(
                patamar_intercambio_url,
                headers=headers,
                json=patamar_intercambio_records,  
            )
            
            if request_patamar_intercambio.status_code != 200:
                raise Exception(f"Erro ao enviar patamar do newave de intercambio para API: {request_patamar_intercambio.text}")
            
            xcom_data = {
                'success': True,
                'message': 'Dados dos Patamares enviados para a API com sucesso',
                'product_datetime': product_datetime_str  # Repassa a data do produto para as próximas tasks
            }

            print(f"task(enviar_dados_para_api) - Retornando dados para XCom: {xcom_data}")
        
            return xcom_data
        
        except Exception as e:
            error_msg = f"Erro ao enviar dados dos Patamares para API: {str(e)}"
            print(error_msg)
            raise Exception(error_msg)
            
    @staticmethod
    def gerar_tabela_diferenca_cargas(**kwargs):
        try:
            product_datetime_str = kwargs.get('dag_run').conf.get('product_details').get('dataProduto')
            if product_datetime_str:
                product_datetime_str = product_datetime_str.replace('/', '')
            
            # Obter informações do produto para determinar o tipo de deck
            product_details = kwargs.get('dag_run').conf.get('product_details', {})
            filename = product_details.get('filename', '')
            
            # Determinar tipo de deck baseado no filename
            tipo_deck = DecksNewaveService.determinar_versao_por_filename(filename)
            
            api_url = os.getenv("URL_API_V2", "http://localhost:8000/api/v2")
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
            
            
            # Pegando valores de carga de ANDE
            cadic_ande_url = f"{api_url}/decks/newave/cadic/ande"
            cadic_ande_response = requests.get(
                cadic_ande_url,
                headers=headers
            )
            if cadic_ande_response.status_code != 200:
                raise Exception(f"Erro ao obter dados de carga do ANDE: {cadic_ande_response.text}")
            
            cadic_ande_values = cadic_ande_response.json() 
            
            
            # Pegando valores de MMGD Total 
            sistema_mmgd_total_url = f"{api_url}/decks/newave/sistema/mmgd_total"
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
                dados_ande=cadic_ande_values,
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
            
            # Gerar nome do arquivo baseado na data do produto e tipo de deck
            if product_datetime_str:
                # dt = datetime.strptime(product_datetime_str, '%Y-%m-%d %H:%M:%S')
                image_filename = f"tabela_diferenca_cargas_{tipo_deck}_{product_datetime_str}.png"
            else:
                image_filename = f"tabela_diferenca_cargas_{tipo_deck}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
            
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
            
            # Obter informações do produto para determinar o tipo de deck
            product_details = kwargs.get('dag_run').conf.get('product_details', {})
            filename = product_details.get('filename', '')
            product_datetime_str = product_details.get('dataProduto')
            
            # Determinar tipo de deck baseado no filename
            tipo_deck = DecksNewaveService.determinar_versao_por_filename(filename)
            tipo_deck_label = 'PRELIMINAR' if tipo_deck == 'preliminar' else 'DEFINITIVO'
            
            image_path = tabela_result.get('image_path')
            image_filename = tabela_result.get('image_filename')
            
            if not image_path or not os.path.exists(image_path):
                raise Exception(f"Arquivo de imagem não encontrado: {image_path}")
            
            print(f"Preparando envio da imagem: {image_path}")
            
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            sys.path.insert(0, project_root)
            from utils.whatsapp_sender import WhatsAppSender
            
            whatsapp_sender = WhatsAppSender()
            
            try:
                success = whatsapp_sender.send_table_notification(
                    table_type=f"Diferença de Cargas NEWAVE {tipo_deck_label}",
                    product_datetime=product_datetime_str or "Data não informada",
                    image_path=image_path,
                    # destinatario="Premissas Preco"
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
    service = DecksNewaveService()
    class MockTaskInstance:
        def xcom_pull(self, task_ids, key=None):
            return {'success': True, 'original_file': '/tmp/deck_preliminar_newave/Deck NEWAVE Preliminar.zip', 'extract_path': '/tmp/deck_preliminar_newave', 'extracted_files': ['ADTERM.DAT', 'AGRINT.DAT', 'ARQUIVOS.DAT', 'BID.DAT', 'CASO.DAT', 'CDEFVAR.DAT', 'CLAST.DAT', 'CONFHD.DAT', 'CONFT.DAT', 'CURVA.DAT', 'CVAR.DAT', 'C_ADIC.DAT', 'DGER.DAT', 'DSVAGUA.DAT', 'ELNINO.DAT', 'ENSOAUX.DAT', 'EXPH.DAT', 'EXPT.DAT', 'FORMAT.TMP', 'GHMIN.DAT', 'GTMINPAT.DAT', 'HIDR.DAT', 'indices.csv', 'ITAIPU.DAT', 'LOSS.DAT', 'MANUTT.DAT', 'MENSAG.TMP', 'MODIF.DAT', 'NewaveMsgPortug.txt', 'PATAMAR.DAT', 'PENALID.DAT', 'polinjus.csv', 'POSTOS.DAT', 'RE.DAT', 'REE.DAT', 'restricao-eletrica.csv', 'selcor.dat', 'SHIST.DAT', 'SISTEMA.DAT', 'tecno.dat', 'TERM.DAT', 'VAZOES.DAT', 'VAZPAST.DAT', 'volref_saz.dat', 'volumes-referencia.csv', 'Leia-me.pdf'], 'message': 'Arquivos extraídos com sucesso de Deck NEWAVE Preliminar.zip'}
    
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
        result = DecksNewaveService.processar_patamar_nw(
            task_instance=MockTaskInstance(),
            params=params
        )
    except Exception as e:
        print(f"Erro ao debugar manualmente: {str(e)}")



