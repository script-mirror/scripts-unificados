# -*- coding: utf-8 -*-
"""
Exemplo prático de como usar o WhatsAppSender em tasks do Airflow
Este arquivo mostra diferentes cenários de uso
"""

import os
import sys
from datetime import datetime

# Adiciona paths necessários
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from utils.whatsapp_sender import WhatsAppSender
from utils.airflow_notifications import AirflowNotificationHelper


class ExemploWhatsAppAirflow:
    """
    Exemplos práticos de uso do WhatsAppSender em tasks do Airflow
    """
    
    @staticmethod
    def exemplo_task_simples(**kwargs):
        """
        Exemplo 1: Task simples que envia notificação de sucesso
        """
        try:
            # Simulação de processamento
            print("Processando dados...")
            
            # Seu código de processamento aqui
            resultado = {"status": "success", "registros_processados": 150}
            
            # Enviar notificação via WhatsApp
            sender = WhatsAppSender()
            sender.send_message(
                destinatario="PMO",
                mensagem=f"✅ Processamento concluído!\n📊 {resultado['registros_processados']} registros processados"
            )
            
            return resultado
            
        except Exception as e:
            # Em caso de erro, enviar notificação
            sender = WhatsAppSender()
            sender.send_message(
                destinatario="PMO",
                mensagem=f"❌ Erro no processamento: {str(e)}"
            )
            raise
    
    @staticmethod
    def exemplo_task_com_arquivo(**kwargs):
        """
        Exemplo 2: Task que gera um arquivo e envia via WhatsApp
        """
        try:
            task_instance = kwargs['task_instance']
            
            # Simulação de geração de arquivo
            output_dir = "/tmp/airflow_whatsapp_example"
            os.makedirs(output_dir, exist_ok=True)
            
            file_path = os.path.join(output_dir, f"relatorio_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")
            
            with open(file_path, 'w') as f:
                f.write(f"Relatório gerado em {datetime.now()}\n")
                f.write("Dados processados com sucesso!\n")
                f.write("Total de registros: 150\n")
            
            # Enviar arquivo via WhatsApp
            sender = WhatsAppSender()
            success = sender.send_document(
                destinatario="PMO",
                file_path=file_path,
                caption=f"📄 Relatório gerado - {datetime.now().strftime('%d/%m/%Y %H:%M')}"
            )
            
            return {
                'success': True,
                'file_path': file_path,
                'whatsapp_sent': success
            }
            
        except Exception as e:
            print(f"Erro na task com arquivo: {str(e)}")
            raise
    
    @staticmethod
    def exemplo_task_tabela_html(**kwargs):
        """
        Exemplo 3: Task que gera uma tabela HTML, converte para imagem e envia
        """
        try:
            # Dados fictícios para a tabela
            dados = [
                {"Submercado": "SE", "Carga": 45000, "Diferenca": -500},
                {"Submercado": "S", "Carga": 12000, "Diferenca": 200},
                {"Submercado": "NE", "Carga": 8000, "Diferenca": -100},
                {"Submercado": "N", "Carga": 5000, "Diferenca": 50}
            ]
            
            # Gerar HTML da tabela
            html_tabela = """
            <div style="font-family: Arial, sans-serif; padding: 20px;">
                <h2 style="color: #2E4057; text-align: center;">Diferença de Cargas por Submercado</h2>
                <table style="border-collapse: collapse; width: 100%; margin: 20px 0;">
                    <thead>
                        <tr style="background-color: #3498DB; color: white;">
                            <th style="border: 1px solid #ddd; padding: 12px; text-align: left;">Submercado</th>
                            <th style="border: 1px solid #ddd; padding: 12px; text-align: right;">Carga (MW)</th>
                            <th style="border: 1px solid #ddd; padding: 12px; text-align: right;">Diferença (MW)</th>
                        </tr>
                    </thead>
                    <tbody>
            """
            
            for item in dados:
                cor = "#E8F5E8" if item["Diferenca"] >= 0 else "#FFE8E8"
                html_tabela += f"""
                        <tr style="background-color: {cor};">
                            <td style="border: 1px solid #ddd; padding: 8px;">{item["Submercado"]}</td>
                            <td style="border: 1px solid #ddd; padding: 8px; text-align: right;">{item["Carga"]:,}</td>
                            <td style="border: 1px solid #ddd; padding: 8px; text-align: right;">{item["Diferenca"]:+,}</td>
                        </tr>
                """
            
            html_tabela += """
                    </tbody>
                </table>
                <p style="color: #7F8C8D; font-size: 12px; text-align: center;">
                    Gerado automaticamente pelo Airflow em """ + datetime.now().strftime('%d/%m/%Y %H:%M:%S') + """
                </p>
            </div>
            """
            
            # Aqui você usaria sua API de conversão HTML para imagem
            # Por enquanto, vamos simular salvando o HTML
            output_dir = "/tmp/airflow_whatsapp_example"
            os.makedirs(output_dir, exist_ok=True)
            
            html_file = os.path.join(output_dir, f"tabela_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html")
            with open(html_file, 'w', encoding='utf-8') as f:
                f.write(html_tabela)
            
            # Simulação: se tivesse a API de conversão, seria algo assim:
            # image_path = converter_html_para_imagem(html_tabela)
            
            # Por enquanto, enviamos o HTML
            sender = WhatsAppSender()
            success = sender.send_document(
                destinatario="PMO",
                file_path=html_file,
                caption="📊 Tabela de Diferença de Cargas\n📅 " + datetime.now().strftime('%d/%m/%Y %H:%M')
            )
            
            return {
                'success': True,
                'html_file': html_file,
                'whatsapp_sent': success,
                'dados_processados': len(dados)
            }
            
        except Exception as e:
            print(f"Erro na task de tabela: {str(e)}")
            raise
    
    @staticmethod
    def exemplo_task_com_helper(**kwargs):
        """
        Exemplo 4: Usando o AirflowNotificationHelper
        """
        try:
            helper = AirflowNotificationHelper()
            
            # Simulação de processamento
            print("Processando com helper...")
            
            # Notificar sucesso
            helper.notify_task_success(
                task_name="Exemplo com Helper",
                details="✅ Processamento realizado com sucesso\n📊 100 registros atualizados"
            )
            
            return {'success': True, 'method': 'helper'}
            
        except Exception as e:
            # Notificar erro
            helper = AirflowNotificationHelper()
            helper.notify_task_error(
                task_name="Exemplo com Helper",
                error_msg=str(e)
            )
            raise
    
    @staticmethod
    def exemplo_task_multiplos_destinos(**kwargs):
        """
        Exemplo 5: Enviando para múltiplos destinatários
        """
        try:
            sender = WhatsAppSender()
            
            # Dados do processamento
            resultado = {
                'timestamp': datetime.now().strftime('%d/%m/%Y %H:%M:%S'),
                'status': 'success',
                'registros': 200
            }
            
            # Lista de destinatários
            destinatarios = ["PMO", "FSARH"]
            
            mensagem = f"""🔄 Processamento Multi-destino
⏰ {resultado['timestamp']}
📊 {resultado['registros']} registros processados
✅ Status: {resultado['status']}"""
            
            # Enviar para cada destinatário
            resultados_envio = {}
            for dest in destinatarios:
                try:
                    success = sender.send_message(dest, mensagem)
                    resultados_envio[dest] = success
                    print(f"Enviado para {dest}: {success}")
                except Exception as e:
                    print(f"Erro ao enviar para {dest}: {str(e)}")
                    resultados_envio[dest] = False
            
            return {
                'success': True,
                'envios': resultados_envio,
                'total_destinatarios': len(destinatarios)
            }
            
        except Exception as e:
            print(f"Erro na task múltiplos destinos: {str(e)}")
            raise


# Funções para usar diretamente em DAGs
def task_notificacao_simples(**kwargs):
    """Task simples para notificação"""
    return ExemploWhatsAppAirflow.exemplo_task_simples(**kwargs)

def task_envio_arquivo(**kwargs):
    """Task para envio de arquivo"""
    return ExemploWhatsAppAirflow.exemplo_task_com_arquivo(**kwargs)

def task_tabela_html(**kwargs):
    """Task para tabela HTML"""
    return ExemploWhatsAppAirflow.exemplo_task_tabela_html(**kwargs)

def task_com_helper(**kwargs):
    """Task usando helper"""
    return ExemploWhatsAppAirflow.exemplo_task_com_helper(**kwargs)

def task_multiplos_destinos(**kwargs):
    """Task para múltiplos destinatários"""
    return ExemploWhatsAppAirflow.exemplo_task_multiplos_destinos(**kwargs)


if __name__ == "__main__":
    # Para teste local
    print("Testando exemplos de WhatsApp Airflow...")
    
    # Teste básico
    try:
        resultado = ExemploWhatsAppAirflow.exemplo_task_simples()
        print(f"Resultado: {resultado}")
    except Exception as e:
        print(f"Erro no teste: {str(e)}")
