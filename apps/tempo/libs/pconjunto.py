import os
import pdb
import glob
import datetime
import pandas as pd
import matplotlib.pyplot as plt

lista = {'Antas': ['14 de Julho', 'Castro Alves'], 'Paranaíba': ['Abaixo do rio Verde', 'Corumba I', 'Corumba IV', 'Emborcacao', 'Itumbiara', 'Nova Ponte'], 'Grande': ['Agua Vermelha', 'Camargos', 'Capao Escuro', 'Euclides da Cunha', 'Funil-MG', 'Furnas', 'Marimbondo', 'Paraguacu', 'Passagem', 'Porto Colombia', 'Porto dos Buenos'], 'Madeira': ['Amaru Mayu', 'Dardanelos', 'Guajara Mirim', 'Guapore', 'Jirau', 'Principe da Beira', 'Rondon II', 'Samuel', 'Santo Antonio'], 'Iguaçu': ['Baixo Iguacu', 'Foz do Areia', 'Jordao Segredo', 'Salto Caxias', 'Santa Clara'], 'Uatuamã': ['Balbina'], 'Itaipu': ['Balsa Santa Maria', 'Florida Estrada', 'Itaipu', 'Ivinhema', 'Porto Taquara'], 'Tocantins': ['Bandeirantes', 'Conceicao do Araguaia', 'Estreito TOC', 'Lajeado', 'Porto Real'], 'Tietê': ['Barra Bonita', 'Edgard de Souza', 'Ibitinga', 'Nova Avanhandava'], 'Uruguai': ['Barra Grande', 'Campos Novos', 'Foz do Chapeco', 'Ita', 'Machadinho', 'Monjolinho', 'Passo Sao Joao', 'Quebra Queixo'], 'Xingu': ['Boa Esperanca', 'Boa Sorte', 'Pimental'], 'São Franci': ['Boqueirao', 'Queimado', 'Retiro Baixo'], 'Doce': ['Candoga', 'Mascarenhas', 'Porto Estrela', 'Sa Carvalho'], 'Paranapane': ['CanoasI', 'Capivara', 'Chavantes', 'Jurumirim', 'Maua', 'Rosana'], 'Itajaí-Açu': ['Capivari Cachoeira', 'Salto Pilao'], 'Tapajós': ['Colider'], 'Curuá-Una': ['Curua Una'], 'Jacuí': ['Dona Francisca', 'Ernestina', 'Passo Real'], 'Paraná': ['Espora', 'Fazenda Buriti', 'Foz do Rio Claro', 'Ilha Solteira', 'Jupia', 'Porto Primavera', 'Salto do Rio Verdinho'], 'Araguari': ['Ferreira Gomes'], 'Paraíba do Sul': ['Funil', 'Ilha dos Pombos Simplicio', 'Santa Branca'], 'Jequitinho': ['Irapé', 'Itapebi'], 'Paraguai': ['Itiquira', 'Jauru', 'Ponte de Pedra'], 'Jaguari': ['Jaguari', 'Picada', 'Santa Cecília'], 'Manso': ['Manso'], 'Paraguaçu': ['Pedra do Cavalo'], 'Itabapoana': ['Rosal']}

def get_pesos(path):

    num_dias_historico = 5
    
    path = os.path.abspath('/WX/WX4TB/Documentos/saidas-modelos/gefs-eta/{}/data/Pesos')
    data_now = datetime.datetime.now()
    data_hoje = datetime.datetime(2022,10,18)
    date_format = data_now.strftime("%Y.%m.%d")
    path_dir = f'C:\\Users\\cs376562\\Desktop\\Tarefas\\Python\\saidaPlotBacias\\{date_format}'
    try:
        os.mkdir(path_dir)
    except:
        pass
    
    for bacia in lista:
        
            fig, ax = plt.subplots()
            df_ec = None
            df_eta = None
            df_gefs = None
            df_media_ec = None
            df_media_eta = None
            df_media_gefs = None

            for d in range(num_dias_historico):
                dt = data_hoje - datetime.timedelta(days=d)
                print(dt)
                for subbacia in lista[bacia]:
                    f = glob.glob(os.path.join(path.format(dt.strftime('%Y%m%d00')), f'Pesos_Bacia_{subbacia}.dat'))
                    df = pd.read_csv(f[0], sep= ' ', skipinitialspace=True)
                    df.fillna(0,inplace=True)
                    
                    if df_ec is None:
                        
                        df_ec = pd.DataFrame(index=df.index)
                        df_eta = pd.DataFrame(index=df.index)
                        df_gefs = pd.DataFrame(index=df.index)
                        df_media_ec = pd.DataFrame(index=df.index)
                        df_media_eta = pd.DataFrame(index=df.index)
                        df_media_gefs = pd.DataFrame(index=df.index)
                            
                    df_ec[subbacia] = df['ECMWF_P']
                    df_eta[subbacia] = df['ETA40_P']
                    df_gefs[subbacia] = df['GEFS_P']
                
                df_media_ec[d] = df_ec.T.mean().mean()
                df_media_eta[d] = df_eta.T.mean().mean()
                df_media_gefs[d] = df_gefs.T.mean().mean()

                if d == 0:
                    plt.title(f'{bacia}')
                    df_ec.T.mean().plot(color="blue", label = 'ec')
                    df_eta.T.mean().plot(color="green", label = 'eta')
                    df_gefs.T.mean().plot(color="orange", label = 'gefs')
                    plt.legend()
                else:
                    df_ec.T.mean().plot(color="blue")
                    df_eta.T.mean().plot(color="green")
                    df_gefs.T.mean().plot(color="orange")
        
            df_media_ec.T.mean().plot(color="blue", linestyle='--')
            df_media_eta.T.mean().plot(color="green", linestyle='--')   
            df_media_gefs.T.mean().plot(color="orange", linestyle='--')
            
            plt.savefig(os.path.join(f'C:\\Users\\cs376562\\Desktop\\Tarefas\\Python\\saidaPlotBacias\\{date_format}', f'{bacia}.png'))
            # quit()
if __name__ == '__main__':
    path = os.path.abspath(r'C:\WX2TB\Documentos\fontes\tempo\gefs-eta-plot\pconjunto-ONS\Arq_Saida\Pesos')
    get_pesos(path)
    
# pdb.set_trace()

    