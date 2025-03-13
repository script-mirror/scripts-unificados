import requests as r
import pandas as pd
import pdb
from pprint import pp
import glob
import os
 
def bln_to_geojson_feature(path:str, cod:str, submercado:str, bacia:str, nome:str, cd_subbacia:int):
    
    geojson_feature = {
    "type": "Feature",
    "geometry": {
        "type": "Polygon",
        "coordinates": []},
    "properties": {
        "cod": cod,
        "submercado": submercado,
        "bacia": bacia,
        "nome": nome
    },
    "id": cd_subbacia
}   
    with open (path, 'r') as file:
        data = file.read()
    try:
        coords_base:list = data.replace(',0', '').split('\n')[1:]
        coords_base = list(tuple(filter(('').__ne__, coords_base)))
        coords = [[float(j) for j in x.split(',')] for x in coords_base]
        coords = [x for x in coords]
        coords = coords[1:]
        geojson_feature['geometry']['coordinates'] = [coords]
    except:
        pdb.set_trace()
    return geojson_feature
    
def geojson_features_to_geojson(geojson_features:list):
    geojson = {
        "type": "FeatureCollection",
        "features": geojson_features
    }
    geojson = str(geojson).replace('\'', '\"')
    with open('geojson-todas-subbacias.json', 'w') as file:
        file.write(geojson)
    return 
 
 
def main():    
    
    # features= [
    #     {'path':'/home/arthur-moraes/Downloads/Produtos(1)/Shadow_juruena/Shadow_Juruena/Juruena.bln', 'cod':'PSATJRN', 'submercado':'Sudeste', 'bacia':'Madeira', 'nome':'Jurema', 'cd_subbacia':129},
    #     {'path':'/home/arthur-moraes/Downloads/Produtos(1)/Shadow_saltoApiacas/Shadow_SaltoApiacas/SaltoApiacas.bln', 'cod':'PSATAPI', 'submercado':'Sudeste', 'bacia':'Madeira', 'nome':'SaltoApiacas', 'cd_subbacia':130}
    #     ]

    paths = glob.glob(os.path.join('/home/arthur-moraes/Downloads/Produtos(1)/PSAT',"**",f"*.bln"), recursive=True)
    paths = paths + glob.glob(os.path.join('/home/arthur-moraes/Downloads/Produtos(1)/Shadow_juruena',"**",f"*.bln"), recursive=True)
    paths = paths + glob.glob(os.path.join('/home/arthur-moraes/Downloads/Produtos(1)/Shadow_madeira',"**",f"*.bln"), recursive=True)
    paths = paths + glob.glob(os.path.join('/home/arthur-moraes/Downloads/Produtos(1)/Shadow_saltoApiacas',"**",f"*.bln"), recursive=True)
    '''
    Amaru
    Guajara
    principe
    Jirau
    St_Antonio
    '''
    paths = paths + ['/home/arthur-moraes/Downloads/Produtos(1)/Remoção de Viés/Remoç╞o de Viés/Iguacu/salto_caxias.bln', '/home/arthur-moraes/Downloads/Produtos(1)/Remoção de Viés/Remoç╞o de Viés/Outras_NNE/Santo Antônio do Jari.bln', '/home/arthur-moraes/Downloads/Produtos(1)/Remoção de Viés/Remoç╞o de Viés/Outras_NNE/UHE_BoaEsperanca.bln', '/home/arthur-moraes/Downloads/Produtos(1)/Remoção de Viés/Remoç╞o de Viés/Outras_SE/Sta Clara MG.bln', '/home/arthur-moraes/Downloads/Produtos(1)/Remoção de Viés/Remoç╞o de Viés/Outras_NNE/Pedra do Cavalo.bln', '/home/arthur-moraes/Downloads/Produtos(1)/Remoção de Viés/Remoç╞o de Viés/Paraiba do Sul/VIA_SantaBranca.bln', '/home/arthur-moraes/Downloads/Produtos(1)/Remoção de Viés/Remoç╞o de Viés/Paraiba do Sul/INC_Funil.bln', '/home/arthur-moraes/Downloads/Produtos(1)/Remoção de Viés/Remoç╞o de Viés/Paraiba do Sul/INC_SantaCecilia.bln', '/home/arthur-moraes/Downloads/Produtos(1)/Remoção de Viés/Remoç╞o de Viés/Paraiba do Sul/VIA_Tocos_Lajes.bln', '/home/arthur-moraes/Downloads/Produtos(1)/Remoção de Viés/Remoç╞o de Viés/Santa Maria da Vitoria/UHE_Suica_ajusteGEFS_rev.bln']
    
    df = pd.DataFrame(r.get('https://tradingenergiarz.com/api/v2/rodadas/subbacias').json())[['id', 'nome', 'nome_submercado', 'nome_bacia', 'nome_smap']].rename(columns={'id':'cd_subbacia', 'nome':'cod', 'nome_submercado':'submercado','nome_bacia':'bacia', 'nome_smap':'nome'})
    df['path'] = ''
    
    for path in paths:
        df['path'][df['nome'] == (path[path.rfind("/")+1:]).replace('.bln', '')] = path
    
    df = df[df['path'] != '']
    features = df.to_dict('records')
    geojson_features = [bln_to_geojson_feature(**feature) for feature in features]
    geojson_features_to_geojson(geojson_features)
    

    pdb.set_trace()
    return None


if __name__ == '__main__':  
    main()