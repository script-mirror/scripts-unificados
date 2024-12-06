import os 
import datetime 
import unidecode
import sqlalchemy as db 
from selenium import webdriver
import pdb
from bs4 import BeautifulSoup
import re
import difflib as dl
from ordered_set import OrderedSet



def get_ultima_pauta(path_dir,driver):
    date_today = datetime.datetime.now().strftime("%Y-%m-%d-%Hh")
    ##primeiro link
    # driver = webdriver.Chrome()
    url = 'https://www2.aneel.gov.br/aplicacoes_liferay/noticias_area/?idAreaNoticia=425'

    driver.get(url)
    
    response = driver.page_source
    soup = BeautifulSoup(response, 'html.parser')

    if not os.path.isdir(path_dir):
        os.mkdir(path_dir)

    #cria arquivo com nome da pauta se n existir
    dir_list = os.listdir(path_dir)
    if not f"pautas.txt" in dir_list:
        txt_pauta = open(f'{path_dir}/pautas.txt', 'x')
        txt_pauta.close()

    #pega o link e o nome da pauta especifica
    href = soup.find_all('a')[0]['href']
    pauta = soup.find_all('a')[0].text

    #escreve a data de hoje e nome da pauta no arquivo
    text_file = open(f"{path_dir}/pautas.txt", "w",encoding='utf-8')
    name=date_today+"_"+pauta
    text_file.write(name)
    text_file.close()
        
    #html do link da pauta especifica
    driver.get(href)
    html = driver.page_source
    soup1 = BeautifulSoup(html, 'html.parser').get_text()   
    
    #data da pauta
    data = soup.find_all('p')[0].get_text().strip()
    data = datetime.datetime.strptime(data,"%d/%m/%Y").strftime("%Y-%m-%d")
    #cria diretório para a pauta

    dir_pauta = os.path.join(path_dir, data) 
    if not os.path.isdir(dir_pauta):
        os.mkdir(dir_pauta)

    if os.listdir(dir_pauta):
        ultima_pauta_salva = os.listdir(dir_pauta)[-1]
        with open(dir_pauta+"/"+ultima_pauta_salva, 'r', encoding='UTF-8') as f1:
            txt = f1.read()
            f1.close
    else:
        txt=""

    if txt != soup1.lstrip():
        flag_diff = True
        #exclui txt mais antigo e adiciona o mais novo 
        dir_txt = os.listdir(dir_pauta)
        if (len(dir_txt) == 2):
            arquivo_antigo = dir_txt[0]
            os.remove(dir_pauta+"/"+arquivo_antigo)

        pauta = unidecode.unidecode(pauta)
        pauta_name_txt = pauta.strip().replace(" ","_").replace('.','').replace('/',"_")
        
        novo_arquivo = open(dir_pauta+f'/{date_today}_{pauta_name_txt}.txt', 'w',encoding='utf-8')
        novo_arquivo.write(soup1.lstrip())
        novo_arquivo.close()
    else:
        flag_diff = False
        print("é igual")
        

    return data ,href,pauta,flag_diff

            
def leitura_ata(path_dir_arquivo_pauta):
    ata ={}
    with open(path_dir_arquivo_pauta, 'r', encoding='UTF-8') as f1:
        num_processo = ''
        for line in f1.readlines():
            linha_split = line.strip().split()
            if len(linha_split) <= 1:
                continue
            
            if linha_split[1] == 'Processo:':
                num_processo = linha_split[2].replace(',','')
                ata[num_processo] = [line.strip()]
            else:
                if num_processo == '':
                    continue
                ata[num_processo].append(line.strip())
                
    return ata

def diferenca_arquivos(path_dir,data):
    dir_pauta = os.path.join(path_dir, data) 
    flag_envia_email = False
    texto_final = ""
    dir_txt = os.listdir(dir_pauta)
    if len(dir_txt) == 2:
        anterior = dir_txt[0]
        atual = dir_txt[-1]
        ata1 = leitura_ata(dir_pauta+"/"+anterior)
        ata2 = leitura_ata(dir_pauta+"/"+atual)
    else:
        flag_envia_email = True
        unica = dir_txt[0]
        ata1 = leitura_ata(dir_pauta+"/"+unica)
        ata2 = ata1
        for key in ata2.keys():
            texto_final += "<div>"
            for value in ata2[key]:
                texto_final += f"<p>{value}</p>"
            texto_final += "</div><br><hr>"       
        print("A pauta é nova")
        
    if ata1 != ata2:
        conjunto_processos_ata1 = OrderedSet(ata1.keys())
        conjunto_processos_ata2 = OrderedSet(ata2.keys())

        text_antigo =""
        text_novo =""
        

        retirados = conjunto_processos_ata1 - conjunto_processos_ata2
        adicionados = conjunto_processos_ata2 - conjunto_processos_ata1
        

        if adicionados:
            for proc_adicionado in adicionados:
                lines2 = ata2[proc_adicionado]
                for line in lines2:
                    texto_final += f'<span style="color: rgb(107,142,35)">{line}</span><br>'
            texto_final+= "<br><hr>"

        if retirados:
            for proc_retirado in retirados:
                lines1 = ata1[proc_retirado]
                for line in lines1:
                    texto_final += f'<span style="text-decoration: line-through; color:red">{line}</span><br>'

            texto_final+= "<br><hr>"

        for proc in conjunto_processos_ata1.intersection(conjunto_processos_ata2):
            for i_linha in range(len(ata2[proc])):

                if len(ata1[proc]) < len(ata2[proc]):
                    ata1[proc] += ["_"]
                elif len(ata1[proc]) > len(ata2[proc]):
                    ata2[proc] += ["_"]
                    
                lines1 = ata1[proc][i_linha].split()
                lines2 = ata2[proc][i_linha].split()


                seq_matcher = dl.SequenceMatcher(None, lines1, lines2,autojunk=False)
                text_antigo ="<div>"
                text_novo ="<div>"

                for tag, i1, i2, j1, j2 in seq_matcher.get_opcodes():
                
                    if tag == "equal":
                        text_antigo +=" "+ " ".join(lines1[i1:i2])
                        text_novo += " "+ " ".join(lines2[j1:j2])

                    elif tag == "insert":
                        # text_antigo += " "+" ".join(lines1[i1:i2])
                        text_antigo += f' <span style="color:rgb(107,142,35)">{" ".join(lines2[j1:j2])}</span> '

                    elif tag == "delete":
                        text_antigo += f' <span style="text-decoration: line-through; color:red">{" ".join(lines1[i1:i2])}</span> '
                        

                    elif tag == "replace":
                        text_antigo += f' <span style="text-decoration: line-through; color:red">{" ".join(lines1[i1:i2])}</span> '
                        text_antigo += f' <span style="color:rgb(107,142,35)">{" ".join(lines2[j1:j2])}</span>'

                text_antigo+="</div><br>"
                text_novo+="</div><br>"

                if text_antigo == text_novo:
                    texto_final += text_antigo
                elif text_antigo != text_novo:
                    texto_final += text_antigo#+"<strong>Texto Atualizado:</strong><br>"+text_novo+ "<hr>"    

            texto_final+="<hr>"
            flag_envia_email=True

    else:
        print("Os processos são iguais")    
    return texto_final, flag_envia_email



def pinta_pautas_aneel(path_dir,data,href,database,palavras_aneel):
    
    texto_final,flag_envia_email = diferenca_arquivos(path_dir, data)
    if flag_envia_email == True:
        ##Get das palavras cadastradas no site
        select = db.select([palavras_aneel.c.palavra,palavras_aneel.c.tag])
        list_palavras = database.conn.execute(select).fetchall()
        for palavra , tag in list_palavras:
            tag = unidecode.unidecode(tag).lower()
            palavra = f" {palavra.lower()} "
            palavra_no_sensitive = re.compile(re.escape(f'{palavra}'), re.IGNORECASE)
            if(tag == "trading"):
                texto_final = palavra_no_sensitive.sub(f'<span style=" background-color:#006dad ; color:white;">{palavra}</span>', texto_final)
            elif(tag == "regulatorio"):
                texto_final = palavra_no_sensitive.sub(f'<span style=" background-color :#782777; color:white;">{palavra}</span>', texto_final)
            else:
                texto_final = texto_final.replace(palavra, f'<span style=" background-color :#426; color:white;">{palavra}</span>')
        texto_final+=f'<a href="{href}">link para pauta</a><br>'
        texto_final+=f'<a href="http://35.173.154.94:8090/palavras-Aneel">link para alterar palavras</a><br>'

    return texto_final, flag_envia_email




