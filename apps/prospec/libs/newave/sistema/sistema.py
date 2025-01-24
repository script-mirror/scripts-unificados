import pandas as pd
import re



INFO_BLOCOS={}
INFO_BLOCOS['CUSTO DO DEFICIT']={

    'regex':'(.{4}) (.{10}) (.{2}) (.{7}) (.{7}) (.{7}) (.{7}) (.{5}) (.{5}) (.{5}) (.{5})',
	'formatacao':'{:>2}  {:>3}  {:>3}  {:>10}'
}

INFO_BLOCOS['LIMITES DE INTERCAMBIO']={
    'regex':'(.{4}) (.{3})(.{6}) (.{7}) (.{7}) (.{7}) (.{7}) (.{7}) (.{7}) (.{7}) (.{7}) (.{7}) (.{7}) (.{7})',
	'formatacao':'{:>4} {:>3}{:>6} {:>7} {:>7} {:>7} {:>7} {:>7} {:>7} {:>7} {:>7} {:>7} {:>7} {:>7}'
}
INFO_BLOCOS['MERCADO DE ENERGIA TOTAL']={
    'regex':'(.{7})(.{7}) (.{7}) (.{7}) (.{7}) (.{7}) (.{7}) (.{7}) (.{7}) (.{7}) (.{7}) (.{7}) (.{7})',
	'formatacao':'{:>4}   {:>7} {:>7} {:>7} {:>7} {:>7} {:>7} {:>7} {:>7} {:>7} {:>7} {:>7} {:>7}'
}
INFO_BLOCOS['GERACAO DE USINAS NAO SIMULADAS']={
    'regex':'(.{7})(.{7}) (.{7}) (.{7}) (.{7}) (.{7}) (.{7}) (.{7}) (.{7}) (.{7}) (.{7}) (.{7}) (.{7})',
	'formatacao':'{:>4}   {:>7} {:>7} {:>7} {:>7} {:>7} {:>7} {:>7} {:>7} {:>7} {:>7} {:>7} {:>7}'
}

ADITIONAL_LINE_REGEX = [
    re.compile(r'(.{4})\|(.{10})\|(.{2})\|(.{7}) (.{7}) (.{7}) (.{7})\|(.{5}) (.{5}) (.{5}) (.{5})\|'), #coluna custo de deficit
    re.compile(r'\s*(\d+)\s+(\S+)\s+(\S+)(.*)'),# pega submercados + tipos de geracao/ limites de intercambio 
    re.compile(r'\s*(\d+)'),# pega os 999 e os submercados da energia total,
    re.compile(r'((. |\n)*?)'),# pega os /n sozinho
    
    ]

SUBMERCADOS_MAPPING={
        1:'SUDESTE',
        2:'SUL',
        3:'NORDESTE',
        4:'NORTE',
}
SUBMERCADOS_MNEMONICO={
        1:'SE',
        2:'S',
        3:'NE',
        4:'N',
}

GERACAO_MAPPING={

    'PCH':'Exp_CGH',
    'EOL':'Exp_EOL',
    'UFV':'Exp_UFV',
    'PCT':'Exp_UTE',
}


def identify_blocks(lines, titles):
    blocks = {}
    current_title = None
    current_content = []
    
    for line in lines:
        if any(title in line for title in titles):
            if current_title and current_content:
                blocks[current_title] = current_content
            current_title = line.strip()
            current_content = []
        elif current_title:
            current_content.append(line.rstrip())
    
    # Salva o último bloco
    if current_title and current_content:
        blocks[current_title] = current_content
    
    return blocks

def parse_block_with_regex(content, line_regex, additional_line_regex):
    """
    Aplica a formatação com regex nas linhas do bloco e inclui linhas adicionais.
    """
    rows = []
    for line in content:
        
        match = line_regex.match(line)
        if match:
            rows.append([group.strip() for group in match.groups()])
        else:
            for additional_regex in additional_line_regex:
                additional_match = additional_regex.match(line)
                if additional_match:
                    rows.append([*additional_match.groups(), *[''] * 10])
                    break
    return rows


def leituraArquivo(file_path):
    with open(file_path, 'r') as file:
        lines = file.readlines()

    # Identificar os blocos no arquivo
    blocks_data = identify_blocks(lines, INFO_BLOCOS.keys())
    dataframes_dict = {}
    for title, content in blocks_data.items():
        line_regex = re.compile(INFO_BLOCOS[title]['regex'])
        rows = parse_block_with_regex(content, line_regex, ADITIONAL_LINE_REGEX)
        if rows:
            df = pd.DataFrame(rows)
            dataframes_dict[title] = df.loc[1:].reset_index(drop=True)

    return dataframes_dict

def append_bloco(path_to_modify:str, values:list):

    with open(path_to_modify, 'r', encoding='iso-8859-1') as file:
        lines = file.readlines()
        values = [f"{linha}\n" for linha in values]
        lines = lines[:lines.index(' GERACAO DE USINAS NAO SIMULADAS\n')+3] + values

    with open(path_to_modify, 'w') as file:
        file.writelines(lines)
        
def sobrescreve_bloco(path_to_modify:str,bloco:str, title:str, values:list,skip_lines:int):

    alterar=False
    start=False
    count_lines=0
    with open(path_to_modify, 'r', encoding='iso-8859-1') as file:

        lines = file.readlines()
        new_lines = []
        for i, line in enumerate(lines):

            #achou o bloco
            if alterar:
                #o titulo das colunas é o inicio
                if title in line: 
                    start=True
                    #adiciona o titulo e as linhas modificadas
                    new_lines.append(line)
                    new_lines.extend([f"{linha}\n" for linha in values])

                if start:
                    #vai pulando as linhas originais do bloco
                    if count_lines <= skip_lines:
                        count_lines += 1
                        continue
                    else:
                        alterar = False
                        start=False
            
            if bloco in line: 
                alterar = True
                count_lines = 0

            new_lines.append(line)

    with open(path_to_modify, 'w') as file:
        file.writelines(new_lines)

