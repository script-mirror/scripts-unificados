import os
import pdb
import sys
import codecs


def converter_arquivo(arquivo_origem, arquivo_destino, encoding='utf-8'):
    
    with codecs.open(arquivo_origem, 'r', encoding='ISO-8859-1') as f:
        with codecs.open(arquivo_destino, 'w', encoding=encoding) as f_destino:
            for linha in f:
                f_destino.write(linha)


def printHelper():
    arq_entrada = os.path.join(os.path.dirname(__file__), 'dadger.rvx')
    arq_saida = arq_entrada.replace('dadger','dadger_conv')
    print('python {} converter_formato arq_entrada "{}" arq_saida "{}"'.format(sys.argv[0], arq_entrada, arq_saida))
    quit()


if __name__ == '__main__':

    if len(sys.argv) > 1:
        if 'help' in sys.argv or '--h' in sys.argv or '-help' in sys.argv:
            printHelper()

        
        for i in range(len(sys.argv)):
            argumento = sys.argv[i].lower()

            p_arq_converter = None
            p_arq_saida = None
            if argumento == 'arq_entrada':
                p_arq_converter = os.path.abspath(sys.argv[i+1])
                if not os.path.exists(p_arq_converter):
                    raise Exception('Arquivo nao encontrado:\n%s' %(p_arq_converter))
            elif argumento == 'arq_saida':
                p_arq_saida = os.path.abspath(sys.argv[i+1])
                dir_saida = os.path.dirname(p_arq_saida)
                if not os.path.exists(dir_saida):
                    os.makedirs(dir_saida)

    else:
        printHelper()
        
    if sys.argv[1] == 'converter_formato':
        pdb.set_trace()
        if p_arq_saida == None:
            p_arq_saida = ''.join([p_arq_converter.split('.')[0], '_conv.', p_arq_converter.split('.')[1]])
        converter_arquivo(p_arq_converter, p_arq_saida)
        print('Arquivo convertido:\n%s' %(p_arq_saida))
