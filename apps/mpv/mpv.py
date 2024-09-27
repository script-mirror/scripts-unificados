import os
import sys
import datetime

path_modulo = os.path.abspath(__file__)
path_app = os.path.dirname(path_modulo)
path_libs = os.path.abspath(os.path.join(path_app, 'libs'))

path_apps = os.path.dirname(path_app)
path_raiz = os.path.dirname(path_apps)
path_libs_universal = os.path.abspath(os.path.join(path_raiz, 'bibliotecas'))

sys.path.insert(1, path_libs_universal)
import wx_manipulaArqCpins

sys.path.insert(1, path_libs)
import mpv_metodos


def rodar_mpv_completo(data):
    mpv_metodos.mpv_saar(data, ['Sao_Francisco'])

def printHelper():
    print("python {} data {}".format(sys.argv[0], data.strftime('%d/%m/%Y')))

if __name__ == '__main__':
    now = datetime.datetime.now()
    data = datetime.datetime.strptime(now.strftime('%Y%m%d'), '%Y%m%d')

    if len(sys.argv) > 1:
        
        if 'help' in sys.argv or '-h' in sys.argv or '-help' in sys.argv:
            printHelper()
            quit()

        for i_argumento in range(1, len(sys.argv)):
            argumento = sys.argv[i_argumento].lower()

            if argumento == 'data':
                try:
                    data = datetime.datetime.strptime(sys.argv[i_argumento+1], "%d/%m/%Y")
                except:
                    print("Erro ao tentar converter {} em data!".format(data))
                    quit()

        rodar_mpv_completo(data)

    else:
        printHelper()
