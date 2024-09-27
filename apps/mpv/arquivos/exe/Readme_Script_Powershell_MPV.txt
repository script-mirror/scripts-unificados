O script deve ter seu formato ".ps1", e ser colocado no mesmo local que os arquivos a remover/modificar, pois é nesse path que ele se baseia.
(Mesmo local onde está a dll: "ONS.MPV.ConsoleApplication")

Abrir o PowerShell e executar o comando, segundo o local acima, segue exemplo:
Ex: C:\_Projetos\ONS\MPV\ONS.MPV.ConsoleApplication\bin\Debug\net5.0\Script_Powershell_MPV.ps1 3 4

onde 3 e 4, ao final, são os parametros que indicam que calculo deseja REMOVER.

public enum TiposCalculos
{
    SimplesDefasagem = 1,
    Muskingum = 2,
    SSARR = 3,
    Todini = 4
}
