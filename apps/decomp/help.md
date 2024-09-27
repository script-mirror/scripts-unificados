
## Remoção de soluções não ótimas

Coloque logo após o mnimonimo SB 
```
&
&-----------------------------------------------------------------------------------------------
&      ELIMINA DE SOLUCOES NAO OTIMAS
&       (REGISTRO TS)
&-----------------------------------------------------------------------------------------------
&TS
&   XXXXXXXXXXXXXXXX  XXXXXXXXXXXXXXXX   X
TS                                       1
&
&
&-----------------------------------------------------------------------------------------------
&      PENALIDADE DAS VARIAVEIS DE FOLGA
&       (REGISTRO PV)
&-----------------------------------------------------------------------------------------------
&PV
&   XXXXXXXXXXXXXXXXXXXX   XXXXXXXXXXXXXXXXXXXX   XXX   XXX   XXXXXXXXXXXXXXXXXXXX   XXXXXXXXXXXXXXXXXXXX
PV       16208.0               0.0100
&

```

Download de todos os arquivos necessários para a geração dos decks:
    - Baixar o deck preliminar do ons
        https://sintegre.ons.org.br/sites/9/52/Paginas/servicos/historico-de-produtos.aspx?produto=Deck%20Preliminar%20DECOMP%20-%20Valor%20Esperado

    - Baixar o ultimo gevazp
        https://sintegre.ons.org.br/sites/9/13/79/Produtos/Forms/AllItems.aspx?RootFolder=%2fsites%2f9%2f13%2f79%2fProdutos%2f237&FolderCTID=0x012000D29A893282068C45A9594DC5F39A639B#InplviewHash55c57170-723a-497f-9d27-422657ca0334=FolderCTID%3D0x012000D29A893282068C45A9594DC5F39A639B-SortField%3DModified-SortDir%3DDesc-RootFolder%3D%252Fsites%252F9%252F13%252F79%252FProdutos%252F237

    - baixar o ultimo deck no NW
        https://www.ccee.org.br/acervo-ccee

    - Baixar o ultimo compilado do pconjunto (pluvia) no prospec
        https://wx.prospec.norus.com.br/


Modificar o DADGER:
    - Comentar as seguintes restrições elétricas (bloco RE):
        restricoes_comentadas = [141, 143, 145, 147, 272, 449, 451, 453,
        464, 470, 471, 501, 503, 505, 509, 513, 515, 517, 519, 521,
        525, 527, 529, 531, 533, 535, 537, 539, 541, 543, 545, 547,
        561, 562, 564, 570, 571, 604, 606, 608, 654, 611, 612,  613,
        614, 615]

        Obs:comentar tbm os blocos ['LU','FU','FT','FI']
    
    - Mudança no GAP ()
        mudanca no valor de tolerancia para convergência (bloco GP) de 0.001 para 0.005, fazendo com que a convergência chegue mais rápido
    
    - Verificar todos os casos de "Flexibilizada para convergencia" nos blocos de HQ e retirar do deck



Modificar o DADGNL:
    - Zerar os despachos que possuem "eletrica" no comentário, até o proximo despacho com a palavra "merito" ?? 
        df_dadgnl['GL'].loc[idx_i:idx_f, 'geracao_p1'] = '0.0'
        df_dadgnl['GL'].loc[idx_i:idx_f, 'geracao_p2'] = '0.0'
        df_dadgnl['GL'].loc[idx_i:idx_f, 'geracao_p3'] = '0.0'

    - remover os despachos do estagio 1 e atualizar os despachos das semanas seguintes (estagio x + 1)
        idx_sem1 = df_dadgnl['GL'].loc[df_dadgnl['GL']['sem'].astype('int') == 1].index
        df_dadgnl['GL'] = df_dadgnl['GL'].drop(idx_sem1)
        df_dadgnl['GL']['sem'] = df_dadgnl['GL']['sem'].astype(int) - 1

    - copiar os valores de despachos da ultima semana para cada uma das usinas presentes no ultimo dadgnl no compilado do pconjunto (pluvia) no prospec

    - Atualizar os estágios do bloco TG escluindo o estágio 1

    - Atualizar as semanas do bloco GS ??

# Para quando a proxima rv não for a 0 (PMO), fazer as modificações da revisao do ONS e gerar deck no prospec com a mesma revisao que acabou de sair
Copiar os arquivos do deck decomp ons para o novo deck sem fazer nenhuma modificacao:
    . caso.dat
    . hidr.dat
    . mlt.dat
    . perdas.dat
    . polinjus.dat
    . vazoes.rv4
    . rvX  *** coloca no script automatico


    Copiar os arquivos do ultimo gevazp ons para o novo deck decomp sem fazer nenhuma modificacao:
        . modif.dat
        . postos.dat
        . regras.dat
        . vazoes.dat****


    Gerar os decks
        https://wx.prospec.norus.com.br/ProspectiveStudies/New
        Título: Gerar deck 
        Mês inicial do estudo: 
        Duração do estudo: 1 Mês
        Desmarcar a opção 'Apenas Revisão Inicial'
        subir com o 'Mês inicial do NEWAVE (4 MB)'
        subir com o Mês inicial do DECOMP (4 MB)'
        Salvar

# Quando a proxima rv for a 0 (PMO), a rv0 do ultimo deck base para o mes presente


Baixar os decks

Copiar o dadgnl alterado manualmente o valor da rv e o arquivo vazoes.dat (vindo do gevazp) antes de gerar o deck para o deck da segunda semana
Copiar o arquivo volume_uhe.csv para todas as revisoes
C:\WX2TB\Documentos\fontes\PMO\scripts_unificados\apps\decomp\arquivos\volume_uhe.csv

Para cada deck, comparar as linhas que possuem diferenças no bloco de inflexibilidade no bloco CT (DADGER). Em caso de dúvidas, pode verificar o BI https://app.powerbi.com/view?r=eyJrIjoiNjNmMzQ5MWItZTNkMy00ZjgwLThhZTYtNmYzZjZiMDZmMjVkIiwidCI6ImUxOGZmMDk0LTMyMzctNGRmOC1iNWZmLTE5ZjNlOWMyNDg3MSJ9&pageName=ReportSection

Deletar a primeira semana baixada

Deixar os dadgnl apenas da primeira RV (retirar dos PMOs)

Após comparado todos os dadgers:
    -“Enviar Estudo”
    Título: 'DC_YYYYMM_rvX_1rv' (onde XX é o mês corrente e o rvX próxima rev)   DC_202208_rv2_1rv
    Marcador: "base" e "YYYYMM" 

    DC_202210_rv1_1rv
    DC_202210_rv1_2rv
    DC_202210_rv1_4rv
    DC_202210_rv1_5rv
    DC_202210_rv1_6rv
    DC_202210_rv1_7rv
    DC_202210_rv1_8rv
    
    
    Gerar os novos decks, zip os decks da semana desejada e as anteriores com os decks nw das semanas, por exemplo:
    Gerando o deck para a rv2 de agosto de 2022:
        - DC_202208_rv2_1rv
            DC202208-sem3
            NW202208
        - DC_202208_rv2_2rv
            DC202208-sem3
            DC202208-sem4
            NW202208
        - DC_202208_rv2_4rv
            DC202208-sem3
            DC202208-sem4
            DC202209-sem1
            DC202209-sem2
            NW202208
            NW202209
        ...

[Testar] Entrar no site da tempoOK (https://www.tempook.com/) > Hidrica > Meus cenários > configurar novo cenario > +90 dias > executar meu cenário
    Modelos: 
        . ECENSav-ETA40-GEFSav
        . ECENS45av_precip
        . CFS90av_precip
        
    - Baixe os PREVS gerados renoveando todos para prevs.rvX
    - copie cada um dos prevs para cada rev gerada
    - Zipar tudo e enviar estudo, incluindo os NW (nome - Consistencia base)
    - Na hora de executar, mude o parametro "Modo de execução" para consistencia
    - selecione a instancia com o melhor custo/beneficio (com asterisco)
    - Na aba "Reaproveitar Volumes/GNL" selecione apenas para a primeira revisao uma rodada que foi rodada com o ultimo deck oficial


Após subir com os decks, atualizar o arquivo ConfigRodadaDiaria.csv com o ID dos decks do prospec
    - /WX2TB/Documentos/fontes/PMO/API_Prospec/ConfigProspecAPI/ConfigRodadaDiaria.csv

    - Atualizar as seguintes linhas (caso nao existir alguns desses estudo, não é necessário atualizar)
        prospecStudyIdToDuplicate1Rv     ;11390
        prospecStudyIdToDuplicate2Rv     ;11391
        prospecStudyIdToDuplicate4Rv     ;11392
        prospecStudyIdToDuplicate5Rv     ;11393
        prospecStudyIdToDuplicate6Rv     ;11394
        prospecStudyIdToDuplicate7Rv     ;11395
        prospecStudyIdToDuplicate8Rv     ;11312

    - Após sair o deck oficial do NW (para facilitar filtre os decks inseridos pelo admin) e modifique apenas a primeira coluna das linhas
        prospecStudyIdToAssociateCuts1Rv ;11334
        prospecStudyIdToAssociateCuts2Rv ;11334
        prospecStudyIdToAssociateCuts4Rv ;11334
        prospecStudyIdToAssociateCuts5Rv ;11334;11345
        prospecStudyIdToAssociateCuts6Rv ;11334;11345
        prospecStudyIdToAssociateCuts7Rv ;11334;11345
        prospecStudyIdToAssociateCuts8Rv ;11334;11345
        prospecStudyIdToAssociateVolumes ;11332
        . Atualizar o nome das rodadas "studyName"
        
        . Atualizar as seguintes linhas (caso nao existir alguns desses estudo, não é necessário atualizar)
            prospecStudyIdToDuplicate1Rv     ;11390
            prospecStudyIdToDuplicate2Rv     ;11391
            prospecStudyIdToDuplicate4Rv     ;11392
            prospecStudyIdToDuplicate5Rv     ;11393
            prospecStudyIdToDuplicate6Rv     ;11394
            prospecStudyIdToDuplicate7Rv     ;11395
            prospecStudyIdToDuplicate8Rv     ;11312

        . Pedir o Gilseu o numero dos estudos se o deck [SOMENTE PMO] Após sair o deck oficial do NW  (para facilitar filtre os decks inseridos pelo admin) e modifique apenas a primeira coluna das linhas
            prospecStudyIdToAssociateCuts1Rv ;11334
            prospecStudyIdToAssociateCuts2Rv ;11334
            prospecStudyIdToAssociateCuts4Rv ;
            
            prospecStudyIdToAssociateCuts5Rv ;11334;11345
            prospecStudyIdToAssociateCuts6Rv ;11334;11345
            prospecStudyIdToAssociateCuts7Rv ;11334;11345
            prospecStudyIdToAssociateCuts8Rv ;11334;11345
        
        . Atualizar com os volumes com o ultimo ID do EC-ext (perguntar o gilseu)
            prospecStudyIdToAssociateVolumes ;12845
            


###################################################################################################################
Atualização das cargas do SUL
    Quando chega a carga atualizar apenas as revisões para as próximas revisões. Para o próximo mes as revisões serão mensal, exceto se na rv0 possuir dias do mês atual, ae será uma revisao semanal para a rv0 e o restante mensal





###################################################################################################################
Atualizacao das restricoes limites elétricos
IPU60 [462]
IPU50 [461]
FZ-IN [não modelado]
Imp_SECO [não modelado]
RSE [441]
RSUL [439]
-RSE = SE-IV [443]
FSUL [437]
FMCCO [419]
FCOMC [não modelado]
FNS + FNESE [429]
FNS = GH LAJEADO GH P.ANGICAL + FC_SE [405]
FSECO [não modelado]
RSECO [não modelado]
Ger. MAD [401]
FSENE [431]
FNESE [409]
EXPN (Exp_N CA = FNE + FCOMC) [427]
RN [não modelado]
FNNE [413]* (Atraso na linha)
FNEN (-FNE) = Fluxo Nordeste - FC [415]* (Atraso na linha)
RNE [403]* (Atraso na linha)
EXPNE [417]* (Atraso na linha)
FXGET+FXGTR [447]
FXGET+FXGTR(Newave) 
FETXG+FTRXG [445]
FXGTU [457]* (Não altera)
FTUXG [455]* (Não altera)


###################################################################################################################


1) Atualizar a planilha do prospectivo
C:\Users\cs341052\OneDrive - MinhaTI\WX - Middle\Simulações\2023\202301\Estudos 2023\2 - Premissas\Planilha_prospectivo_2023_base_curvaSF.xlsx

    a - Atualizar a aba RE
    b - Atualizar a aba HQ
    c - Atualizar a aba HV (apenas em junho/julho que sai a nova curva de tucurui)
    d - Atualizar a aba CVU
        Atualizar o estrutural com CLAST
            . Copiar o bloco para uma tabela do excel
            . Filtrar os CVUs diferentes de 0
            . Ordenar por numero da usina
        Atualizar o conjutural com DADGER (Bloco CT) e lembrar de adicionar as 3 do dadgnl no final
            . Copiar o bloco CT para o Excel
            . Ordenar por numero da usina
            . Retirar os valores zerados (ultima coluna)
            . Colocar apenas os valores para o periodo 1
    e - Atualizar a ABA RE-NW
        . atualizar a probabilidade das usinas utiliazando algum relato de estudo rodado do DC no prospec, procurar pela "Produt" referente a "65% VU"


2) Atualizar a Planilha "Dados prospectivo_202x_base.xlsx"
    a - Atualizar a aba VE (baixar a ultima planilha dentro do prospec)
    b - Atualizar a aba do ano em exercício (copia da aba "Tabela Prospec" da planilha em 1)
        . Filtrar todos os minimonicos desejados (CVU, HQ, HV, RE, IA-DC)
    c - Atualizar a aba "Inflexibilidade (CT)", se necessário
    d - Não alterar a aba "Disponibilidade (CT)"


2) Baixar os decks na ccee (ultimas revisoes)
    a - NW
    b - DC
    c - Gevazp

3) Adequar os decks para subir no prospec e gerar os decks
    a - DC
        . Remover todas as flexibilizações para convergencia ("Flexibilizada para convergencia")
        . copiar os arquivos 'MODIF.DAT', 'POSTOS.DAT', 'REGRAS.DAT', 'VAZOES.DAT' do gevazp para dentro do deck
    b - NW
        . Fazer uma cópia do TERM.DAT original e zerar o TEIF e IP utilizando a aba "Zerar TEIF" da planilha 1 do deck NW

4) Gerar o deck no prospec
    a - Configurações:
        . Nome: Gerar Deck
        . Mês inicial: mesmo do ultimo deck oficial
        . Duração: Depende de sua necessidade, mas geralmente vai ser o restante do ano
        . Mês inicial do NEWAVE: O deck do NW da ccee com o TERM.DAT atualizado
        . Mês inicial do DECOMP: O deck do DC da ccee sem as flexibilizacoes para convergência
        . Facilitador de Atualização de Dados do Prospectivo: A planilha 2 Atualizada
        . Apenas Revisão Inicial: Deixar apenas os meses que deseja todas as revisões
        . Estágio mensal no 1º mês: Desabilitar para todos

5) Conferir e adequar os decks gerados
    a - Conferir se as inflex estão zeradas de acordo com a aba "Inflexibilidade (CT)" da planilha 2 para os meses que estão apenas com a revisão semanal
        Regex: ^CT +(24|25|26|27|42|47|96|97|107|171) .*
    b - Retirar as médias de RE para as viradas de meses (semana 4/5), deixando apenas para as RE 455 e 457, pois o ONS não pondera a vira de mes e o prospec sim
        Regex: "^LU.{8}5"
    c - Voltar com o TEIF e IP que foi zerado no 3-a

6) Completar os decks semanais com os decks gerados no item anterior até completar as 8 semanas. Pode utilizar os decks NW de saida do prospec

7) Rodar o consistencia

8) Gerar os prevs de 60 dias no Pluvia 

9) Para rodar o NW e conseguir os cortes, basta apagar os IDs do aproveitamento de corte da planilha de config e deixar o ecextendido rodar. Apos finalizar, insira o id do extendido para aproveitar os cortes






#primeiro deck
caso.dat
dadger.rv3
dadgnl.rv3
hidr.dat
mlt.dat
modif.dat
perdas.dat
polijus.dat
postos.dat
prevs.rv3
regras.dat
rv3
vazoes.dat

#restante dos deck
caso.dat
dadger.rv3
dadgnl.rv3
hidr.dat
mlt.dat
modif.dat
perdas.dat
polijus.dat
postos.dat
prevs.rv3
regras.dat
rv3
vazoes.dat


__________________________________________________________________________________

RE
1) Utilizar os seguintes pdfs abaixos (atualizados) como base para atualizar o ano todo 
C:\Users\cs341052\OneDrive - MinhaTI\WX - Middle\Simulações\2023\202301\Estudos 2023\2 - Premissas\
    a - RT-ONS DPL 0687_2022_Limites PMO_Janeiro-2023.pdf
    b - MINUTA - Volume 01 - Interligações.pdf
    c -  RT-ONS DPL0625-2022 - Limites e GT -Janeiro de 23 a Dezembro de 27_rev1.pdf


HQ
1) Ir na pagina:
    https://integracaoagentes.ons.org.br/FSAR-H/SitePages/Exibir_Forms_FSARH.aspx


2) Desmarcar a flag "Ativas"
3) pesquisar pelo nome do reservatório
4) Colocar em ordem de status (apenas as aceitas)
5) Procurar as que possuem o evento "PMO"


    105 - Lembrar do periodo de praia em tocantins (olhar no BI)
    118 - Lembrar do periodo de praia em tocantins (olhar no BI)



CVU
- Baixar as planilhas de CVU atualizadas
- Atualizar o deck decomp
Obs: caso o deck do decomp for liberado após ou no 5* DU, o mesmo já vai estar atualizado e não tem a necessidade de atualizar
 




############################### RE ######################################
Obs1: Não levar em consideração nda que tenha "número de máquinas" na conta
Obs2: A geração de usina hidroelétrica pode ser consultada no sistema SIAGIC do sintegre, Fazer uma média mensal dos ultimos 3 anos (sem considerar o ultimo ano)


Primeira coisa a se fazer é atualizar a carga com o ultimo deck da CCEE ou caso tiver revisao quadrimestral de carga, utilizar ela:
    Obs: Caso nao tiver o ultimo ano desejado, projetar o ultimo ano com a mesma taxa dos 2 ultimos anos
        https://www.epe.gov.br/pt/publicacoes-dados-abertos/publicacoes/revisoes-quadrimestrais-da-carga
    


RE 403 -  Recebimento NE (FNE +FSENE)
    Fonte: "RT-ONS DPL0625-2022 - Limites e GT -Janeiro de 23 a Dezembro de 27_rev1.pdf"
        - Tabela: "Limites nas Interligações Norte-Nordeste, Sudeste-Nordeste e Norte-Sudeste"
        - Coluna: RNE (Nó imp -> NE + FSENE)
    Obs: Limites conjunturais podem ser localizados no "RT-ONS DPL 0048_2023_Limites PMO_Fevereiro-2023.pdf"


RE 447 -  fluxo FC -> SE (FNS)
    * Atualizar a tabela RE 447 na aba de calculos
    Fonte: "RT-ONS DPL0625-2022 - Limites e GT -Janeiro de 23 a Dezembro de 27_rev1.pdf"
        - Tabela: "Limites Sazonalizados das Grandezas Norte → SE e do Agrupamento Norte→SE + FNS + FNESE"
        - Linhas: Limites Norte -> SE
    Obs: Possivel pegar os valores tambem o fluxo médio no NW e a patamarização


RE 405 -  fluxo FC -> SE (FNS)
    -Depende:
        . Carga
        . FXGET+FXGTR (RE 447)

    * Atualizar as tabela RE 405 na aba de calculos
        . Atualizar a carga do SIN
        . Atualizar a geração da GH Serra da mesa

    // Não achei essa parte comentada ???
    //Fonte: "RT-ONS DPL0625-2022 - Limites e GT -Janeiro de 23 a Dezembro de 27_rev1.pdf"
    //    - Tabela: "Limites nas Interligações Norte-Nordeste, Sudeste-Nordeste e Norte-Sudeste"
    //    - Coluna: Gurupi -> SMesa (FNS)
    
    Fonte: "Volume 01 - Interligações.pdf"
        - Tabela: "Tabela 6-9: Limites de FNS em função do FXGET+FXGTR após a entrada em operação das LT 500 kV Xingu – Serra Pelada C1 e C2"
    
    Fonte: "Volume 01 - Interligações.pdf"
        - Tabela: "Tabela 6-11: Condicionante em relação a geração da UHE Serra da Mesa"


RE 409 -  fluxo NE -> SE (FNESE)
    -Depende:
        . FXGET+FXGTR (RE 447)
            Fonte: "Volume 01 - Interligações.pdf"
            Tabela 7-14: Limites de FNESE em função do FXGET+FXGTR após a entrada em operação das LT 500 kV Xingu – Serra Pelada C1 e C2


        . FNXG (esse limite é irreduntante)
            Fonte: "Volume 01 - Interligações.pdf"
            Tabela 7-16: Limites de FNESE em função do FNXG após a entrada em operação da LT 500 kV Xingu – Serra Pelada C1 e C2


RE 413 -  FNE
    Fonte: "Volume 01 - Interligações.pdf"
    Tabela: "Limites nas Interligações Norte-Nordeste, Sudeste-Nordeste e Norte-Sudeste"
    Coluna: FNNE

RE 415 -  EXP_NE
    Fonte: "Volume 01 - Interligações.pdf"
    Tabela: "Limites nas Interligações Norte-Nordeste, Sudeste-Nordeste e Norte-Sudeste"
    Coluna: FNEN

RE 427 -  EXP_N CA
    Fonte: "Volume 01 - Interligações.pdf"
    Tabela: "Limites nas Interligações Norte-Nordeste, Sudeste-Nordeste e Norte-Sudeste"
    Coluna: Exp do Norte CA

RE 429 -  FNS + FNESE
    Tentar colocar o maior fluxo possível para FNSE + FNESE. Faça o cálculo da restrição levando em conta apenas o fluxo FXGET+FXGTR e a carga do SIN (Tabela 6-17: Limites de FNS+FNESE em função do FXGET+FXGTR após a entrada em operação da LT 500 kV Xingu – Serra Pelada C1 e C2) levando em consideração a geração de serra da mesa. Após isso tentar encontrar o o maior fluxo FNXG podendo fazer correção manualmente para patamares próximos.
    -Depende:
        . Carga
        . FXGET+FXGTR (RE 447)

RE 437
    Fonte: "RT-ONS DPL 0420-2023 - Limites e GT -Setembro de 23 a Dezembro de 27.pdf"
    Tabela: Tabela 5-2: Limites nas Interligações Sul - Sudeste (MW)
    Coluna: FSUL

RE 439
    - FBTA: não é levado em consideração
    - Geração de itaipu, verificar no IPDO, pego no SAGIC (ITAIPU 60 HZ-PR)

RE 441: Somatório dos fluxos nos elos CC de furnas, xingu e madeira ?
    elos CC de furnas: geracao 50 hz itaipu no sagic (ITAIPU 50 HZ-PY)
    xingu: Bipolos (RE 447 Fluxo Norte -> SE )
    madeira:  RE 401

RE 443: É constante em todos os meses/patamares em 6500

RE 461 e 462: 
    Deixamos com a geracao minima igial a 0 e a máxima igual a 7000 pois ela é calculada calculado no bloco RI

RE 455:
    Carga de AMAPA + MANAUS: (Planilha rvisao quadrimestral do EPE)
        . https://www.epe.gov.br/pt/publicacoes-dados-abertos/publicacoes/revisoes-quadrimestrais-da-carga
    PQ: Deck, mas nao sofre muito ajuste entao nao precisa se preocupar
    Ajustes: média calculada pelo Gilseu, não é muito alterado


RE 417
RE 437

=================================================================
Carga do SIN

403
405 (depende: 447)
409
413
415
417
427
429
437
439
441 ????? Não consegui 
443 ????? Não consegui
447
461
462
455 ????? O bloco PQ não possui os dados de geração para o ano todo


HQ 68 a vazao superior estava em 1700
HQ 85 no ultimo deck tem apenas o limite inferior, mas possui uma FSAR-H 396 que coloca uma maxima
HQ 105 Verificar a resolução da ana
HQ 216 Verificar novamente


