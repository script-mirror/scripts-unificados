import os
import re
import sys
import pdb
import codecs
import datetime
import pandas as pd

sys.path.insert(1,"/WX2TB/Documentos/fontes/")
from PMO.scripts_unificados.bibliotecas import wx_dbLib


info_blocos = {}
info_blocos['TE'] = {'campos':['mnemonico',
                            'comentario',
                        ],
                        'regex':'(.{2})  (.*)(.*)',
                        'formatacao':'{:>2}  {}'}

info_blocos['SB'] = {'campos':['mnemonico',
                            'cod',
                            'sub',
                        ],
                        'regex':'(.{2})  (.{2})   (.{1,})(.*)',
                        'formatacao':'{:>2}  {:>2}   {:<2}'}

info_blocos['UH'] = {'campos':[
				'mnemonico',
				'uhe',
				'ree',
				'vini',
				'defmin',
				'evap',
				'oper',
				'vmortoini',
				'limsup',
				'fator',
			],
			'regex':'(.{2})  (.{3})  (.{2})   (.{10})(.{10})     (.{1}) {0,4}(.{0,2}) {0,3}(.{0,10})(.{0,10})(.{0,1})(.*)',
			'formatacao':'{:>2}  {:>3}  {:>2}   {:>10}{:>10}     {:>1}    {:>2}   {:>10}{:>10}{:>1}'}

info_blocos['CT'] = {'campos':[
                                'mnemonico',
                                'cod',
                                'sub',
                                'nome_usin',
                                'estagio',
                                'infl_p1',
                                'disp_p1',
                                'cvu_p1',
                                'infl_p2',
                                'disp_p2',
                                'cvu_p2',
                                'infl_p3',
                                'disp_p3',
                                'cvu_pat3',
                ],
                'regex':'(.{2})  (.{3})  (.{2})   (.{10})(.{2})   (.{5})(.{5})(.{10})(.{5})(.{5})(.{10})(.{5})(.{5})(.{10})(.*)',
                'formatacao':'{:>2}  {:>3}  {:>2}   {:>10}{:>2}   {:>5}{:>5}{:>10}{:>5}{:>5}{:>10}{:>5}{:>5}{:>10}'}

info_blocos['UE'] = {'campos':[
                                'mnemonico',
                                'num',
                                'sub',
                                'nome',
                                'mont',
                                'jus',
                                'bomb_min',
                                'bomb_max',
                                'tax_cons',
                        ],
                        'regex':'(.{2})  (.{3})  (.{2})   (.{12})   (.{3})  (.{3})  (.{10})(.{10})(.{10})(.*)',
                        'formatacao':'{:>2}  {:>3}  {:>2}   {:>12}   {:>3}  {:>3}  {:>10}{:>10}{:>10}'}

info_blocos['DP'] = {'campos':[
                                'mnemonico',
                                'ip',
                                'sub',
                                'pat',
                                'mwmed_p1',
                                'horas_p1',
                                'mwmed_p2',
                                'horas_p2',
                                'mwmed_p3',
                                'horas_p3',
                        ],
                        'regex':'(.{2})  (.{2})   (.{2})  (.{3})   (.{010})(.{10})(.{10})(.{10})(.{10})(.{10})(.*)',
                        'formatacao':'{:>2}  {:>2}   {:>2}  {:>3}   {:>10}{:>10}{:>10}{:>10}{:>10}{:>10}'}
info_blocos['CD'] = {'campos':[
                                'mnemonico',
                                'num',
                                'sub',
                                'nome',
                                'ind',
                                'limsp_p1',
                                'custo_p1',
                                'limsp_p2',
                                'custo_p2',
                                'limsp_p3',
                                'custo_p3',
                        ],
                        'regex':'(.{2})  (.{2})   (.{2})   (.{10})(.{2})   (.{5})(.{10})(.{5})(.{10})(.{5})(.{10})(.*)',
                        'formatacao':'{:>2}  {:>2}   {:>2}   {:>10}{:>2}   {:>5}{:>10}{:>5}{:>10}{:>5}{:>10}'}

info_blocos['BE'] = {'campos':[
                                'mnemonico',
                                'nome',
                                'sub',
                                'estag_ger',
                                'gerac_p1',
                                'gerac_p2',
                                'gerac_p3',
                                'earm',
                                'earmx',
                                'eaf',
                        ],
                        'regex':'(.{2})  (.{10})(.{2})   (.{3})  (.{5})(.{5})(.{5})(.{0,5})(.{0,5})(.{0,5})(.*)',
                        'formatacao':'{:>2}  {:>10}{:>2}   {:>3}  {:>5}{:>5}{:>5}{:>5}{:>5}{:>5}'}

info_blocos['PQ'] = {'campos':[
                                'mnemonico',
                                'nome',
                                'sub',
                                'estagio',
                                'gerac_p1',
                                'gerac_p2',
                                'gerac_p3',
                        ],
                        'regex':'(.{2})  (.{11})(.{1})   (.{2})   (.{5})(.{5})(.{5})(.*)',
                        'formatacao':'{:>2}  {:>11}{:>1}   {:>2}   {:>5}{:>5}{:>5}'}


info_blocos['RI'] = {'campos':[
                                'mnemonico',
                                'uhe',
                                'estagio',
                                'sub',
                                'min60_p1',
                                'max60_p1',
                                'min50_p1',
                                'max50_p1',
                                'ande_p1',
                                'min60_p2',
                                'max60_p2',
                                'min50_p2',
                                'max50_p2',
                                'ande_p2',
                                'min60_p3',
                                'max60_p3',
                                'min50_p3',
                                'max50_p3',
                                'ande_p3',
                        ],
                        'regex':'(.{2})  (.{3})   (.{1})   (.{1}) (.{7})(.{7})(.{7})(.{7})(.{7})(.{7})(.{7})(.{7})(.{7})(.{7})(.{7})(.{7})(.{7})(.{7})(.{7})(.*)',
                        'formatacao':'{:>2}  {:>3}   {:>1}   {:>1} {:>7}{:>7}{:>7}{:>7}{:>7}{:>7}{:>7}{:>7}{:>7}{:>7}{:>7}{:>7}{:>7}{:>7}{:>7}'}

info_blocos['IA'] = {'campos':[
                                'mnemonico',
                                'estagio',
                                's1',
                                's2',
                                'de_para_p1',
                                'para_de_p1',
                                'de_para_p2',
                                'para_de_p2',
                                'de_para_p3',
                                'para_de_p3',
                        ],
                        'regex':'(.{2})  (.{2})   (.{2})   (.{2})   (.{10})(.{10})(.{10})(.{10})(.{10})(.{10})(.*)',
                        'formatacao':'{:>2}  {:>2}   {:>2}   {:>2}   {:>10}{:>10}{:>10}{:>10}{:>10}{:>10}'}

info_blocos['RC'] = {'campos':[
                                'mnemonico',
                                'escada',
                        ],
                        'regex':'(.{2})  (.{6})(.*)',
                        'formatacao':'{:>2}  {:>6}'}

info_blocos['TX'] = {'campos':[
                                'mnemonico',
                                'valor',
                        ],
                        'regex':'(.{2})  (.{5})(.*)',
                        'formatacao':'{:>2}  {:>5}'}

info_blocos['GP'] = {'campos':[
                                'mnemonico',
                                'valor',
                        ],
                        'regex':'(.{2})  (.{10})(.*)',
                        'formatacao':'{:>2}  {:>10}'}

info_blocos['NI'] = {'campos':[
                                'mnemonico',
                                'valor',
                        ],
                        'regex':'(.{2})  (.{3})(.*)',
                        'formatacao':'{:>2}  {:>3}'}

info_blocos['PD'] = {'campos':[
                                'mnemonico',
                                'algoritimo',
                        ],
                        'regex':'(.{2})  (.{6})(.*)',
                        'formatacao':'{:>2}  {:>6}'}

info_blocos['DT'] = {'campos':[
                                'mnemonico',
                                'dia',
                                'mes',
                                'ano',
                        ],
                        'regex':'(.{2})  (.{2})   (.{2})   (.{4})(.*)',
                        'formatacao':'{:>2}  {:>2}   {:>2}   {:>4}'}

info_blocos['MP'] = {'campos':[
                                'mnemonico',
                                'uh',
                                'hertz',
                                'f1',
                                'f2',
                                'f3',
                                'f4',
                                'f5',
                                'f6',
                                'f7',
                                'f8',
                                'f9',
                                'f10',
                                'f11',
                                'f12',
                        ],
                        'regex':'(.{2})  (.{3})(.{2})(.{5})(.{5})(.{0,5})(.{0,5})(.{0,5})(.{0,5})(.{0,5})(.{0,5})(.{0,5})(.{0,5})(.{0,5})(.{0,5})(.*)',
                        'formatacao':'{:>2}  {:>3}{:>2}{:>5}{:>5}{:>5}{:>5}{:>5}{:>5}{:>5}{:>5}{:>5}{:>5}{:>5}{:>5}'}

info_blocos['FD'] = {'campos':[
                                'mnemonico',
                                'uh',
                                'hertz',
                                'f1',
                                'f2',
                                'f3',
                                'f4',
                                'f5',
                                'f6',
                                'f7',
                                'f8',
                                'f9',
                                'f10',
                                'f11',
                                'f12',
                                'f13',
                                'f14',
                                'f15',
                                'f16',
                                'f17',
                        ],
                        'regex':'(.{2})  (.{3})(.{2})(.{5})(.{5})(.{0,5})(.{0,5})(.{0,5})(.{0,5})(.{0,5})(.{0,5})(.{0,5})(.{0,5})(.{0,5})(.{0,5})(.{0,5})(.{0,5})(.{0,5})(.{0,5})(.{0,5})(.*)',
                        'formatacao':'{:>2}  {:>3}{:>2}{:>5}{:>5}{:>5}{:>5}{:>5}{:>5}{:>5}{:>5}{:>5}{:>5}{:>5}{:>5}{:>5}{:>5}{:>5}{:>5}{:>5}'}

info_blocos['VE'] = {'campos':[
                                'mnemonico',
                                'uh',
                                'f1',
                                'f2',
                                'f3',
                                'f4',
                                'f5',
                                'f6',
                                'f7',
                                'f8',
                                'f9',
                                'f10',
                                'f11',
                                'f12',
                                'f13',
                                'f14',
                                'f15',
                                'f16',
                                'f17',
                            ],
                            'regex':'(.{2})  (.{3})  (.{5})(.{5})(.{0,5})(.{0,5})(.{0,5})(.{0,5})(.{0,5})(.{0,5})(.{0,5})(.{0,5})(.{0,5})(.{0,5})(.{0,5})(.{0,5})(.{0,5})(.{0,5})(.{0,5})(.*)',
                            'formatacao':'{:>2}  {:>3}  {:>5}{:>5}{:>5}{:>5}{:>5}{:>5}{:>5}{:>5}{:>5}{:>5}{:>5}{:>5}{:>5}{:>5}{:>5}{:>5}{:>5}'}

# RE, LU, FU, FT, FI, FE
info_blocos['RE'] = {'campos':[
                                'mnemonico',
                                'id_restricao',
                                'estag_inicial',
                                'estag_final',
                        ],
                        'regex':'(.{2})  (.{4}) (.{2})   (.{2})(.*)',
                        'formatacao':'{:>2}  {:<4} {:>2}   {:>2}'}

info_blocos['LU'] = {'campos':[
                                'mnemonico',
                                'id_restricao',
                                'est',
                                'gmin_p1',
                                'gmax_p1',
                                'gmin_p2',
                                'gmax_p2',
                                'gmin_p3',
                                'gmax_p3',
                        ],
                        'regex':'(.{2})  (.{4}) (.{2})   (.{10})(.{10})(.{10})(.{10})(.{0,10})(.{0,10})(.*)',
                        'formatacao':'{:>2}  {:<4} {:>2}   {:>10}{:>10}{:>10}{:>10}{:>10}{:>10}'}

info_blocos['FU'] = {'campos':[
                                'mnemonico',
                                'id_restricao',
                                'est',
                                'uh',
                                'fator',
                                'freq_itaipu',
                        ],
                        'regex':'(.{2})  (.{4}) (.{2})   (.{3})  (.{10}) {0,1}(.{0,2})(.*)',
                        'formatacao':'{:>2}  {:<4} {:>2}   {:>3}  {:>10} {:>2}'}

info_blocos['FT'] = {'campos':[
                                'mnemonico',
                                'id_restricao',
                                'est',
                                'ut',
                                'sub',
                                'fator',
                        ],
                        'regex':'(.{2})  (.{4}) (.{2})   (.{3})  (.{2})   (.{10})(.*)',
                        'formatacao':'{:>2}  {:<4} {:>2}   {:>3}  {:>2}   {:>10}'}

info_blocos['FI'] = {'campos':[
                                'mnemonico',
                                'id_restricao',
                                'estagio',
                                'sub_de',
                                'sub_para',
                                'fator',
                        ],
                        'regex':'(.{2})  (.{4}) (.{2})   (.{2})   (.{2})   (.{10})(.*)',
                        'formatacao':'{:>2}  {:<4} {:>2}   {:>2}   {:>2}   {:>10}'}

info_blocos['FE'] = {'campos':[
                                'mnemonico',
                                'id_restricao',
                                'est',
                                'ci_ce',
                                'sub',
                                'fator',
                        ],
                        'regex':'(.{2})  (.{4}) (.{2})   (.{3})  (.{2})   (.{10})(.*)',
                        'formatacao':'{:>2}  {:>4} {:>2}   {:>3}  {:>2}   {:>10}'}

info_blocos['VI'] = {'campos':[
                                'mnemonico',
                                'usi',
                                'dur',
                                'qdef1',
                                'qdef2',
                                'qdef3',
                                'qdef4',
                                'qdef5',
                                'qdef6',
                                'qdef7',
                                'qdef8',
                                'qdef9',
                        ],
                        'regex':'(.{2})  (.{3})  (.{3})  (.{5})(.{5})(.{5})(.{5})(.{5})(.{0,5})(.{0,5})(.{0,5})(.{0,5})(.*)',
                        'formatacao':'{:>2}  {:>3}  {:>3}  {:>5}{:>5}{:>5}{:>5}{:>5}{:>5}{:>5}{:>5}{:>5}'}


info_blocos['AC'] = {'campos':[
                                'mnemonico',
                                'uhe',
                                'parametro_modificado',
                                'valor',
                                'mes',
                                'semana',
                                'ano',
                            ],
                            'regex':'(.{2})  (.{3})  (.{6}) {0,4}(.{0,50})(.{0,3}) {0,2}(.{0,1}) {0,1}(.{0,4})(.*)',
                            'formatacao':'{:>2}  {:>3}  {:>6}    {:<50}{:>3}  {:>1} {:>4}'}

info_blocos['RV'] = {'campos':[
                                'mnemonico',
                                'rv',
                                'esti',
                                'estf',
                        ],
                        'regex':'(.{2})  (.{7})   (.{1})    (.{1})(.*)',
                        'formatacao':'{:>2}  {:>7}   {:>1}    {:>1}'}

info_blocos['FP'] = {'campos':[
                                'mnemonico',
                                'usi',
                                'iper',
                                'tp',
                                'npt',
                                'qmin',
                                'qmax',
                                'tp',
                                'npt',
                                'vmin',
                                'vmax',
                                'ghmin',
                                'ghmax',
                                'tol',
                                'flgd',
                                'tp',
                                'percemt_n1',
                                'percemt_n2',
                                'ni',
                                'verif',
                        ],
                        'regex':'(.{2})  (.{3})  (.{3})  (.{1}) (.{4}) (.{5}) (.{5})  (.{1}) (.{4}) (.{5}) (.{5})  (.{5}) (.{5}) (.{3})  (.{1})    (.{1}) (.{5}) (.{5}) (.{2})   (.{1})(.*)',
                        'formatacao':'{:>2}  {:>3}  {:>3}  {:>1} {:>4} {:>5} {:>5}  {:>1} {:>4} {:>5} {:>5}  {:>5} {:>5} {:>3}  {:>1}    {:>1} {:>5} {:>5} {:>2}   {:>1}'}

info_blocos['IR'] = {'campos':[
                                'mnemonico',
                                'arq_saida',
                                'estagio_limite',
                                'lim_linhas_pags',
                        ],
                        'regex':'(.{2})  (.{1,7}) {0,}(.{0,2}) {0,}(.{0,2})(.*)',
                        'formatacao':'{:>2}  {:<7}   {:>2}   {:>2}'}

info_blocos['CI'] = {'campos':[
                                'mnemonico',
                                'num_contrato',
                                'sub',
                                'nome',
                                'est',
                                'linf_p1',
                                'lsup_p1',
                                'custo_p1',
                                'linf_p2',
                                'lsup_p2',
                                'custo_p2',
                                'linf_p3',
                                'lsup_p3',
                                'custo_p3',
                        ],
                        'regex':'(.{2})  (.{3}) (.{2}) (.{10})   (.{2})   (.{5})(.{5})(.{10})(.{5})(.{5})(.{10})(.{5})(.{5})(.{10})(.*)',
                        'formatacao':'{:>2}  {:>3} {:>2} {:>10}   {:>2}   {:>5}{:>5}{:>10}{:>5}{:>5}{:>10}{:>5}{:>5}{:>10}'}

info_blocos['RS'] = {'campos':[
                                'mnemonico',
                                'arq_defl_passadas',
                                'arq_saida',
                        ],
                        'regex':'(.{2})  (.{5})     (.{59})(.*)',
                        'formatacao':'{:>2}  {:>5}     {:>59}'}

info_blocos['FC'] = {'campos':[
                                'mnemonico',
                                'arq_inf',
                                'nome_arquivo',
                        ],
                        'regex':'(.{2})  (.{6})    (.{0,48})(.*)',
                        'formatacao':'{:>2}  {:>6}    {:<48}'}

info_blocos['TI'] = {'campos':[
                                'mnemonico',
                                'uhe',
                                'estg1',
                                'estg2',
                                'estg3',
                                'estg4',
                                'estg5',
                                'estg6',
                        ],
                        'regex':'(.{2})  (.{3})  (.{5})(.{5})(.{0,5})(.{0,5})(.{0,5})(.{0,5})(.*)',
                        'formatacao':'{:>2}  {:>3}  {:>5}{:>5}{:>5}{:>5}{:>5}{:>5}'}

info_blocos['RQ'] = {'campos':[
                                'mnemonico',
                                'reservatorio_eq',
                                'estg1',
                                'estg2',
                                'estg3',
                                'estg4',
                                'estg5',
                                'estg6',
                        ],
                        'regex':'(.{2})  (.{2})   (.{5})(.{5})(.{0,5})(.{0,5})(.{0,5})(.{0,5})(.*)',
                        'formatacao':'{:>2}  {:>2}   {:>5}{:>5}{:>5}{:>5}{:>5}{:>5}'}

info_blocos['EZ'] = {'campos':[
                                'mnemonico',
                                'uhe',
                                'vutil',
                        ],
                        'regex':'(.{2})  (.{3})  (.{5})(.*)',
                        'formatacao':'{:>2}  {:>3}  {:>5}'}

info_blocos['HV'] = {'campos':[
                                'mnemonico',
                                'id_restricao',
                                'estg_i',
                                'estg_f',
                        ],
                        'regex':'(.{2})  (.{3})  (.{2})   (.{2})(.*)',
                        'formatacao':'{:>2}  {:>3}  {:>2}   {:>2}'}

info_blocos['LV'] = {'campos':[
                                'mnemonico',
                                'id_restricao',
                                'num_estag',
                                'lim_inf',
                                'lim_supf',
                        ],
                        'regex':'(.{2})  (.{3})  (.{2})   (.{10})(.{0,10})(.*)',
                        'formatacao':'{:>2}  {:>3}  {:>2}   {:>10}{:>10}'}

info_blocos['CV'] = {'campos':[
                                'mnemonico',
                                'id_restricao',
                                'num_estag',
                                'uhe_ue',
                                'coef_hv',
                                'tipo_rest',
                        ],
                        'regex':'(.{2})  (.{3})  (.{2})   (.{3})  (.{10})     (.{4})(.*)',
                        'formatacao':'{:>2}  {:>3}  {:>2}   {:>3}  {:>10}     {:>4}'}

info_blocos['HQ'] = {'campos':[
                                'mnemonico',
                                'id_restricao',
                                'estag_inicial',
                                'estag_final',
                        ],
                        'regex':'(.{2})  (.{3})  (.{2})   (.{2})(.*)',
                        'formatacao':'{:>2}  {:>3}  {:>2}   {:>2}'}

info_blocos['LQ'] = {'campos':[
                                'mnemonico',
                                'id_restricao',
                                'est',
                                'lim_inf_p1',
                                'lim_sup_p1',
                                'lim_inf_p2',
                                'lim_sup_p2',
                                'lim_inf_p3',
                                'lim_sup_p3',
                        ],
                        'regex':'(.{2})  (.{3})  (.{2})   (.{10})(.{10})(.{10})(.{10})(.{10})(.{0,10})(.*)',
                        'formatacao':'{:>2}  {:>3}  {:>2}   {:>10}{:>10}{:>10}{:>10}{:>10}{:>10}'}

info_blocos['CQ'] = {'campos':[
                                'mnemonico',
                                'id_restricao',
                                'est',
                                'uhe',
                                'coef',
                                'tipo_restricao',
                        ],
                        'regex':'(.{2})  (.{3})  (.{2})   (.{3})  (.{10})     (.{4})(.*)',
                        'formatacao':'{:>2}  {:>3}  {:>2}   {:>3}  {:>10}     {:>4}'}

info_blocos['AR'] = {'campos':[
				'mnemonico',
				'est',
				'lamb',
				'alfa',
			],
			'regex':'(.{2})   (.{3}) {0,}(.{0,5}) {0,}(.{0,5})(.*)',
			'formatacao':'{:>2}   {:>3}   {:>5} {:>5}'}

info_blocos['EV'] = {'campos':[
				'mnemonico',
				'modelo',
				'volume_referencia',
			],
			'regex':'(.{2})  (.{1})    (.{3})(.*)',
			'formatacao':'{:>2}  {:>1}    {:>3}'}

info_blocos['FJ'] = {'campos':[
				'mnemonico',
				'nome_arquivo',
			],
			'regex':'(.{2})  (.{12})(.*)',
			'formatacao':'{:>2}  {:>12}'}


info_blocos['HE'] = {'campos':[
				'mnemonico',
				'id_restricao',
				'tipo',
				'limite_inf_ear',
				'estag',
				'penalid',
				'flag_produtiv',
				'flag_tipo_valores',
				'flag_tratamento',
				'nome_arquivo',
				'flag_tolerancia',
			],
			'regex':'(.{2})  (.{3})  (.{1})    (.{10}) (.{2}) (.{10}) (.{1}) (.{1}) (.{1}) {0,}(.{0,60}) {0,1}(.{0,1})(.*)',
			'formatacao':'{:>2}  {:>3}  {:>1}    {:>10} {:>2} {:>10} {:>1} {:>1} {:>1} {:>60} {:>1}'}

info_blocos['CM'] = {'campos':[
				'mnemonico',
				'id_restricao',
				'indice_ree',
				'coef',
			],
			'regex':'(.{2})  (.{3})  (.{3})  (.{10})(.*)',
			'formatacao':'{:>2}  {:>3}  {:>3}  {:>10}'}

info_blocos['CM'] = {'campos':[
				'mnemonico',
				'id_restricao',
				'indice_ree',
				'coef',
			],
			'regex':'(.{2})  (.{3})  (.{3})  (.{10})(.*)',
			'formatacao':'{:>2}  {:>3}  {:>3}  {:>10}'}

info_blocos['VL'] = {'campos':[
                'mnemonico',
                'id',
                'fator',
                'coef0',
                'coef1',
                'coef2',
                'coef3',
                'coef4',
            ],
            'regex':'(.{2})  (.{4})  (.{1,15}) {0,}(.{0,15}) {0,}(.{0,15}) {0,}(.{0,15}) {0,}(.{0,15}) {0,}(.{0,15})(.*)',
            'formatacao':'{:>2}  {:<4}  {:<15} {:<15} {:<15} {:<15} {:<15} {:<15}'}

info_blocos['VU'] = {'campos':[
                'mnemonico',
                'id',
                'id_uh_incluenciadora',
                'fator',
            ],
            'regex':'(.{2})  (.{4})  (.{4})  (.{1,15})(.*)',
            'formatacao':'{:>2}  {:>4}  {:>4}  {:<15}'}

info_blocos['VA'] = {'campos':[
                'mnemonico',
                'id',
                'id_uh',
                'fator',
            ],
            'regex':'(.{2})  (.{4})  (.{4})  (.{1,15})(.*)',
            'formatacao':'{:>2}  {:>4}  {:>4}  {:>15}'}

def leituraArquivo(filePath):

    file = open(filePath, 'r', encoding='latin-1')
    
    arquivo = file.readlines()
    file.close()

    coment = []
    comentarios = {}
    blocos = {}
    for iLine in range(len(arquivo)):
        line = arquivo[iLine]
        if line[0] == '&':
            coment.append(line)
        elif line[0].strip() == '':
            continue
        else:
            mnemonico = line.split()[0]
            if mnemonico not in info_blocos:
                print(mnemonico)
                continue
                
            infosLinha = re.split(info_blocos[mnemonico]['regex'], line)
            if len(infosLinha) < 2:
                print(mnemonico)
                continue

            if mnemonico not in blocos:
                blocos[mnemonico] = []
                comentarios[mnemonico] = {}
                
            if len(coment) > 0:
                comentarios[mnemonico][len(blocos[mnemonico])] = coment
                coment = []
            
            blocos[mnemonico].append(infosLinha[1:-2])   # ultimo termo da lista e o que sobra da expressao regex (/n)
        
    if len(coment) > 0:
        comentarios[mnemonico][len(blocos[mnemonico])] = coment
        
    df_dadger = {}
    for mnemonico in blocos:
        df_dadger[mnemonico] = pd.DataFrame(blocos[mnemonico], columns=info_blocos[mnemonico]['campos'])
    
    return df_dadger, comentarios
            

# Bloco de restricoes possui um tratamento diferente do restante dos blocos
def escrever_bloco_restricoes(fileOut, df_dadger, mnemonico_restricao, submnemonicos_restricao, comentarios):
    
    if mnemonico_restricao == 'HE':
        for index, row in df_dadger[mnemonico_restricao].iterrows():
            if index in comentarios[mnemonico_restricao]:
                for coment in comentarios[mnemonico_restricao][index]:
                    fileOut.write(coment)
            fileOut.write('{}\n'.format(info_blocos[mnemonico_restricao]['formatacao'].format(*row.values).strip()))
            
            restricoes_mesma_rhe = df_dadger[mnemonico_restricao].loc[df_dadger[mnemonico_restricao]['id_restricao'] == row['id_restricao']]
            
            # Somente escreve o bloco CM se for a ultima restricao HE para aquela rhe
            if row.name == restricoes_mesma_rhe.iloc[-1].name:
                id_restr = int(row['id_restricao'])
                for mnemon in ['CM']:
                    restricoes_mnemon = df_dadger[mnemon].loc[df_dadger[mnemon]['id_restricao'].astype('int') == id_restr]
                    for index, row in restricoes_mnemon.iterrows():
                        if index in comentarios[mnemon]:
                            for coment in comentarios[mnemon][index]:
                                fileOut.write(coment)
                        fileOut.write('{}\n'.format(info_blocos[mnemon]['formatacao'].format(*row.values).strip()))

        # Escrita do ultimo comentário, se existir
        if index+1 in comentarios[mnemon]:
            for coment in comentarios[mnemon][index+1]:
                fileOut.write(coment)
    
    else:
        for index, row in df_dadger[mnemonico_restricao].iterrows():
        
            if index in comentarios[mnemonico_restricao]:
                for coment in comentarios[mnemonico_restricao][index]:
                    fileOut.write(coment)
            fileOut.write('{}\n'.format(info_blocos[mnemonico_restricao]['formatacao'].format(*row.values).strip()))
            id_restr = int(row['id_restricao'])
            
            for mnemon in submnemonicos_restricao:
                restricoes_mnemon = df_dadger[mnemon].loc[df_dadger[mnemon]['id_restricao'].astype('int') == id_restr]
                
                for index, row in restricoes_mnemon.iterrows():
                    if index in comentarios[mnemon]:
                        for coment in comentarios[mnemon][index]:
                            fileOut.write(coment)
                    fileOut.write('{}\n'.format(info_blocos[mnemon]['formatacao'].format(*row.values).strip()))


def escrever_dadger(df_dadger, comentarios, filePath):
    
    blocos_restricoes = {}
    blocos_restricoes['RE'] = ['LU', 'FU', 'FT', 'FI']
    blocos_restricoes['HQ'] = ['LQ', 'CQ']
    blocos_restricoes['HV'] = ['LV', 'CV']
    blocos_restricoes['HE'] = ['CM']
    
    # # Aproveitando a funcao de restricao para inserir a Influência de 
    # # vazões laterais
    # blocos_restricoes['VL'] = ['VU']
    
    bloco_dependentes = {}
    bloco_dependentes['VL'] = ['VU']
    
    
    blocos_infos_restricoes = []
    for mnemonico_rest in blocos_restricoes:
        blocos_infos_restricoes += blocos_restricoes[mnemonico_rest]

    fileOut = codecs.open(filePath, 'a+', 'utf-8')
    for mnemonico in df_dadger:
        
        if mnemonico in blocos_restricoes:
            escrever_bloco_restricoes(fileOut, df_dadger, mnemonico, blocos_restricoes[mnemonico], comentarios)

        elif mnemonico in blocos_infos_restricoes:
            continue
        
        else:
            for index, row in df_dadger[mnemonico].iterrows():
                if index in comentarios[mnemonico]:
                    for coment in comentarios[mnemonico][index]:
                        fileOut.write(coment)
                fileOut.write('{}\n'.format(info_blocos[mnemonico]['formatacao'].format(*row.values).strip()))
                
                # 
                if mnemonico in bloco_dependentes:
                    for dep in bloco_dependentes[mnemonico]:
                        
                        mnemon_depend = df_dadger[dep].loc[df_dadger[dep]['id'].astype('int') == int(row['id'])]
                        df_dadger[dep].drop(mnemon_depend.index, inplace=True)
                
                        for index, row in mnemon_depend.iterrows():
                            if index in comentarios[dep]:
                                for coment in comentarios[dep][index]:
                                    fileOut.write(coment)
                            fileOut.write('{}\n'.format(info_blocos[dep]['formatacao'].format(*row.values).strip()))

    fileOut.close()
    print(filePath)
    return filePath

import os

def comentar_dadger_ons_ccee(df_dadger, comentarios):
    
    restricoes_comentadas = [141, 143, 145, 147, 272, 449, 451, 453,
        464, 470, 471, 501, 503, 505, 509, 513, 515, 517, 519, 521,
        525, 527, 529, 531, 533, 535, 537, 539, 541, 543, 545, 547,
        561, 562, 564, 570, 571, 604, 606, 608, 654, 611, 612,  613,
        614, 615]
    
    df_restricoes_comentadas = df_dadger['RE'].loc[df_dadger['RE']['id_restricao'].astype('int').isin(restricoes_comentadas)]
    for index, row in df_restricoes_comentadas.iterrows():
        
        restricao_comentada = []
        
        if index in comentarios['RE']:
            restricao_comentada = comentarios['RE'][index]
        
        restricao_comentada += ['&{}\n'.format(info_blocos['RE']['formatacao'].format(*row.values).strip())]

        for mnemon_info_rest in ['LU','FU','FT','FI']:
            for idx_inf_rest, inf_rest in df_dadger[mnemon_info_rest].loc[df_dadger[mnemon_info_rest]['id_restricao'].astype('int') == int(row['id_restricao'])].iterrows():
                restricao_comentada.append('&{}\n'.format(info_blocos[mnemon_info_rest]['formatacao'].format(*inf_rest.values).strip()))
                df_dadger[mnemon_info_rest].drop(idx_inf_rest, inplace=True)
                
        if index+1 in comentarios['RE']:
            comentarios['RE'][index+1] = restricao_comentada + comentarios['RE'][index+1]
        else :
            comentarios['RE'][index+1] = restricao_comentada

        df_dadger['RE'].drop(index, inplace=True)

    return df_dadger, comentarios


def add_protecao_gap_negativo(df_dadger, comentarios):

    if 'UH' not in comentarios:
        comentarios['UH'] = {}
    if 0 not in comentarios['UH']:
        comentarios['UH'][0] = []

    novos_comentarios = []
    novos_comentarios.append('&\n')
    novos_comentarios.append('&-----------------------------------------------------------------------------------------------\n')
    novos_comentarios.append('&      ELIMINA DE SOLUCOES NAO OTIMAS\n')
    novos_comentarios.append('&       (REGISTRO TS)\n')
    novos_comentarios.append('&-----------------------------------------------------------------------------------------------\n')
    novos_comentarios.append('&TS\n')
    novos_comentarios.append('&   XXXXXXXXXXXXXXXX  XXXXXXXXXXXXXXXX   X\n')
    novos_comentarios.append('TS                                       1\n')
    novos_comentarios.append('&\n')
    novos_comentarios.append('&\n')
    novos_comentarios.append('&-----------------------------------------------------------------------------------------------\n')
    novos_comentarios.append('&      PENALIDADE DAS VARIAVEIS DE FOLGA\n')
    novos_comentarios.append('&       (REGISTRO PV)\n')
    novos_comentarios.append('&-----------------------------------------------------------------------------------------------\n')
    novos_comentarios.append('&PV\n')
    novos_comentarios.append('&   XXXXXXXXXXXXXXXXXXXX   XXXXXXXXXXXXXXXXXXXX   XXX   XXX   XXXXXXXXXXXXXXXXXXXX   XXXXXXXXXXXXXXXXXXXX\n')
    novos_comentarios.append('PV       16208.0               0.0100\n')
    novos_comentarios.append('&\n')

    comentarios['UH'][0] = novos_comentarios + comentarios['UH'][0]

    return df_dadger, comentarios

def importar_dadger_decomp(dt_inicio_rv, path_arquivo, fonte):

    
    dicionario_sub = {'SE': 1, 'S': 2, 'NE': 3, 'N': 4}
    dicionario_fonte = {'ons': 1, 'ccee':2}
    
    cd_fonte = dicionario_fonte[fonte.lower()]
    
    df_dadger, comentarios = leituraArquivo(path_dadger)
    
    df_bloco_pq = df_dadger['PQ']
    
    df_bloco_pq['tipo'] = df_bloco_pq['nome'].str.strip().str[-3:]
    df_bloco_pq['sub'] = df_bloco_pq['sub'].astype(int)
    df_bloco_pq['estagio'] = df_bloco_pq['estagio'].astype(int)
    df_bloco_pq['gerac_p1'] = df_bloco_pq['gerac_p1'].astype(int)
    df_bloco_pq['gerac_p2'] = df_bloco_pq['gerac_p2'].astype(int)
    df_bloco_pq['gerac_p3'] = df_bloco_pq['gerac_p3'].astype(int)

    datab = wx_dbLib.WxDataB('mysql')
    datab.dbDatabase = 'db_decks'
    
    query_get_id_rodada = 'SELECT id FROM tb_cadastro_decomp where dt_inicio_rv = %s and id_fonte = %s;'
    answer = datab.execute(query_get_id_rodada, (dt_inicio_rv ,cd_fonte))
    
    if len(answer) == 0:
        query_insert_rodada = 'INSERT INTO tb_cadastro_decomp (dt_inicio_rv, id_fonte) VALUES(%s, %s);'
        datab.execute(query_insert_rodada, (dt_inicio_rv, cd_fonte))
        answer = datab.execute(query_get_id_rodada, (dt_inicio_rv ,cd_fonte))
        if len(answer) != 1:
            print('Problema em cadastrar a rodada')
            return
        else:
            id_deck = answer[0][0]
            print('Rodada cadastrada com sucesso')
    else:
        id_deck = answer[0][0]
        print('Rodada já estava cadastrada no banco de dados!')
        
        query_delete_linhas_pequenas_ger = 'DELETE FROM tb_dc_dadger_pq WHERE id_deck = {};'.format(id_deck)
        answer = datab.requestServer(query_delete_linhas_pequenas_ger)
        

    df_geracao_p1 = df_bloco_pq.pivot(index=['sub','estagio'], values='gerac_p1', columns='tipo').reset_index()
    df_geracao_p1['patamar'] = 1
    df_geracao_p1['id_deck'] = id_deck
    
    df_geracao_p2 = df_bloco_pq.pivot(index=['sub','estagio'], values='gerac_p2', columns='tipo').reset_index()
    df_geracao_p2['patamar'] = 2
    df_geracao_p2['id_deck'] = id_deck
    
    df_geracao_p3 = df_bloco_pq.pivot(index=['sub','estagio'], values='gerac_p3', columns='tipo').reset_index()
    df_geracao_p3['patamar'] = 3
    df_geracao_p3['id_deck'] = id_deck
    
    orden_colunas = ['id_deck', 'estagio', 'patamar', 'sub', 'PCT','PCH','EOL','UFV']
    
    geracoes = []
    geracoes += list(df_geracao_p1[orden_colunas].itertuples(index=False, name=None))
    geracoes += list(df_geracao_p2[orden_colunas].itertuples(index=False, name=None))
    geracoes += list(df_geracao_p3[orden_colunas].itertuples(index=False, name=None))
     
    query_insert_geracao = '''INSERT INTO db_decks.tb_dc_dadger_pq
    (id_deck, vl_estagio, vl_patamar, cd_submercado, vl_geracao_pct, vl_geracao_pch, vl_geracao_eol, vl_geracao_ufv)
    VALUES(%s, %s, %s, %s, %s, %s, %s, %s);'''
    
    answer = datab.executemany(query_insert_geracao, geracoes)
    print('Inserido {} linhas na tabela tb_dc_dadger_pq'.format(answer))
    

if __name__ == '__main__':
    
    path_dadger = os.path.abspath(r"C:\Users\cs341052\Downloads\deck_202207_r3\PMO_deck_preliminar\DEC_ONS_072022_RV3_VE\dadger.rv3")
    # path_novo_dadger = path_dadger.replace('dadger.rv', 'dadger_rz2.rv')
    # df_dadger, comentarios = leituraArquivo(path_dadger)
    # escrever_dadger(df_dadger, comentarios, path_novo_dadger)
    # df_dadger, comentarios = comentar_dadger_ons_ccee(df_dadger, comentarios)
    
    path_dadger = os.path.abspath(r"C:\Users\cs341052\Downloads\decks_ds\PMO_deck_preliminar(1)\DEC_ONS_082022_RV1_VE\dadger.rv1")
    data_deck = datetime.datetime(2022,8,6)
    fonte = 'ons'
    importar_dadger_decomp(data_deck, path_dadger, fonte=fonte)
