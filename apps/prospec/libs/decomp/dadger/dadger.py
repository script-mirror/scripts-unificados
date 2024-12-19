import re
import pandas as pd
import logging
logging.basicConfig(level=logging.DEBUG,
                    format='%(levelname)s:\t%(asctime)s\t %(name)s.py:%(lineno)d\t %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    handlers=[
                        logging.StreamHandler()
                    ])

logger = logging.getLogger(__name__)

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
            

info_blocos['CX'] = {'campos':[
                'mnemonico',
                'acoplamento_nw',
                'acoplamento_dc',
            ],
            'regex':'(.{2})  (.{4}) (.{4})(.*)',
            'formatacao':'{:>2}   {:>4} {:>4}'}
            


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
                logger.warning(f'Mnemonico {mnemonico} nao encontrado.')
                continue
                
            infosLinha = re.split(info_blocos[mnemonico]['regex'], line)
            if len(infosLinha) < 2:
                logger.warning(f"Linha incorreta: {infosLinha}")
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

def sobrescreve_bloco(path_to_modify:str,mnemonico_bloco:str, values:list,skip_lines:int):

    alterar=False
    count_lines=0

    with open(path_to_modify, 'r', encoding='iso-8859-1') as file:

        lines = file.readlines()
        new_lines = []

        for i, line in enumerate(lines):

            if alterar:
                if count_lines < skip_lines and '&-----------' not in line:
                    count_lines += 1
                    continue
                else:
                    alterar = False

            new_lines.append(line)
            
            if f"&{mnemonico_bloco.upper()}" in line: 
                alterar = True
                count_lines = 0
                new_lines.extend([f"{linha}\n" for linha in values])

    with open(path_to_modify, 'w') as file:
        file.writelines(new_lines)
