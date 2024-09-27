
var colors={
2000:['rgba(255,0,0,1)'],
2001:['rgba(0,255,0,1)'],
2002:['rgba(0,0,255,1)'],
2003:['rgba(255,255,0,1)'],
2004:['rgba(0,255,255,1)'],
2005:['rgba(255,0,255,1)'],
2006:['rgba(128,128,0,1)'],
2007:['rgba(128,0,128,1)'],
2008:['rgba(0,128,128,1)'],
2009:['rgba(128,128,128,1)'],
2010:['rgba(255,128,0,1)'],
2011:['rgba(0,255,128,1)'],
2012:['rgba(128,0,255,1)'],
2013:['rgba(194,227,47,1)'],
2014:['rgba(155,50,187,1)'],
2015:['rgba(209,128,123,1)'],
2016:['rgba(212,138,246,1)'],
2017:['rgba(255,0,0,1)'],
2018:['rgba(75,42,218,1)'],
2019:['rgba(20,219,251,1)'],
2020:['rgba(224,92,8,1)'],
2021:['rgba(221,194,150,1)'],
2022:['rgba(212,21,164,1)'],
2023:['rgba(68,231,205,1)'],
2024:['rgba(63,191,82,1)'],
2025:['rgba(230,52,98,1)'],
2026:['rgba(82,156,187,1)'],
2027:['rgba(191,127,63,1)'],
2028:['rgba(52,230,211,1)'],
2029:['rgba(163,82,187,1)'],
2030:['rgba(107,63,191,1)'],
'max':['rgba(128, 128, 128,0.5)'],
'min':['rgba(128, 128, 128,0.5)']
};




cores = {}
cores['RV'] = ['rgba(106,90,205,1.0)', 'rgba(106,90,205,0.9)', 'rgba(106,90,205,0.8)', 'rgba(106,90,205,0.7)', 'rgba(106,90,205,0.5)', 'rgba(106,90,205,0.4)']
cores['ACOMPH'] = ['rgba(6, 187, 199,1.0)', 'rgba(6, 187, 199,0.9)', 'rgba(6, 187, 199,0.8)', 'rgba(6, 187, 199,0.7)', 'rgba(6, 187, 199,0.5)', 'rgba(6, 187, 199,0.3)']
cores['PCONJUNTO'] = ['rgba(0,0,255,1.0)', 'rgba(0,0,255,0.9)', 'rgba(0,0,255,0.8)', 'rgba(0,0,255,0.7)', 'rgba(0,0,255,0.5)', 'rgba(0,0,255,0.3)']
cores['GEFS'] = ['rgba(46,139,87,1.0)', 'rgba(46,139,87,0.9)', 'rgba(46,139,87,0.8)', 'rgba(46,139,87,0.7)', 'rgba(46,139,87,0.5)', 'rgba(46,139,87,0.3)']
cores['GFS'] = ['rgba(218,165,32,1.0)', 'rgba(218,165,32,0.9)', 'rgba(218,165,32,0.8)', 'rgba(218,165,32,0.7)', 'rgba(218,165,32,0.5)', 'rgba(218,165,32,0.3)'] 
cores['PZERADA'] = ['rgba(242,9,9,1.0)', 'rgba(242,9,9,0.9)', 'rgba(242,9,9,0.8)', 'rgba(242,9,9,0.7)', 'rgba(242,9,9,0.5)', 'rgba(242,9,9,0.3)'] 
cores['EC'] = ['rgba(0,255,0,1.0)', 'rgba(0,255,0,0.9)', 'rgba(0,255,0,0.8)', 'rgba(0,255,0,0.7)', 'rgba(0,255,0,0.5)', 'rgba(0,255,0,0.3)'] 
cores['EC-ensremvies'] = ['rgba(0,174,0,1.0)', 'rgba(0,174,0,0.9)', 'rgba(0,174,0,0.8)', 'rgba(0,174,0,0.7)', 'rgba(0,174,0,0.5)', 'rgba(0,174,0,0.3)'] 
cores['ETA40'] = ['rgba(255,0,255,1.0)', 'rgba(255,0,255,0.9)', 'rgba(255,0,255,0.8)', 'rgba(255,0,255,0.7)', 'rgba(255,0,255,0.5)', 'rgba(255,0,255,0.3)'] 
cores['ETA40remvies'] = ['rgba(213,0,255,1.0)', 'rgba(213,0,255,0.9)', 'rgba(213,0,255,0.8)', 'rgba(213,0,255,0.7)', 'rgba(213,0,255,0.5)', 'rgba(213,0,255,0.3)'] 
cores['PCONJUNTO-EXT'] = ['rgba(129,214,0,1)', 'rgba(129,214,0,0.9)', 'rgba(129,214,0,0.8)', 'rgba(129,214,0,0.7)', 'rgba(129,214,0,0.5)', 'rgba(129,214,0,0.3)'] 
cores['GEFSremvies'] = ['rgba(224,107,11.0)', 'rgba(224,107,11,0.9)', 'rgba(224,107,11,0.8)', 'rgba(224,107,11,0.7)', 'rgba(224,107,11,0.5)', 'rgba(224,107,11,0.3)']
cores['PMEDIA'] = ['rgba(52, 56, 55, 1)', 'rgba(52, 56, 55, 0.9)', 'rgba(52, 56, 55, 0.8)', 'rgba(52, 56, 55, 0.7)', 'rgba(52, 56, 55, 0.5)', 'rgba(52, 56, 55, 0.3)']
cores['PCONJUNTO2'] = ['rgba(93, 33, 208, 1)', 'rgba(93, 33, 208, 0.9)', 'rgba(93, 33, 208, 0.8)', 'rgba(93, 33, 208, 0.7)', 'rgba(93, 33, 208, 0.5)', 'rgba(93, 33, 208, 0.3)']
cores['MLT'] = ['rgba(14,0,0)']
cores['MERGE'] = ['rgba(94, 146, 206, 1)']




modelosCarga = {} 
modelosCarga['DECOMP'] = ['rgba(242,9,9,1.0)','rgba(0,0,255,1.0)','rgba(0, 0, 0, 1)','rgba(218,165,32,1.0)','rgba(213,0,255,0.3)']
modelosCarga['DESSEM'] = ['rgba(129,214,0,0.9)','rgba(255,0,255,1.0)','rgba(6, 187, 199,1.0)','rgba(93, 33, 208, 1)','rgba(52, 56, 55, 1)']
modelosCarga['NEWAVE'] = ['rgba(0, 0, 0, 1)','rgba(242,9,9,1.0)','rgba(0,0,255,1.0)','rgba(218,165,32,1.0)','rgba(6, 187, 199,1.0)','rgba(93, 33, 208, 1)']

colors_semanas = {}
colors_semanas[0] = ['rgba(242,9,9,1.0)', 'rgba(242,9,9,0.9)', 'rgba(242,9,9,0.8)', 'rgba(242,9,9,0.7)', 'rgba(242,9,9,0.5)', 'rgba(242,9,9,0.3)'] 
colors_semanas[1] = ['rgba(6, 187, 199,1.0)', 'rgba(6, 187, 199,0.9)', 'rgba(6, 187, 199,0.8)', 'rgba(6, 187, 199,0.7)', 'rgba(6, 187, 199,0.5)', 'rgba(6, 187, 199,0.3)']
colors_semanas[2] = ['rgba(46,139,87,1.0)', 'rgba(46,139,87,0.9)', 'rgba(46,139,87,0.8)', 'rgba(46,139,87,0.7)', 'rgba(46,139,87,0.5)', 'rgba(46,139,87,0.3)']
colors_semanas[3] = ['rgba(255,0,255,1.0)', 'rgba(255,0,255,0.9)', 'rgba(255,0,255,0.8)', 'rgba(255,0,255,0.7)', 'rgba(255,0,255,0.5)', 'rgba(255,0,255,0.3)']
colors_semanas[4] = ['rgba(106,90,205,1.0)', 'rgba(106,90,205,0.9)', 'rgba(106,90,205,0.8)', 'rgba(106,90,205,0.7)', 'rgba(106,90,205,0.5)', 'rgba(106,90,205,0.4)']


function getColorSemana(numSemana){

  return colors_semanas[numSemana]

}


function ramdomColor(){
    int1 = Math.floor(255 * Math.random())
    int2 = Math.floor(255 * Math.random())
    int3 = Math.floor(255 * Math.random())
  
    str_defalt = 'rgba(' + int1 + ',' + int2 + ',' + int3 + ','
    lst_cores = [str_defalt+'1)', str_defalt+'0.9)', str_defalt+'0.8)', str_defalt+'0.7)', str_defalt+'0.5)', str_defalt+'0.3)']
    return lst_cores
  }



function getColor_byYear(year){
  anoAtual = new Date().getFullYear();
  if (anoAtual == year){
    colors[year] = ['rgba(0,0,0,1)']
  }
  return colors[year]
  
}	


function getColor_byModelo(modelo){
	return cores[modelo]
}	


function getColor_byModeloCargas(modelo){
	return modelosCarga[modelo]
}	


// site comparativo_carga_newave
// cor padrao para os 6 anos
let anosNewave = {
  'coresNewave': ['rgba(0,0,255,0.6)','rgba(242,9,9,0.6)', 'rgba(218,165,32,0.6)','rgba(6, 187, 199,0.6)','rgba(99, 56, 55, 0.6)', 'rgba(129,214,0,0.6)','rgba(255,0,255,1.0)','rgba(106,90,205,1.0)','rgba(221,194,150,1)','rgba(0,128,128,1)','rgba(230,52,98,1)','rgba(28,165,32,0.3)','rgba(0, 0, 0, 1)','rgba(25,9,155,0.6)','rgba(212,138,246,1)','rgba(128, 128, 128,0.5)'],
};

function getColor_byModeloNewave(modelo) {
  return anosNewave[modelo] || [];
}
