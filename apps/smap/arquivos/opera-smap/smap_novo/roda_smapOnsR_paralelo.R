


cat("\014")
rm(list = ls())
gc()

library(doParallel)
library(smapOnsR)
library(data.table)

#------------------inicia escrita do caso

#pasta opera smap, path via cmd
args <- commandArgs(trailingOnly = TRUE)
pasta_entrada <- args[1]

#para testes, colocar o caminho da pasta dos arquivos novos aqui
# pasta_entrada <- "C:/Users/cs399274/Downloads/CT28.1/arquivos/opera-smap/smap_novo/Arq_entrada"

print(pasta_entrada)
entrada <- le_arq_entrada_novo(pasta_entrada)




#assimilacao nova nos dados de entrada
entrada$inicializacao[variavel == "ajusta_precipitacao", valor := 1]

#------------------paralelismo
cores <- detectCores()
cl <- makeCluster(cores[1] - 1)
registerDoParallel(cl)
parallel::clusterEvalQ(cl, {library("smapOnsR")})
parallel::clusterEvalQ(cl, {library("data.table")})
#--------------------
tasks <- 1:entrada$sub_bacias[, length(nome)]

parallel::clusterExport(cl, c("entrada"))

saida <- parallel::parLapply(cl, tasks, function(ibacia) {

     saida <- rodada_encadeada_oficial(entrada$parametros,
            entrada$inicializacao, entrada$precipitacao_observada, entrada$precipitacao_prevista, entrada$evapotranspiracao_nc, entrada$vazao,
            entrada$postos_plu, entrada$datas_rodadas, entrada$sub_bacias$nome[ibacia])
    
    saida$previsao <- saida$previsao[variavel == "Qcalc"]
    saida
})
parallel::stopCluster(cl)#
previsao <- data.table::rbindlist(lapply(saida, "[[", "previsao"))

file_saida <- file.path(dirname(pasta_entrada),'Arq_saida',"previsao.csv")
fwrite(previsao, file = file_saida, sep = ";",bom = TRUE)


file_saida <- file.path(dirname(pasta_entrada),'Arq_saida',"entrada.csv")
fwrite(entrada$vazao, file = file_saida, sep = ";",bom = TRUE)










propaga_tv <- function(vazao_montante,  vazao_jusante,  tempo_viagem){
  Nper <- length(vazao_jusante)
  vazao_propagada <- array(rep(0, Nper), c(Nper))
  lag_dias <- ceiling(tempo_viagem / 24)
  fator_segundo_dia <- ((24 * lag_dias) - tempo_viagem) / 24
  fator_primeiro_dia <- (tempo_viagem - (24 * (lag_dias - 1))) / 24
  for (iper in (1 + lag_dias):Nper) {
    if (lag_dias > 0) {
      vazao_propagada[iper] <- (((vazao_montante[iper - lag_dias + 1] * fator_segundo_dia) + (vazao_montante[iper - lag_dias] * fator_primeiro_dia) ) ) + vazao_jusante[iper]
    }else{
      vazao_propagada[iper] <- (((vazao_montante[iper] * fator_segundo_dia) + (vazao_montante[iper] * fator_primeiro_dia))) + vazao_jusante[iper]
    }
  }
  vazao_propagada
}

propaga_muskingum <- function(vazao_montante, vazao_jusante, n, coeficientes){
  Nper <- length(vazao_jusante)
  vazao_passo_n <- array(rep(0, (n + 1), Nper), c(Nper, (n + 1)))
  vazao_passo_n[1, ] <- vazao_montante[1]
  vazao_passo_n[, 1] <- vazao_montante # primeira coluna: vazao original
  vazao_propagada <- array(rep(0, Nper), c(Nper))
  for (iper in 2:Nper) {
    for (indiceN in 2:(n + 1)) {
      vazao_passo_n[iper, indiceN] <- vazao_passo_n[iper, (indiceN-1)] * coeficientes[1] +  vazao_passo_n[(iper - 1), (indiceN-1)] * coeficientes[2] +  vazao_passo_n[iper-1, indiceN] * coeficientes[3]
    }
    vazao_propagada[iper] <- vazao_passo_n[iper, (n + 1)] + vazao_jusante[iper]
  }
  vazao_propagada
}

totaliza_previsao  <- function(previsao, vazao_observada, configuracao) {

  #combina obs e prev
  obs <- data.table::copy(vazao_observada)
  obs[, data_rodada := previsao[, unique(data_caso)]]
  data.table::setnames(obs, "posto", "nome")
  previsao_caso <- data.table::copy(previsao)
  previsao_caso[, variavel := NULL]
  data.table::setnames(previsao_caso, "data_caso", "data_rodada")
  previsao_caso <- combina_observacao_previsao(obs, previsao_caso)
  data.table::setorder(previsao_caso, "data_rodada", "nome", "cenario", "data_previsao")


  #distribui incrementais
  previsao_totalizada <- data.table::data.table()
  for (posto_total in configuracao$posto) {
    previsao_totalizada_subbacia <- data.table::copy(previsao_caso[nome == configuracao[posto == posto_total, sub_bacia_agrupada]])
    previsao_totalizada_subbacia[nome == configuracao[posto == posto_total, sub_bacia_agrupada], valor := valor * configuracao[posto == posto_total, fator]]
    previsao_totalizada_subbacia[nome == configuracao[posto == posto_total, sub_bacia_agrupada], nome := configuracao[posto == posto_total, nome_real]]   
    previsao_totalizada <- data.table::rbindlist(list(previsao_totalizada, previsao_totalizada_subbacia))
  }
  data.table::setnames(previsao_totalizada, "valor", "previsao_distribuida")
  previsao_totalizada[, previsao_incremental := previsao_distribuida]
  data.table::setorder(previsao_totalizada, "data_rodada", "nome", "cenario", "data_previsao")

  # propaga postos flu
  configuracao_postos_plu <- configuracao[bacia_smap == "posto_flu"]
  ordem <- ordem_afluencia(configuracao_postos_plu$posto, configuracao_postos_plu$posto_jusante)
  for (indice_ordem in 1:max(ordem)) {

    indice_configuracao <- which(ordem == indice_ordem)
    for (indice_usina in indice_configuracao) {

      nome_montante <- configuracao_postos_plu[indice_usina, nome_real]
      nome_jusante <- configuracao[posto == configuracao_postos_plu[indice_usina, posto_jusante], nome_real]
      
      data_inicio <- max(max(previsao_totalizada[nome == nome_jusante, min(data_previsao)],
                            previsao_totalizada[nome == nome_jusante, min(data_previsao)],
                            previsao[, unique(data_caso) - 60]))

      if (configuracao_postos_plu[indice_usina, tv] == 0){
        
        n <- configuracao_postos_plu[nome_real == nome_montante, n]
        coeficientes <- c(0, 0, 0)
        coeficientes[1] <- configuracao_postos_plu[nome_real == nome_montante, c1]
        coeficientes[2] <- configuracao_postos_plu[nome_real == nome_montante, c2]
        coeficientes[3] <- configuracao_postos_plu[nome_real == nome_montante, c3]
        
        for (nome_cenario in previsao_totalizada[, unique(cenario)]) {
          
          previsao_totalizada[nome == nome_jusante & cenario == nome_cenario & data_previsao >= data_inicio, previsao_incremental := 
                                propaga_muskingum(previsao_totalizada[nome == nome_montante & cenario == nome_cenario & data_previsao >= data_inicio, previsao_incremental], 
                                                  previsao_totalizada[nome == nome_jusante & cenario == nome_cenario & data_previsao >= data_inicio, previsao_incremental], n, coeficientes)]
        }
        
      } else {
        tv <- configuracao_postos_plu[nome_real == nome_montante, tv]
        for (nome_cenario in previsao_totalizada[, unique(cenario)]) {
          previsao_totalizada[nome == nome_jusante & cenario == nome_cenario & data_previsao >= data_inicio, previsao_incremental := 
                                propaga_tv(previsao_totalizada[nome == nome_montante & cenario == nome_cenario & data_previsao >= data_inicio, previsao_incremental], 
                                          previsao_totalizada[nome == nome_jusante & cenario == nome_cenario & data_previsao >= data_inicio, previsao_incremental], tv)]
        }
      }
    }
    

  }

  # propaga subbacias
  configuracao_sem_postos_plu <- configuracao[!bacia_smap == "posto_flu"]
  previsao_totalizada[, previsao_total := previsao_incremental]
  # previsao_totalizada[, previsao_total_sem_tv := previsao_incremental]
  ordem <- ordem_afluencia(configuracao_sem_postos_plu$posto, configuracao_sem_postos_plu$posto_jusante)
  for (indice_ordem in 1:max(ordem)) {
      indice_configuracao <- which(ordem == indice_ordem)
      for (indice_usina in indice_configuracao) {
          nome_montante <- configuracao_sem_postos_plu[indice_usina, nome_real]
          nome_jusante <- configuracao[posto == configuracao_sem_postos_plu[indice_usina, posto_jusante], nome_real]
          if (length(nome_jusante) != 0) {
              data_inicio <- max(max(previsao_totalizada[nome == nome_jusante, min(data_previsao)],
                                  previsao_totalizada[nome == nome_jusante, min(data_previsao)],
                                  previsao[, unique(data_caso) - 60]))
              if (configuracao_sem_postos_plu[indice_usina, tv] == 0){
                  n <- configuracao_sem_postos_plu[nome_real == nome_montante, n]
                  coeficientes <- c(0, 0, 0)
                  coeficientes[1] <- configuracao_sem_postos_plu[nome_real == nome_montante, c1]
                  coeficientes[2] <- configuracao_sem_postos_plu[nome_real == nome_montante, c2]
                  coeficientes[3] <- configuracao_sem_postos_plu[nome_real == nome_montante, c3]
                  
                    for (nome_cenario in previsao_totalizada[, unique(cenario)]) {
                        
                        if ((coeficientes != c(0,0,0))[1] == TRUE){
                          previsao_totalizada[nome == nome_jusante & cenario == nome_cenario & data_previsao >= data_inicio, previsao_total := 
                              propaga_muskingum(previsao_totalizada[nome == nome_montante & cenario == nome_cenario & data_previsao >= data_inicio, previsao_total], 
                              previsao_totalizada[nome == nome_jusante & cenario == nome_cenario & data_previsao >= data_inicio, previsao_total], n, coeficientes)]
                        }
                        # previsao_totalizada[nome == nome_jusante & cenario == nome_cenario & data_previsao >= data_inicio, previsao_total_sem_tv := 
                        #     previsao_totalizada[nome == nome_montante & cenario == nome_cenario & data_previsao >= data_inicio, previsao_total_sem_tv] + 
                        #     previsao_totalizada[nome == nome_jusante & cenario == nome_cenario & data_previsao >= data_inicio, previsao_total_sem_tv]]
                    }
                  
              } else {
                  tv <- configuracao_sem_postos_plu[nome_real == nome_montante, tv]
                  for (nome_cenario in previsao_totalizada[, unique(cenario)]) {
                      previsao_totalizada[nome == nome_jusante & cenario == nome_cenario & data_previsao >= data_inicio, previsao_total := 
                      propaga_tv(previsao_totalizada[nome == nome_montante & cenario == nome_cenario & data_previsao >= data_inicio, previsao_total], 
                      previsao_totalizada[nome == nome_jusante & cenario == nome_cenario & data_previsao >= data_inicio, previsao_total], tv)]

                      # previsao_totalizada[nome == nome_jusante & cenario == nome_cenario & data_previsao >= data_inicio, previsao_total_sem_tv := 
                      #     previsao_totalizada[nome == nome_montante & cenario == nome_cenario & data_previsao >= data_inicio, previsao_total_sem_tv] +  
                      #     previsao_totalizada[nome == nome_jusante & cenario == nome_cenario & data_previsao >= data_inicio, previsao_total_sem_tv]]
                  }
              }
          }
      }
  }

  previsao_totalizada <- previsao_totalizada[nome %in% configuracao[!bacia_smap == "posto_flu", nome_real]]
  previsao_totalizada <- previsao_totalizada[data_previsao >= data_rodada]
}


# Totaliza as previsoes
configuracao <- data.table::fread(file.path(dirname(pasta_entrada),"configuracao.csv"))
configuracao[, sub_bacia_agrupada := tolower(sub_bacia_agrupada)]

# vazao_observada <- entrada$vazao
# previsao <- previsao

# previsao_totalizada <- totaliza_previsao(previsao,vazao_observada,configuracao)


previsao_totalizada_final <- data.table()
for (dt_rodada in unique(previsao$data_caso)) {
    print(as.Date(dt_rodada))
    previsao_dt_rodada <-  previsao[data_caso  == as.Date(dt_rodada)]
    entrada_vazao_obs <- entrada$vazao[data < dt_rodada]

    previsao_totalizada <- totaliza_previsao(previsao_dt_rodada,entrada_vazao_obs,configuracao)
    previsao_totalizada_final <- rbind(previsao_totalizada_final, previsao_totalizada)

}

# #escreve csv com os postos propagados
file_saida <- file.path(dirname(pasta_entrada),'Arq_saida',"vazoes_prevista_totais_propagadas.csv")
fwrite(previsao_totalizada_final, file = file_saida, sep = ";",bom = TRUE)
print(file_saida)

