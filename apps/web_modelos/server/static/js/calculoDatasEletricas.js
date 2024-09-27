    function inicioMesEletrico(dataReferente){

       let proxSexta = new Date(dataReferente)
       let dia_semana = proxSexta.getDay()

       // encontra a proxima sexta (ultimo dia da semana eletrica)
       while(dia_semana != 5){
           proxSexta.setDate(proxSexta.getDate() + 1)
           dia_semana = proxSexta.getDay()
       }

       let inicioMes = new Date(dataReferente)
       inicioMes.setDate(1)
       dia_semana = inicioMes.getDay()

       while(dia_semana != 6){
           inicioMes.setDate(inicioMes.getDate() - 1)
           dia_semana = inicioMes.getDay()
       }

       return inicioMes
    }

    function dataProximaRev(dataReferente){

        let dia_semana = dataReferente.getDay()
        let proxRev = new Date(dataReferente)

        while(dia_semana != 6){
            proxRev.setDate(proxRev.getDate() + 1)
            dia_semana = proxRev.getDay()
        }

        return proxRev

    }


    function dataProximaRev_moment(dataReferente){

        let dia_semana = dataReferente.weekday()
        let proxRev = moment(dataReferente)

        while(dia_semana != 6){
            proxRev.add(1, 'days')
            dia_semana = proxRev.weekday()
        }
        return proxRev
    }


    function inicioMesEletrico_moment(dataReferente){

       let proxSexta = moment(dataReferente)
       let dia_semana = proxSexta.weekday()

       // encontra a proxima sexta (ultimo dia da semana eletrica)
       while(dia_semana != 5){
           proxSexta.add(1, 'days')
           dia_semana = proxSexta.weekday()
       }

       let inicioMes = moment(dataReferente)
       inicioMes.startOf('month')
       dia_semana = inicioMes.weekday()

       while(dia_semana != 6){
           inicioMes.subtract(1, 'days')
           dia_semana = inicioMes.weekday()
       }

       return inicioMes
    }
    