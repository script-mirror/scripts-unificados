def sobrescreve_clast_file(output_path,df_estrutural,df_conjuntural):
    """
    Reescreve o DataFrame no formato especificado, lidando com valores vazios: 'XXXX   XXXX.XX  XX XXXX  XX XXXX'.
    """
    with open(output_path, "w") as file:

        file.write(" NUM  NOME CLASSE  TIPO COMB.  CUSTO   CUSTO   CUSTO   CUSTO   CUSTO " + "\n")
        file.write(" XXXX XXXXXXXXXXXX XXXXXXXXXX XXXX.XX XXXX.XX XXXX.XX XXXX.XX XXXX.XX" + "\n")

        for _, row in df_estrutural.iterrows():
            
            try:
                # BLOCO CONJUNTURAL
                num = f"{str(row[0]):>4}"
                nome = f"{str(row[1]):<12}"
                tipo = f"{str(row[2]):<10}"
                custo1 = f"{f'{float(row[3]):>4.2f}':>7}"
                custo2 = f"{f'{float(row[4]):>4.2f}':>7}"
                custo3 = f"{f'{float(row[5]):>4.2f}':>7}"
                custo4 = f"{f'{float(row[6]):>4.2f}':>7}"
                custo5 = f"{f'{float(row[7]):>4.2f}':>7}"

                line = f" {num} {nome} {tipo} {custo1} {custo2} {custo3} {custo4} {custo5}"
                file.write(line + "\n")

            except ValueError as e:
                pass

        file.write(" 9999" + "\n")
        file.write(" NUM     CUSTO" + "\n")
        file.write(" XXXX   XXXX.XX  XX XXXX  XX XXXX" + "\n")

        for _, row in df_conjuntural.iterrows():

            # BLOCO CONJUNTURAL
            try:
                num = f"{row[0]:>4}" 
                cost = f"{f'{float(row[1]):>4.2f}':>7}" 
                month1 = f"{row[2]:>2}"   
                year1 = f"{row[3]:>4}" 
                month2 = f"{row[4]:>2}" 
                year2 = f"{row[5]:>4}" 
                name= row[6]

                # Formatação da linha
                line = f" {num}   {cost}  {month1} {year1}  {month2} {year2}  {name}"
                file.write(line + "\n")
            except ValueError as e:
                pass
