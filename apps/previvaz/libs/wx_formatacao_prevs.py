

ordemPostos = [1, 2, 6, 7, 8, 9, 10, 11, 12, 14, 15, 16, 17, 18, 22, 23, 24, 25, 28]
ordemPostos += [31, 32, 33, 34, 47, 48, 49, 50, 51, 52, 57, 61, 62, 63, 71, 72, 73]
ordemPostos += [74, 76, 77, 78, 81, 88, 89, 92, 93, 94, 97, 98, 99, 101, 102, 103, 104]
ordemPostos += [109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122]
ordemPostos += [123, 125, 129, 130, 134, 135, 141, 144, 145, 148, 149, 154, 155, 156, 158]
ordemPostos += [160, 161, 166, 168, 169, 171, 172, 173, 175, 176, 178, 183, 188, 190]
ordemPostos += [191, 196, 197, 198, 201, 202, 203, 204, 205, 206, 207, 209, 211, 213, 215]
ordemPostos += [216, 217, 220, 222, 227, 228, 229, 230, 237, 238, 239, 240, 241, 242]
ordemPostos += [243, 244, 245, 246, 247, 248, 249, 251, 252, 253, 254, 255, 257, 259]
ordemPostos += [261, 262, 263, 266, 269, 270, 271, 273, 275, 277, 278, 279, 280, 281]
ordemPostos += [283, 284, 285, 286, 287, 288, 290, 291, 294, 295, 296, 297, 301, 320]
ordemPostos += [53,225, 260, 226]

def write_prevs(path_saida, prevs_df):
    
    prevs_file_out = open(path_saida,'w')
    for i, posto in enumerate(ordemPostos):
        linha = "{index:>6d}{posto:5d}".format(index=i+1, posto=posto)
        for sem in prevs_df.columns[:6]:
            vazao = prevs_df.loc[posto,sem]
            linha = linha + "{vazao:10.0f}".format(vazao=round(vazao,0))
        linha = linha + "\n"
        prevs_file_out.write(linha)
    prevs_file_out.close()

    print('PREVS gerado com sucesso.')
    print('{}'.format(path_saida))
    