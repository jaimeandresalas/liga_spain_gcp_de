import apache_beam as beam
import csv


headers = 'Date,HomeTeam,AwayTeam,FTHG,FTAG,FTR,HTHG,HTAG,HTR,HS,AS,HST,AST,HF,AF,HC,AC,HY,AY,HR,AR,' + \
            'B365H,B365D,B365A,BWH,BWD,BWA,IWH,IWD,IWA,PSH,PSD,PSA,WHH,WHD,WHA,VCH,VCD,VCA,MaxH,MaxD,MaxA,' + \
            'AvgH,AvgD,AvgA,B365_gt_2_5,B365_lt_2_5,P_gt_2_5,P_lt_2_5,Max_gt_2_5,Max_lt_25,Avg_gt_2_5,Avg_lt_2_5,AHh,B365AHH,B365AHA,' + \
            'PAHH,PAHA,MaxAHH,MaxAHA,AvgAHH,AvgAHA,B365CH,B365CD,B365CA,BWCH,BWCD,BWCA,IWCH,IWCD,IWCA,PSCH,' + \
            'PSCD,PSCA,WHCH,WHCD,WHCA,VCCH,VCCD,VCCA,MaxCH,MaxCD,MaxCA,AvgCH,AvgCD,AvgCA,B365C_gt_2_5,B365C_lt_2_5,' + \
            'PC_gt_2_5,PC_lt_2_5,MaxC_gt_2_5,MaxC_lt_2_5,AvgC_gt_2_5,AvgC_lt_2_5,AHCh,B365CAHH,B365CAHA,PCAHH,PCAHA,MaxCAHH,' + \
            'MaxCAHA,AvgCAHH,AvgCAHA'

class FillNullValues(beam.DoFn):
    def process(self, element):
        # Define el ancho esperado para las filas
        expected_width = 104
        
        # Separa los elementos de la fila por comas
        row = element.split(',')
        
        # Si la fila es más corta que el ancho esperado, completa con valores nulos
        if len(row) < expected_width:
            row += [''] * (expected_width - len(row))
        
        # Convierte los elementos a float si son números y no están vacíos
        for i, item in enumerate(row):
            if item and item.replace('.', '', 1).isdigit():
                row[i] = float(item)
        
        return [','.join(map(str, row))]

def raw_proces(row):
    if  int(row[1].split('/')[2]) >= 2019:
        union = row[1] + ',' + ','.join(row[3:])
        return union
    else:
        return ','.join(row[1:])
    



def read_csv_files(folder_path):
    with beam.Pipeline() as pipeline:
        csv_files = pipeline | 'Read CSV Files' >> beam.io.ReadFromText(folder_path, skip_header_lines=1)
        parsed_data = csv_files | 'Parse CSV' >> beam.Map(lambda line: next(csv.reader([line])))
        cleaned_data = parsed_data | 'Clean Data' >> beam.Map(raw_proces)
        filled_data = cleaned_data| 'Fill Null Values' >> beam.ParDo(FillNullValues())
        filled_data | 'Write CSV' >> beam.io.WriteToText('include/dataset/beam_output/output.csv', 
                                                          header=headers, 
                                                          num_shards=1, 
                                                          shard_name_template='')
    

folder_path = 'include/dataset/liga_spain/*.csv'
cleaned_data = read_csv_files(folder_path)
