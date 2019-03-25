import pandas as pd

def df_to_xl(df_map, filename):
    writer = pd.ExcelWriter(filename)
    for sheet_name, df in df_map.items():
        if df.index.name:
            index=True
        else:
            index=False
        df.to_excel(writer, sheet_name=sheet_name, index=index)

    writer.save()