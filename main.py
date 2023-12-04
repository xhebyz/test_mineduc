import pandas as pd

clean_columns = ['AGNO', 'COD_REG_RBD', 'NOM_RBD', 'NOM_REG_RBD_A', 'RBD', 'MRUN', 'EDAD_ALU', 'COD_DEPE', 'COD_DEPE2',
                 'SIT_FIN', 'SIT_FIN2']


def clean_columns_df(df, year):
    df = df[(df['AGNO'].isin(year))
            & (df['COD_DEPE2'] == 2) & (df['COD_DEPE'] == 3)]

    columns = [col for col in clean_columns if col in df.columns]
    df_limpio = df[columns]
    return df_limpio

def establecimiento_data(df):
    df_disengaged = df[
        (df['RBD_x'] != df['RBD_y']) |
        (df['RBD_x'].isna() & df['RBD_y'].notna()) |
        (df['RBD_x'].notna() & df['RBD_y'].isna()) |
        (df['COD_REG_RBD_x'] != df['COD_REG_RBD_y']) |
        (df['COD_REG_RBD_x'].isna() & df['COD_REG_RBD_y'].notna()) |
        (df['COD_REG_RBD_x'].notna() & df['COD_REG_RBD_y'].isna())
        ]

    count_disengaged_rbd = df_disengaged.groupby(
        ['RBD_x', 'NOM_RBD_x', 'COD_REG_RBD_x', 'NOM_REG_RBD_A_x']).size().reset_index(name='count')
    count_disengaged_rbd = count_disengaged_rbd.rename(columns={"RBD_x": "RBD", "count": "count_disengaged"})

    count_matricula_rbd = df.groupby(["RBD_y"]).size().reset_index(name='count')
    count_matricula_rbd = count_matricula_rbd.rename(columns={"RBD_y": "RBD", "count": "count_matricula"})

    df_rbd = pd.merge(count_disengaged_rbd, count_matricula_rbd, on="RBD", how="outer")
    df_rbd['matricula_teorica'] = df_rbd["count_matricula"] + df_rbd["count_disengaged"]


    #df_disengaged_reg = df[df['COD_REG_RBD_x'] != df['COD_REG_RBD_y']]
    count_disengaged_reg = df_disengaged.groupby(['COD_REG_RBD_x', 'NOM_REG_RBD_A_x']).size().reset_index(
        name='count')
    count_disengaged_reg = count_disengaged_reg.rename(
        columns={"COD_REG_RBD_x": "COD_REG_RBD", "count": "count_disengaged"})

    count_matricula_reg = df.groupby(["COD_REG_RBD_y", "NOM_REG_RBD_A_y"]).size().reset_index(name='count')
    count_matricula_reg = count_matricula_reg.rename(
        columns={"COD_REG_RBD_y": "COD_REG_RBD", "count": "count_matricula"})

    df_rbd_reg = pd.merge(count_disengaged_reg, count_matricula_reg, on="COD_REG_RBD", how="outer")
    df_rbd_reg['matricula_teorica'] = df_rbd["count_matricula"] + df_rbd["count_disengaged"]

    totales_rbd = df_rbd[["count_matricula", "count_disengaged"]].sum()
    totales_rbd["matricula_teorica"] = totales_rbd["count_matricula"] + totales_rbd["count_disengaged"]
    totales_rbd["tasa_disengaged"] = totales_rbd["count_disengaged"] / totales_rbd["matricula_teorica"]


    totales_reg = df_rbd_reg[["count_matricula", "count_disengaged"]].sum()
    totales_reg["matricula_teorica"] = totales_reg["count_matricula"] + totales_reg["count_disengaged"]
    totales_reg["tasa_disengaged"] = totales_reg["count_disengaged"] / totales_reg["matricula_teorica"]


    # matrciula teorica

    # tasa de desvinculacion
    df_rbd["tasa_disengaged"] = df_rbd["count_disengaged"] / df_rbd['matricula_teorica']
    df_rbd_reg["tasa_disengaged"] = df_rbd["count_disengaged"] / df_rbd_reg['matricula_teorica']

    print(df_rbd_reg)
    print(df_rbd)

    pass


if __name__ == '__main__':
    data_rendimiento = pd.read_csv("data/Rendimiento-2022/20230209_Rendimiento_2022_20230131_WEB_PS.csv", sep=";")
    data_matricula = pd.read_csv("data/Matricula-por-estudiante-2023/20230906_Matrícula_unica_2023_20230430_WEB.CSV",
                                 sep=";")

    data_matricula = clean_columns_df(data_matricula, [2023])
    data_rendimiento = clean_columns_df(data_rendimiento, [2022])

    df_fusionado = pd.merge(data_rendimiento, data_matricula, on='MRUN', how='outer')

    establecimiento_data(df_fusionado)
