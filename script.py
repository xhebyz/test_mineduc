import pandas as pd
import numpy as np

# Nombres de columnas que se quieren conservar en el DataFrame limpio
CLEAN_COLUMNS = ['AGNO', 'COD_REG_RBD', 'NOM_RBD', 'NOM_REG_RBD_A', 'RBD', 'MRUN', 'EDAD_ALU', 'COD_DEPE', 'COD_DEPE2',
                 'SIT_FIN', 'SIT_FIN2']


def clean_dataframe(df, years):
    """
    Limpia el DataFrame manteniendo solo las filas y columnas deseadas.
    """
    # Filtrar por años y códigos de dependencia
    df_filtered = df[(df['AGNO'].isin(years))
                     & (df['COD_DEPE2'] == 2) & (df['COD_DEPE'] == 3)]

    # Seleccionar solo las columnas que existen en el DataFrame
    columns_to_keep = [col for col in CLEAN_COLUMNS if col in df_filtered.columns]
    return df_filtered[columns_to_keep]


def replace_columns(df, name_replace, name_value):
    df[name_replace] = np.where((df[name_replace] == 0) | (df[name_replace].isna()) | (df[name_replace] == ''),
                                df[name_value], df[name_replace])

    return df

def calculate_disengagement_stats(df):
    """
    Calcula estadísticas de desvinculación para establecimientos y regiones.
    """
    # Condición de desvinculación
    disengagement_condition = (
            (df['RBD_x'] != df['RBD_y']) |
            (df['RBD_x'].isna() & df['RBD_y'].notna()) |
            (df['RBD_x'].notna() & df['RBD_y'].isna()) |
            (df['COD_REG_RBD_x'] != df['COD_REG_RBD_y']) |
            (df['COD_REG_RBD_x'].isna() & df['COD_REG_RBD_y'].notna()) |
            (df['COD_REG_RBD_x'].notna() & df['COD_REG_RBD_y'].isna())
    )

    # Filtrar desvinculados
    df_disengaged = df[disengagement_condition]

    # Agrupar y contar para establecimientos y regiones
    df_disengaged_rbd = group_and_count(df_disengaged, ['RBD_x', 'NOM_RBD_x', 'COD_REG_RBD_x', 'NOM_REG_RBD_A_x'],
                                        'count_disengaged')
    df_disengaged_reg = group_and_count(df_disengaged, ['COD_REG_RBD_x', 'NOM_REG_RBD_A_x'], 'count_disengaged')

    df_disengaged_rbd = df_disengaged_rbd.rename(columns={"RBD_x": "RBD"})
    df_disengaged_reg = df_disengaged_reg.rename(columns={"COD_REG_RBD_x": "COD_REG_RBD"})

    # Agrupar y contar matrículas
    df_enrollment_rbd = group_and_count(df, ["RBD_y", 'NOM_RBD_y', 'COD_REG_RBD_y', 'NOM_REG_RBD_A_y'],
                                        'count_matricula')
    df_enrollment_reg = group_and_count(df, ["COD_REG_RBD_y", "NOM_REG_RBD_A_y"], 'count_matricula')

    df_enrollment_rbd = df_enrollment_rbd.rename(columns={"RBD_y": "RBD"})
    df_enrollment_reg = df_enrollment_reg.rename(columns={"COD_REG_RBD_y": "COD_REG_RBD"})

    # Fusionar y calcular tasas
    df_final_rbd = merge_and_calculate_rates(df_disengaged_rbd, df_enrollment_rbd, 'RBD')
    df_final_reg = merge_and_calculate_rates(df_disengaged_reg, df_enrollment_reg, 'COD_REG_RBD')

    return df_final_rbd, df_final_reg


def group_and_count(df, group_columns, count_column_name):
    """
    Agrupa por las columnas dadas y cuenta las ocurrencias.
    """
    return df.groupby(group_columns).size().reset_index(name=count_column_name)


def merge_and_calculate_rates(df1, df2, on_column):
    """
    Fusiona dos DataFrames y calcula las tasas de desvinculación.
    """
    df_merged = pd.merge(df1, df2, on=on_column, how="outer")
    df_merged.fillna(0, inplace=True)
    df_merged = get_tasa(df_merged)
    return df_merged


def get_tasa(df):
    df['matricula_teorica'] = df['count_matricula'] + df['count_disengaged']
    df['tasa_disengaged'] = df['count_disengaged'] / df['matricula_teorica']
    return df


def get_totales(df):
    df.fillna(0, inplace=True)
    totales = df[["count_matricula", "count_disengaged"]].sum()
    return get_tasa(totales)


def read_data_statics():
    # Cargar datos
    data_performance = pd.read_csv("data/Rendimiento-2022/20230209_Rendimiento_2022_20230131_WEB_PS.csv", sep=";",
                                   low_memory=False)
    data_enrollment = pd.read_csv("data/Matricula-por-estudiante-2023/20230906_Matrícula_unica_2023_20230430_WEB.CSV",
                                  sep=";", low_memory=False)

    # Limpiar datos
    clean_data_enrollment = clean_dataframe(data_enrollment, [2023])
    clean_data_performance = clean_dataframe(data_performance, [2022])

    # Fusionar y calcular estadísticas
    merged_data = pd.merge(clean_data_performance, clean_data_enrollment, on='MRUN', how='outer')
    rbd_stats, reg_stats = calculate_disengagement_stats(merged_data)
    rbd_stats = replace_columns(rbd_stats, "NOM_RBD_x", "NOM_RBD_y")
    rbd_stats = replace_columns(rbd_stats, "NOM_REG_RBD_A_x", "NOM_REG_RBD_A_y")
    rbd_stats = replace_columns(rbd_stats, "COD_REG_RBD_x", "COD_REG_RBD_y")

    rbd_stats = rbd_stats[["RBD", "NOM_RBD_x", "COD_REG_RBD_x", "NOM_REG_RBD_A_x",
                           "count_disengaged", "count_matricula", "matricula_teorica", "tasa_disengaged"]]

    totales_rbd = get_totales(rbd_stats)
    totales_reg = get_totales(reg_stats)


    return {
        "RBD": {
            "stats": rbd_stats,
            "totales": totales_rbd
        },
        "REG": {
            "stats": reg_stats,
            "totales": totales_reg
        }
    }


if __name__ == '__main__':
    read_data_statics()
