from celery import  shared_task
import pandas as pd
from core import models
import sqlite3




@shared_task
def read_and_insert_data():
    df = pd.read_excel('inquerito_socio_economico_data.xlsx', sheet_name=0)
    df.drop_duplicates(subset='KEY', inplace=True, keep='first')
    column_to_field_mapping = {
        col: col.replace('-', '_') for col in df.columns
    }

    instances = [
        models.Inquerito(**{field: row[col] for col, field in column_to_field_mapping.items()})
        for _, row in df.iterrows()
    ]

    # You may want to split the instances into smaller batches if the Excel file is huge
    models.Inquerito.objects.bulk_create(instances)


@shared_task
def read_and_insert_renda_proveniente():
    df = pd.read_excel('inquerito_socio_economico_data.xlsx', sheet_name=2)
    df.drop_duplicates(subset='KEY', inplace=True, keep='first')
    column_to_field_mapping = {
        col: col.replace('-', '_') for col in df.columns
    }
    
    instances = [ 
        models.RendaProvenienteActividade(**{field: row[col] for col, field in column_to_field_mapping.items()})
        for _, row in df.iterrows()
    ]

    models.RendaProvenienteActividade.objects.bulk_create(instances)

@shared_task
def read_and_insert_educacao_saude():
    df = pd.read_excel('inquerito_socio_economico_data.xlsx', sheet_name=3)
    df.drop_duplicates(subset='KEY', inplace=True, keep='first')
    column_to_field_mapping = {
        col: col.replace('-', '_') for col in df.columns
    }
    
    instances = [ 
        models.EducacaoSaude(**{field: row[col] for col, field in column_to_field_mapping.items()})
        for _, row in df.iterrows()
    ]

    models.EducacaoSaude.objects.bulk_create(instances)

@shared_task
def read_and_insert_estrutaras_habitacionais():
    df = pd.read_excel('inquerito_socio_economico_data.xlsx', sheet_name=5)
    df.drop_duplicates(subset='KEY', inplace=True, keep='first')
    column_to_field_mapping = {
        col: col.replace('-', '_') for col in df.columns
    }
    
    instances = [ 
        models.EstruturaHabitacional(**{field: row[col] for col, field in column_to_field_mapping.items()})
        for _, row in df.iterrows()
    ]

    models.EstruturaHabitacional.objects.bulk_create(instances)

@shared_task
def read_and_insert_agregado_familiar():
    df = pd.read_excel('inquerito_socio_economico_data.xlsx', sheet_name=6)
    df.drop_duplicates(subset='KEY', inplace=True, keep='first')
    column_to_field_mapping = {
        col: col.replace('-', '_') for col in df.columns
    }
    
    instances = [ 
        models.AgregadoFamiliar(**{field: row[col] for col, field in column_to_field_mapping.items()})
        for _, row in df.iterrows()
    ]

    models.AgregadoFamiliar.objects.bulk_create(instances)

@shared_task
def read_and_insert_estrutura_habitacionais_talhao():
    df = pd.read_excel('inquerito_socio_economico_data.xlsx', sheet_name=7)
    df.drop_duplicates(subset='KEY', inplace=True, keep='first')
    column_to_field_mapping = {
        col: col.replace('-', '_') for col in df.columns
    }
    
    instances = [ 
        models.EstruturaHabitacionalTalhao(**{field: row[col] for col, field in column_to_field_mapping.items()})
        for _, row in df.iterrows()
    ]

    models.EstruturaHabitacionalTalhao.objects.bulk_create(instances)
    
@shared_task
def read_and_insert_fonte_de_renda():
    df = pd.read_excel('inquerito_socio_economico_data.xlsx', sheet_name=8)
    df.drop_duplicates(subset='KEY', inplace=True, keep='first')
    column_to_field_mapping = {
        col: col.replace('-', '_') for col in df.columns
    }
    
    instances = [ 
        models.FonteDeRenda(**{field: row[col] for col, field in column_to_field_mapping.items()})
        for _, row in df.iterrows()
    ]

    models.FonteDeRenda.objects.bulk_create(instances)

@shared_task
def read_and_insert_pecuaria_arvores():
    df = pd.read_excel('inquerito_socio_economico_data.xlsx', sheet_name=9)
    df.drop_duplicates(subset='KEY', inplace=True, keep='first')
    column_to_field_mapping = {
        col: col.replace('-', '_') for col in df.columns
    }
    
    instances = [ 
        models.PecuariaArvore(**{field: row[col] for col, field in column_to_field_mapping.items()})
        for _, row in df.iterrows()
    ]

    models.PecuariaArvore.objects.bulk_create(instances)

@shared_task
def read_and_insert_patrimonio_bens():
    df = pd.read_excel('inquerito_socio_economico_data.xlsx', sheet_name=10)
    df.drop_duplicates(subset='KEY', inplace=True, keep='first')
    column_to_field_mapping = {
        col: col.replace('-', '_') for col in df.columns
    }
    
    instances = [ 
        models.PatrimonioBens(**{field: row[col] for col, field in column_to_field_mapping.items()})
        for _, row in df.iterrows()
    ]

    models.PatrimonioBens.objects.bulk_create(instances)

@shared_task
def read_and_insert_tipo_animais_cria():
    df = pd.read_excel('inquerito_socio_economico_data.xlsx', sheet_name=13)
    df.drop_duplicates(subset='KEY', inplace=True, keep='first')
    column_to_field_mapping = {
        col: col.replace('-', '_') for col in df.columns
    }
    
    instances = [ 
        models.PecuariaTipoAnimaisCria(**{field: row[col] for col, field in column_to_field_mapping.items()})
        for _, row in df.iterrows()
    ]

    models.PecuariaTipoAnimaisCria.objects.bulk_create(instances)

@shared_task
def read_and_insert_fontes_renda_agricultura():
    df = pd.read_excel('inquerito_socio_economico_data.xlsx', sheet_name=15)
    df.drop_duplicates(subset='KEY', inplace=True, keep='first')
    column_to_field_mapping = {
        col: col.replace('-', '_') for col in df.columns
    }
    
    instances = [ 
        models.PrincipaisFontesRendaAgricultura(**{field: row[col] for col, field in column_to_field_mapping.items()})
        for _, row in df.iterrows()
    ]

    models.PrincipaisFontesRendaAgricultura.objects.bulk_create(instances)

@shared_task
def query_data():
    conn = sqlite3.connect('db.sqlite3')
    # Define the SQL query with multiple LEFT JOINs
    sql_query = """
    SELECT *
    FROM core_inquerito
    LEFT JOIN core_agregadofamiliar ON core_inquerito.KEY = core_agregadofamiliar.PARENT_KEY
    LEFT JOIN core_educacaosaude ON core_inquerito.KEY = core_educacaosaude.PARENT_KEY
    LEFT JOIN core_estruturahabitacional ON core_inquerito.KEY = core_estruturahabitacional.PARENT_KEY
    LEFT JOIN core_estruturahabitacionaltalhao ON core_inquerito.KEY = core_estruturahabitacionaltalhao.PARENT_KEY
    LEFT JOIN core_fontederenda ON core_inquerito.KEY = core_fontederenda.PARENT_KEY
    LEFT JOIN core_patrimoniobens ON core_inquerito.KEY = core_patrimoniobens.PARENT_KEY
    LEFT JOIN core_pecuariaarvore ON core_inquerito.KEY = core_pecuariaarvore.PARENT_KEY
    LEFT JOIN core_pecuariatipoanimaiscria ON core_inquerito.KEY = core_pecuariatipoanimaiscria.PARENT_KEY
    LEFT JOIN core_rendaprovenienteactividade ON core_inquerito.KEY = core_rendaprovenienteactividade.PARENT_KEY
    GROUP BY core_inquerito.KEY
    """

    # Execute the query and fetch the results into a pandas DataFrame
    combined_data = pd.read_sql_query(sql_query, conn)

    # Save the combined data to an Excel file
    output_file = "combined_data_final.xlsx"
    combined_data.to_excel(output_file, index=False)    