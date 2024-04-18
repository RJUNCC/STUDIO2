import time
from datetime import datetime
import airflow
from airflow import DAG, task, TaskGroup, MsSqlHook, BaseHook
# from airflow.models.dag import DAG 
# from airflow.decorators import task 
# from airflow.utils.task_group import TaskGroup 
# from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import pandas as pd
from sqlalchemy import create_engine

# extract tasks
@task()
def get_src_tables():
    hook = MsSqlHook(mssql_conn_id='sqlsevrver')
    sql = """SELECT t.name as table_name
        from sys.tables t where t.name in ('Dimproduct', 'DimProductSubcategory', 'DimProductCategory') """
    df = hook.get_pandas_df(sql)
    print(df)
    tbl_dict = df.to_dict('dict')
    return tbl_dict

@task()
def load_src_data(tbl_dict: dict):
    conn = BaseHook.get_connection('postgres')
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    all_tbl_name = []
    start_time = time.time()

    # access the table_name element in dictionaries
    for k, v in tbl_dict['table_name'].items():
        all_tbl_name.append(v)
        rows_imported = 0
        sql = f'select * FROM {v}'
        hook = MsSqlHook(mssql_conn_id='sqlsevrver')
        df = hook.get_pandas_df(sql)
        print(f'importing rows {rows_imported} to {rows_imported + len(df)}... for table {v}', end='')
        df.to_sql(f'src_{v}', engine, index=False, if_exists='replace')
        rows_imported += len(df)
        print(f'Done. {len(df)} rows imported to {v} in {time.time() - start_time} seconds')
    print("Data imported successfully")
    return all_tbl_name

# transform tasks
@task()
def transform_srcProduct():
    conn = BaseHook.get_connection('postgres')
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    pdf = pd.read_sql_query('select * from public.src_DimProductSubcategory', engine)

    # drop columns
    revised = pdf[['ProductSubcategoryKey', 'EnglishProductSubcategoryName', 'ProductSubcategoryAlternateKey', 'EnglishProductSubcategoryName', 'ProductCategoryKey']]

    # rename columns with rename function
    revised = revised.rename(columns={"EnglishProductSubcategoryName":"ProductSubcategoryName"})
    revised.to_sql(f"stg_DimProductSubcategory", engine, if_exists='replace', index=False)

    # rename columns with rename function
    revised = revised.rename(columns={"EnglishProductCategoryName":"ProductCategoryName"})
    revised.to_sql(f"stg_DimProductCategory", engine, if_exists='replace', index=False)
    return {"table(s) processed": "Data imported successful"}

@task()
def transform_srcProductSubcategory():
    conn = BaseHook.get_connection('postgres')
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    pdf = pd.read_sql_query('select * from public.src_DimProductSubcategory', engine)

    # drop columns
    revised = pdf[['ProductSubcategoryKey', 'EnglishProductSubcategoryName', 'ProductSubcategoryAlternateKey', 'EnglishProductSubcategoryName', 'ProductCategoryKey']]

    # rename columns with rename function
    revised = revised.rename(columns={"EnglishProductSubcategoryName":"ProductSubcategoryName"})
    revised.to_sql(f"stg_DimProductSubcategory", engine, if_exists='replace', index=False)
    return {"table(s) processed": "Data imported successful"}

@task()
def transform_srcProductCategory():
    conn = BaseHook.get_connection('postgres')
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    pdf = pd.read_sql_query('select * from public.src_DimProductCategory', engine)

    # drop columns
    revised = pdf[['ProductCategoryKey', 'EnglishProductCategoryName', 'ProductCategoryAlternateKey', 'EnglishProductCategoryName']]

    # rename columns with rename function
    revised = revised.rename(columns={"EnglishProductCategoryName":"ProductCategoryName"})
    revised.to_sql(f"stg_DimProductCategory", engine, if_exists='replace', index=False)
    return {"table(s) processed": "Data imported successful"}

# load
@task()
def prdProduct_model():
    conn = BaseHook.get_connection('postgres')
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    pc = pd.read_sql_query('select * from public.stg_DimProductCategory', engine)
    p = pd.read_sql_query('select * from public.stg_DimProduct', engine)
    # p['ProductSubcategoryKey'] = p['ProductSubcategoryKey'].astype(float)
    p['ProductSubcategoryKey'] = p['ProductSubcategoryKey'].astype(int)
    ps = pd.read_sql_query('select * from public.stg_DimProductSubcategory', engine)

    # join tables
    merged = p.merge(ps, on='ProductSubcategoryKey').merge(pc, on="ProductCategoryKey")
    merged.to_sql(f"prd_DimProductCateogory", engine, if_exists='replace', index=False)
    return {"table(s) processed": "Data imported successful"}

# [START how_to_task_group]
with DAG(
        dag_id='product_etl_dag', 
        schedule_interval="0.9 * * *", 
        start_date=datetime(2024, 4, 2),
        catchup=False, 
        tags=["product_model"]
    ) as dag:

    with TaskGroup("extract_dimProducts_load", 
                   tooltip="Extract and load source data") as extract_load_src:
        src_product_tbls = get_src_tables()
        load_dimProducts = load_src_data(src_product_tbls)

        # define order
        src_product_tbls >> load_dimProducts

    with TaskGroup("transform_dimProducts",
                   tooltip="Transform and stage data") as transform_src_product:
        transform_srcProduct = transform_srcProduct()
        transform_srcProductSubcategory = transform_srcProductSubcategory()
        transform_srcProductCategory = transform_srcProductCategory()

        #define task order
        [transform_srcProduct, transform_srcProductSubcategory, transform_srcProductCategory]
    
    with TaskGroup("load_product_model", tooltip="Final Product model") as load_product_model:
        prdProduct_model = prdProduct_model()

        # define order
        prdProduct_model
    
    extract_load_src >> transform_src_product >> load_product_model