import json
import duckdb
import boto3
import awswrangler as wr

s3_client = boto3.client("s3")

def lambda_handler(event, context):

    con = duckdb.connect(":memory:")

    df_members = wr.s3.read_csv("s3://bucket_name/asset_meetup/members.csv", use_threads=True, encoding_errors="ignore")

    con.execute("SET home_directory='/tmp'")   

    con.query("install httpfs; load httpfs;")

    con.sql(""" create table members_aux1_filtros as 
            select
            	city ,
            	member_id,
            	min(joined) as joined
            from
            	df_members
            group by 1,2;
    """)
    
    con.sql("""create table members_aux2_filtros as
            select
            	city,
            	member_id,
            	joined,
            	datepart('year', strptime(joined, '%Y-%m-%d %H:%M:%S')) as anio,
            	datepart('month', strptime(joined, '%Y-%m-%d %H:%M:%S')) as mes
            from
            	members_aux1_filtros maf ;
    """)
    
    con.sql("""create table members_aux3_filtros as
            select
            	city,
            	anio,
            	count(member_id) as count_visits
            from 
            	members_aux2_filtros
            group by 
            	1,
            	2
            	;
    """)
    
    con.sql("""create table members_aux4_filtros as
            SELECT
              city,
              anio,
              count_visits,
              RANK() OVER (PARTITION BY anio ORDER BY count_visits DESC) AS rank
            FROM
              members_aux3_filtros;
    """)

    con.table("members_aux4_filtros").show()
    
    df = con.execute("select * from members_aux4_filtros where rank >= 1").fetchdf()
    
    wr.s3.to_parquet(df, "s3://bucket_name/curated/", compression="snappy", mode="overwrite", dataset=True)

    con.close()
    return {
        'statusCode': 200,
        'body': {"output_path": "s3://bucket_name/curated/" }
    }
