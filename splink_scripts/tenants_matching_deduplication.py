import splink
import pandas as pd
from splink import DuckDBAPI, Linker, SettingsCreator, block_on
import splink.comparison_library as cl
import psycopg2
import splink.comparison_library as cl
from splink.backends.duckdb import DuckDBAPI
import pandas as pd
import splink.comparison_library as cl
from splink import Linker, SettingsCreator, block_on, splink_datasets
from splink.backends.duckdb import DuckDBAPI
import splink.comparison_level_library as cll 


from credentials import host,port,db,user,password







conn = psycopg2.connect(
    host=host,
    port=port,
    dbname=db,
    user=user,
    password=password
)

query = """SELECT * FROM all_tenants               
  """





df=pd.read_sql(query, conn)
df=df.rename(columns={'source_table':'source_dataset'})
 
 
 
 
 
 
naics_comparison = cl.CustomComparison(
    output_column_name="naics",
    comparison_levels=[
        cll.NullLevel("naics"),
        
        cll.ExactMatchLevel("naics"),
        
        cll.CustomLevel(
            sql_condition="SUBSTR(naics_l::text, 1, 4) = SUBSTR(naics_r::text, 1, 4)",
            label_for_charts="4-digit NAICS match"
        ),
        
        cll.CustomLevel(
            sql_condition="SUBSTR(naics_l::text, 1, 2) = SUBSTR(naics_r::text, 1, 2)",
            label_for_charts="2-digit NAICS match"
        ),

        cll.ElseLevel()
    ]
)




sic_comparison = cl.CustomComparison(
    output_column_name="sic",
    comparison_levels=[
        cll.NullLevel("sic"),
        
        cll.ExactMatchLevel("sic"),
        
        cll.CustomLevel(
            sql_condition="SUBSTR(sic_l::text, 1, 2) = SUBSTR(sic_r::text, 1, 2)",
            label_for_charts="2-digit SIC match"
        ),
        cll.ElseLevel()
    ]
)


sales_volume_comparison = cl.CustomComparison(
    output_column_name="sales_volume",
    comparison_levels=[
        cll.NullLevel("sales_volume"),
        cll.ExactMatchLevel("sales_volume"),
        cll.PercentageDifferenceLevel("sales_volume", percentage_threshold=0.01),
        cll.PercentageDifferenceLevel("sales_volume", percentage_threshold=0.05),
        cll.ElseLevel()
    ]
)






settings=SettingsCreator( link_type='link_and_dedupe',
                         comparisons=[
                             
                             cl.NameComparison("company_name"),
                             cl.ExactMatch("state"),
                             cl.ExactMatch("city"),
                             cl.ExactMatch("year_established"),
                             cl.ExactMatch("website"),
                             cl.ExactMatch("type_of_location"),
                             
                             
                             
                             cl.LevenshteinAtThresholds("street_address",[1,3,4]),
                             cl.ExactMatch("zip_code"),
                             cl.LevenshteinAtThresholds("company_phone",[1,2]),
                             naics_comparison,
                             sic_comparison,
                             sales_volume_comparison
                             
                             
                             
                             
                         ],
                         
                         
                         blocking_rules_to_generate_predictions=[
                             block_on("website"),block_on("company_name"),block_on("street_address","city","zip_code"),block_on("company_name","city","zip_code"),block_on("zip_code","naics","sic")
                                                                ],
                         unique_id_column_name="id",
                         additional_columns_to_retain=["company_name","alternative_name","website","zip_code","street_address","year_established","employee_total","naics","sic","company_phone","sales_volume"]
                         
    
                        )

api=DuckDBAPI()
linker=Linker(df,settings,api)
linker.training.estimate_u_using_random_sampling(max_pairs=1e7)
linker.training.estimate_parameters_using_expectation_maximisation(block_on("street_address","zip_code"))
#linker.training.estimate_parameters_using_expectation_maximisation(block_on("zip_code"))
linker.training.estimate_parameters_using_expectation_maximisation(block_on("company_name","naics","sic"))
linker.training.estimate_parameters_using_expectation_maximisation(block_on("company_name","website"))
linker.training.estimate_parameters_using_expectation_maximisation(block_on("zip_code","naics","sic"))





df_predictions = linker.inference.predict(threshold_match_probability=0.97)
preds=df_predictions.as_pandas_dataframe(limit=100000)





df_clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(
    df_predictions, threshold_match_probability=0.97
)
df_clusters=df_clusters.as_pandas_dataframe()
result=df_clusters[['cluster_id','id']]





import io
with conn.cursor() as curs: 
     curs.execute("DROP TABLE IF EXISTS matched_tenants")  
     curs.execute("CREATE TABLE IF NOT EXISTS matched_tenants( cluster_id text, id text)")
     buffer = io.StringIO()
    
     result.to_csv(buffer, index=False, header=False)
    
     buffer.seek(0)

     copy_sql = f"""
         COPY matched_tenants ("cluster_id","id") 
         FROM STDIN WITH (FORMAT CSV)
     """

     curs.copy_expert(sql=copy_sql, file=buffer)
     conn.commit()
     print("Copy complete.")

    