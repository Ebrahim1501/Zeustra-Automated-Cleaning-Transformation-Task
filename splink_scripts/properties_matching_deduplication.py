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

query = """SELECT * FROM all_properties               
  """




df=df.rename(columns={'source_table':'source_dataset'})




from splink.blocking_analysis import cumulative_comparisons_to_be_scored_from_blocking_rules_chart
db_api = DuckDBAPI()



prediction_rules = [
     block_on("address"),
    block_on("zip_code"),
    block_on("city","county"),
    block_on("state","city"),
    block_on('city'),
    block_on('county')]
cumulative_comparisons_to_be_scored_from_blocking_rules_chart(
    table_or_tables=df,
    blocking_rules=prediction_rules,
    db_api=db_api,
    link_type="link_and_dedupe",
    source_dataset_column_name='source_dataset',
    unique_id_column_name='original_source_id'
)







sqft_range_comparison=cl.CustomComparison(output_column_name='within_sqft_area_range',
                                    comparison_levels=[
                                                          cll.CustomLevel(
                                                        sql_condition="""
                                                        (total_building_sqft_l IS NOT NULL AND (sqft_min_r IS NULL OR sqft_max_r IS NULL)) OR
                                                        (total_building_sqft_r IS NOT NULL AND (sqft_min_l IS NULL OR sqft_max_l IS NULL))
                                                        """
                                                    ).configure(is_null_level=True),
                                                        cll.CustomLevel(sql_condition="""
                                                                                    (total_building_sqft_l BETWEEN sqft_min_r AND sqft_max_r) OR
                                                                                    (total_building_sqft_r BETWEEN sqft_min_l AND sqft_max_l)
                                                                                      """),
                                                        cll.CustomLevel(sql_condition="""
                                                                                     (total_building_sqft_l BETWEEN (sqft_min_r * 0.95) AND (sqft_max_r * 1.05)) OR
                                                                                     (total_building_sqft_r BETWEEN (sqft_min_l * 0.95) AND (sqft_max_l * 1.05))
                                                                                      """),
                                                        cll.ElseLevel()
                                                       ]
                                    
                                    )

settings=SettingsCreator( link_type='link_only',#'link_and_dedupe',
                        comparisons=[ cl.LevenshteinAtThresholds("zip_code",[1]).configure(term_frequency_adjustments=True),
                                     #cl.JaroWinklerAtThresholds("address", [0.95, 0.90]).configure(term_frequency_adjustments=True),
                                     cl.LevenshteinAtThresholds("address",[1,2]),
                                     #cl.ExactMatch("address"),
                                     cl.ExactMatch("state"),
                                     cl.ExactMatch("county"),
                                    cl.ExactMatch("city"),
                                    #cl.ExactMatch("zip_code"),
                                    #cl.ExactMatch("latitude","longitude"),
                                    #cl.JaroWinklerAtThresholds('city',[0.9,0.80]).configure(term_frequency_adjustments=True),
                                    cl.DistanceInKMAtThresholds('latitude','longitude',[0.1]),
                                    sqft_range_comparison,
                                    
                                    
                                    ],
                        additional_columns_to_retain=['state','county','city','zip_code','total_building_sqft','sqft_min','sqft_max','source_dataset'],
                        
                        blocking_rules_to_generate_predictions=[block_on("city","county"),block_on("address"),block_on("zip_code")
                                                                
                                                                
                                                                ],
                        retain_matching_columns=True,
                        unique_id_column_name="original_source_id"

                        )

db_api = DuckDBAPI()
linker = Linker(df, settings, db_api=db_api)

linker.training.estimate_u_using_random_sampling(max_pairs=1e7)
print("Done estimating u values")
# linker.training.estimate_parameters_using_expectation_maximisation(block_on("county","city"))
# linker.training.estimate_parameters_using_expectation_maximisation(block_on("zip_code"))
# linker.training.estimate_parameters_using_expectation_maximisation(block_on("address"))
linker.training.estimate_parameters_using_expectation_maximisation(block_on("address"))
linker.training.estimate_parameters_using_expectation_maximisation(block_on("zip_code"))
linker.training.estimate_parameters_using_expectation_maximisation(block_on("city","county"))


print("Done estimating m values")








df_predictions = linker.inference.predict(threshold_match_probability=0.90)



df_clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(
    df_predictions, threshold_match_probability=0.97
)
df_clusters=df_clusters.as_pandas_dataframe()
result=df_clusters[['cluster_id','id']]




import io
with conn.cursor() as curs:
    curs.execute("DROP TABLE IF EXISTS matched_properties")  
    curs.execute("CREATE TABLE IF NOT EXISTS matched_properties( cluster_id text, id text)")
    buffer = io.StringIO()
    
    result.to_csv(buffer, index=False, header=False)
    
    buffer.seek(0)

    copy_sql = f"""
        COPY matched_properties ("cluster_id","id") 
        FROM STDIN WITH (FORMAT CSV)
    """

    curs.copy_expert(sql=copy_sql, file=buffer)
    
    conn.commit()
    print("Copy complete.")

    