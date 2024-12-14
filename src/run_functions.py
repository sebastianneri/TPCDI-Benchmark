import os
from statistics import geometric_mean
import time
import pandas as pd

from src.aux_functions import get_max, id_generator
from src.exe_create_functions import clean_warehouse, create_warehouse
from src.exe_load_functions import load_account, load_customers, load_dim_date, load_dim_time, load_dimen_account, load_dimen_customer, load_fact_cash_balances, load_fact_holdings, load_fact_watches, load_finwire_files, load_finwires_into_dim_company, load_finwires_into_dim_security, load_finwires_into_financial_table, load_staging_FactMarketStory, load_staging_Industry, load_staging_Prospect, load_staging_dim_trade, load_staging_hr_file, load_status_type, load_tax_rate, load_trade_type
from src.exe_load_updates_functions import load_update_dimen_trade, load_update_fact_cash_balances, load_update_fact_holdings, load_update_fact_watches, load_update_staging_FactMarketStory, load_update_staging_Prospect

def run_historical_load(spark,scale_factors=["Scale3"]):#, "Scale4", "Scale5", "Scale6"]):
    dbname = "test"
     # Options "Scale3"]):#, "Scale4", "Scale5", "Scale6"
    for scale_factor in scale_factors:
        metrics = {}
        # Init DB
        start = time.time()
        clean_warehouse(spark,dbname)
        create_warehouse(spark,dbname)
        end = time.time() - start
        
        metrics["create_db_time"] = end
        
        staging_area_folder = f"{os.getcwd()}/data/{scale_factor}/Batch1"
        
        # Run historical load
        start = time.time()
        dimdate = load_dim_date(spark,dbname, staging_area_folder)
        dimtime = load_dim_time(spark,dbname, staging_area_folder)
        taxrate = load_tax_rate(spark,dbname, staging_area_folder)
        staginghr = load_staging_hr_file(spark,dbname, staging_area_folder)
        industry = load_staging_Industry(spark,dbname, staging_area_folder)

        
        load_finwire_files(spark,dbname, scale_factor)
        dimcompany = load_finwires_into_dim_company(spark,dbname, scale_factor)
        dimsecurity = load_finwires_into_dim_security(spark,dbname)
        fintable = load_finwires_into_financial_table(spark,dbname)
        
        statustype = load_status_type(spark,dbname, staging_area_folder)
        tradetype = load_trade_type(spark,dbname, staging_area_folder)
        
        factmarkethistory =load_staging_FactMarketStory(spark,dbname, staging_area_folder)
        prospect = load_staging_Prospect(spark,dbname, staging_area_folder)
        
        customer = load_customers(spark,dbname, staging_area_folder)
        account = load_account(spark,dbname, staging_area_folder)
        
        dimtrade = load_staging_dim_trade(spark,dbname, staging_area_folder)
        factcashbalance = load_fact_cash_balances(spark,dbname, staging_area_folder)
        holding = load_fact_holdings(spark,dbname, staging_area_folder)
        watch = load_fact_watches(spark,dbname, staging_area_folder)
        end = time.time() - start
        
        metrics["et"] = end
        
        dimdate_count = dimdate.count()
        dimtime_count = dimtime.count()
        taxrate_count = taxrate.count()
        staginghr_count = staginghr.count()
        dimcompany_count = dimcompany.count()
        dimsecurity_count = dimsecurity.count()
        fintable_count = fintable.count()
        statustype_count = statustype.count()
        tradetype_count = tradetype.count()
        factmarkethistory_count = factmarkethistory.count()
        prospect_count = prospect.count()
        industry_count = industry.count()
        dimtrade_count = dimtrade.count()
        factcashbalance_count = factcashbalance.count()
        holding_count = holding.count()
        watch_count = watch.count()
        customer_count = customer.count()
        account_count = account.count()

        # Sum the individual counts
        rows = dimdate_count + dimtime_count + taxrate_count + staginghr_count + dimcompany_count + dimsecurity_count + fintable_count + statustype_count + tradetype_count + factmarkethistory_count + prospect_count + industry_count + dimtrade_count + factcashbalance_count + holding_count + watch_count + customer_count + account_count

        metrics["rows"] = rows
        metrics["throughput"] = (rows / end)
        
        metrics_df = pd.DataFrame(metrics, index=[0])

        metrics_df.to_csv(f"{os.getcwd()}/results/data/historical_load_{scale_factor}.csv", index=False)



# def run_historical_load(spark, dbname, scale_factor, file_id):
#     metrics = {}
#     # Init DB
#     start = time.time()
#     clean_warehouse(spark,dbname)
#     create_warehouse(spark,dbname)
#     end = time.time() - start

#     metrics["create_db_time"] = end

#     staging_area_folder = f"{os.getcwd()}/data/{scale_factor}/Batch1/"

#     # Run historical load
#     start = time.time()
#     dimdate = load_dim_date(spark, dbname, staging_area_folder)
#     dimtime = load_dim_time(spark, dbname, staging_area_folder)
#     taxrate = load_tax_rate(spark, dbname, staging_area_folder)
#     staginghr = load_staging_hr_file(spark,dbname, staging_area_folder)
#     industry = load_staging_Industry(spark, dbname, staging_area_folder)


#     load_finwire_files(spark,dbname, scale_factor)
#     dimcompany = load_finwires_into_dim_company(spark,dbname, scale_factor)
#     dimsecurity = load_finwires_into_dim_security(spark,dbname)
#     fintable = load_finwires_into_financial_table(spark,dbname)

#     statustype = load_status_type(spark,dbname, staging_area_folder)
#     tradetype = load_trade_type(spark,dbname, staging_area_folder)

#     factmarkethistory = load_staging_FactMarketStory(spark,dbname, staging_area_folder)
#     prospect = load_staging_Prospect(spark,dbname, staging_area_folder)

#     customer = load_customers(spark,dbname, staging_area_folder)
#     account = load_account(spark,dbname, staging_area_folder)

#     dimtrade = load_staging_dim_trade(spark,dbname, staging_area_folder)
#     factcashbalance = load_fact_cash_balances(spark,dbname, staging_area_folder)
#     holding = load_fact_holdings(spark,dbname, staging_area_folder)
#     watch = load_fact_watches(spark,dbname, staging_area_folder)
#     end = time.time() - start

#     metrics["et"] = end
#     dimdate_count = dimdate.count()
#     dimtime_count = dimtime.count()
#     taxrate_count = taxrate.count()
#     staginghr_count = staginghr.count()
#     dimcompany_count = dimcompany.count()
#     dimsecurity_count = dimsecurity.count()
#     fintable_count = fintable.count()
#     statustype_count = statustype.count()
#     tradetype_count = tradetype.count()
#     factmarkethistory_count = factmarkethistory.count()
#     prospect_count = prospect.count()
#     industry_count = industry.count()
#     dimtrade_count = dimtrade.count()
#     factcashbalance_count = factcashbalance.count()
#     holding_count = holding.count()
#     watch_count = watch.count()
#     customer_count = customer.count()
#     account_count = account.count()

#     # Sum the individual counts
#     rows = dimdate_count + dimtime_count + taxrate_count + staginghr_count + dimcompany_count + dimsecurity_count + fintable_count + statustype_count + tradetype_count + factmarkethistory_count + prospect_count + industry_count + dimtrade_count + factcashbalance_count + holding_count + watch_count + customer_count + account_count

#     metrics["rows"] = rows
#     metrics["throughput"] = (rows / end)

#     metrics_df = pd.DataFrame(metrics, index=[0])
    
#     metrics_df.to_csv(f"{os.getcwd()}/results/data/historical_load_{scale_factor}_{file_id}.csv", index=False)
#     return metrics_df

def run_incremental_load(spark,dbname, scale_factor, file_id):
    metrics = {}

    clean_warehouse(spark,dbname)
    create_warehouse(spark,dbname)

    staging_area_folder = f"{os.getcwd()}/data/{scale_factor}/Batch2"

    # Run incremental update
    start = time.time()
    customer = load_dimen_customer(spark,dbname, staging_area_folder)
    account = load_dimen_account(spark,dbname, staging_area_folder)
    dimtrade = load_update_dimen_trade(spark,dbname, staging_area_folder)

    factcashbalance = load_update_fact_cash_balances(spark,dbname, staging_area_folder)
    holding = load_update_fact_holdings(spark,dbname, staging_area_folder)
    watch = load_update_fact_watches(spark,dbname, staging_area_folder)

    factmarkethistory =load_update_staging_FactMarketStory(spark,dbname, staging_area_folder)
    prospect = load_update_staging_Prospect(spark,dbname, staging_area_folder)
    end = time.time() - start

    metrics["et"] = end


    factmarkethistory_count = factmarkethistory.count()
    prospect_count = prospect.count()
    dimtrade_count = dimtrade.count()
    factcashbalance_count = factcashbalance.count()
    holding_count = holding.count()
    watch_count = watch.count()
    customer_count = customer.count()
    account_count = account.count()

    # Sum the individual counts
    rows = factmarkethistory_count + prospect_count + dimtrade_count + factcashbalance_count + holding_count + watch_count + customer_count + account_count


    metrics["rows"] = rows
    metrics["throughput"] = (rows / get_max(end,1800))

    metrics_df = pd.DataFrame(metrics, index=[0])
    metrics_df.to_csv(f"{os.getcwd()}/results/data/incremental_load_{scale_factor}_{file_id}.csv", index=False)

    return metrics_df

def run(scale_factors=["Scale3"]):#, "Scale4", "Scale5", "Scale6"]): 
    dbname = "test"
     # Options "Scale3"]):#, "Scale4", "Scale5", "Scale6"
    file_id = id_generator()

    for scale_factor in scale_factors:
        metrics = {}
        hist_res = run_historical_load(dbname, scale_factor, file_id)
        hist_incr = run_incremental_load(dbname, scale_factor, file_id)
        
        metrics["TPC_DI_RPS"] = int(geometric_mean([hist_res["throughput"], hist_incr["throughput"]]))
        metrics_df = pd.DataFrame(metrics, index=[0])
        metrics_df.to_csv(f"{os.getcwd()}/results/data/overall_stats_{scale_factor}_{file_id}.csv", index=False)
    
        print(hist_res, hist_incr, metrics_df)



