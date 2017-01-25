# -*- coding: utf-8 -*-
"""
Created on Wed Jan 25 10:30:26 2017

@author: pmaldonado
"""

import xmlrpc.client
from datetime import datetime 
import pandas as pd

from keboola import docker

class Campaigns:
    # Custom extractor 
    
    def run(self):
        # initialize KBC configuration
        cfg = docker.Config()
        # validate application parameters
        parameters = cfg.get_parameters()
        
        username = parameters.get('username')
        password = parameters.get('password')
        
        start_date = parameters.get('start_date')
        end_date = parameters.get('end_date')
        
        
        s = xmlrpc.client.ServerProxy('https://api.sklik.cz/cipisek/RPC2')
    
        
        session = s.client.login(username, password)['session']
        
        camp_list_res = s.campaigns.list( dict(session = session))
        
        camp_ids = [camp_list_res['campaigns'][i]['id'] \
                    for i in range(len(camp_list_res['campaigns'])) ]
        
        camp_names = [camp_list_res['campaigns'][i]['name'] \
                    for i in range(len(camp_list_res['campaigns'])) ]                               
                                       
                                       
        stats = []            
        
        for ix in range(len(camp_ids)):
            
            # Fetch the stats table
            stats_res = s.campaigns.stats(dict(session = session), 
                              [camp_ids[ix]], 
                              dict(
                                   dateFrom = datetime.strptime(start_date, "%Y-%m-%d"), 
                                   dateTo = datetime.strptime(end_date, "%Y-%m-%d"), 
                                   granularity = "daily"
                                   ))
            n_dates =len(stats_res['report'][0]['stats'])
            
            campaign_id = camp_ids[ix]
            campaign_name = camp_names[ix]
            
            for date in range(n_dates):
                tmp = stats_res['report'][0]['stats'][date]
                tmp['campaign_id'] = campaign_id
                tmp['campaign_name'] = campaign_name
                stats.append(tmp)
                
                
        # Get the campaigns table
        res = s.campaigns.get(dict(session=session), [camp_ids[ix]])
        
        df = pd.DataFrame.from_dict(res['campaigns'])   
        
        cols = ['id','name','deleted',
                'status', 'dayBudget', 'exhaustedDayBudget',
                'adSelection','createDate','totalBudget',
                'exhaustedTotalBudget', 'totalClicks',
                'exhaustedTotalClicks', 'userId'] 
                
        camps = df[cols]                    
        
        output = pd.DataFrame.from_dict(stats)
        
        
        tables = cfg.get_expected_output_tables()
       
            
        out_table_stats = tables[0]
        out_table_camps = tables[1]

        # physical location of the target file with output data
        out_file_stats = out_table_stats['full_path']
        out_file_camps = out_table_camps['full_path']
        
        
        ## Save the CSVs
        output[['campaign_id',
                'campaign_name',
                'date',
                'clicks',
                'price',
                'impressions']].to_csv(out_file_stats, index = False)
                
        camps.to_csv(out_file_camps, index = False)                
                
        print("Data successfully imported")                