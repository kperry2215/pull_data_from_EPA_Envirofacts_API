"""
This script is used to query data directly from the EPA's Envirofacts API, and land in
a pandas data frame. In this script, the GHG data tables are pulled and merged together to 
create a master dataframe containing all of the GHG data we'd need for analysis:
facility location, sector, and subsector, and emissions and emission type by year
"""
import pandas as pd
import io
import requests

class EPAQuery():
    """
    This class is used to pull EPA data directly into Python
    """

    def __init__(self, table_name):
        self.base_url='https://data.epa.gov/efservice/'
        self.table_name = table_name
        self.desired_output_format='CSV' 
        
    def construct_query_URL(self,
                        desired_state=None, desired_county=None,
                        desired_area_code=None, desired_year=None,
                        rows_to_include=None):
        """
        This function constructs the URL that we want to pull the data from 
        based on function inputs
        Arguments:
            table_name: String. Name of the table in the Envirofacts database 
            that we want to pull from
            desired_output_format: String. Can be 'EXCEL', 'CSV', or 'JSON'; 
            the format that you want the data delivered in. We set default to csv as 
            that's how  we're gonna pull into pandas
            desired_state: name of the state abbreviation that you want to pull from.
            DEFAULT SET TO NONE
            desired_county: name of the county that you want to pull from.
            DEFAULT SET TO NONE
            desired_area_code: area code that you want to pull from.
            DEFAULT SET TO NONE
            desired_year: year that you want to pull from.
            DEFAULT SET TO NONE
            rows_to_include: rows that you want to include when pulling the query. EX:
            1:19--rows 1 thru 19. DEFAULT SET TO NONE
        Outputs:
            query: string. URL that we want to pull
        """
        #Base of the query that we're going to build off of
        query_base=self.base_url
        #Add in the table name
        query=query_base+self.table_name+'/'
        #Add in the state qualifier, if the desired_state variable is named
        if desired_state!=None:
            query=query+'state_abbr/'+desired_state+'/'
        #Add in the county qualifier, if the desired_county variable is named
        if desired_county!=None:
            query=query+'county_name/'+desired_county+'/'
        #Add in the area code qualifier, if the desired_area_code variable is named
        if desired_area_code!=None:
            query=query+'zip_code/'+desired_area_code+'/'
        #Add in the year qualifier, if the desired_year variable is named
        if desired_year!=None:
            query=query+'reporting_year/'+desired_year+'/'
        #Add in the desired output format to the query
        query=query+self.desired_output_format
        #If there is a row qualifier, add it here
        if rows_to_include!=None:
            query=query+'/rows/'+rows_to_include
        #Return the completed query
        return query

    def read_query_into_pandas(self, query):
        """
        This function takes the query URL, pings it, and writes to a pandas dataframe
        that is returned
        Arguments:
            query: string. Name of the URL that we want to pull
        Outputs:
            dataframe: pandas dataframe. Dataframe generated from the file URL
        """
        s=requests.get(query).content
        dataframe=pd.read_csv(io.StringIO(s.decode('utf-8')), engine='python',
                              encoding='utf-8', error_bad_lines=False)
        return dataframe
            
def main():
    #Declare the names of the tables that we want to pull
    table_names=['PUB_DIM_FACILITY', 'PUB_FACTS_SECTOR_GHG_EMISSION',
                 'PUB_DIM_SECTOR', 'PUB_DIM_SUBSECTOR', 'PUB_DIM_GHG']
    #Dataframe dictionary
    epa_dfs={}
    #Object dictionary
    table_objects={}
    #Loop through all of the table names in the list, and generate
    #a query to pull via the API, and save to a pandas dataframe
    for table_name in table_names:
        #Generate a new object
        table_objects[table_name]=EPAQuery(table_name)
        #Construct the desired query name
        query=table_objects[table_name].construct_query_URL()
        #Pull in via the URL, and generate a pandas df, 
        #which is then saved into a dictionary of dataframes called
        #epa_dfs for future reference
        epa_dfs[table_name]=table_objects[table_name].read_query_into_pandas(query)
    #Generate a master dataframe by joining all of the
    #dataframes together
    #Merge PUB_DIM_FACILITY and PUB_FACTS_SECTOR_GHG_EMISSION
    master_df=pd.merge(epa_dfs['PUB_DIM_FACILITY'], epa_dfs['PUB_FACTS_SECTOR_GHG_EMISSION'],
                       left_on=['PUB_DIM_FACILITY.FACILITY_ID', 'PUB_DIM_FACILITY.YEAR'],
                       right_on=['PUB_FACTS_SECTOR_GHG_EMISSION.FACILITY_ID',
                                 'PUB_FACTS_SECTOR_GHG_EMISSION.YEAR'], how='inner')
    #Merge master_df with PUB_DIM_SECTOR
    master_df=pd.merge(master_df, epa_dfs['PUB_DIM_SECTOR'], 
                       left_on='PUB_FACTS_SECTOR_GHG_EMISSION.SECTOR_ID',
                       right_on='PUB_DIM_SECTOR.SECTOR_ID')
    #Merge master_df with PUB_DIM_SUBSECTOR
    master_df=pd.merge(master_df, epa_dfs['PUB_DIM_SUBSECTOR'], 
                       left_on='PUB_FACTS_SECTOR_GHG_EMISSION.SUBSECTOR_ID',
                       right_on='PUB_DIM_SUBSECTOR.SUBSECTOR_ID')
    #Merge master_df with PUB_DIM_GHG
    master_df=pd.merge(master_df, epa_dfs['PUB_DIM_GHG'], 
                       left_on='PUB_FACTS_SECTOR_GHG_EMISSION.GAS_ID',
                       right_on='PUB_DIM_GHG.GAS_ID')
    #Subset to include only the important columns
    master_df_subsetted=master_df[['PUB_DIM_FACILITY.LATITUDE', 'PUB_DIM_FACILITY.LONGITUDE',
                     'PUB_DIM_FACILITY.CITY', 'PUB_DIM_FACILITY.STATE',
                     'PUB_DIM_FACILITY.ZIP', 'PUB_DIM_FACILITY.COUNTY', 
                     'PUB_DIM_FACILITY.ADDRESS1', 'PUB_DIM_FACILITY.YEAR',
                     'PUB_DIM_FACILITY.PARENT_COMPANY', 'PUB_DIM_SECTOR.SECTOR_NAME',
                     'PUB_DIM_SUBSECTOR.SUBSECTOR_DESC', 'PUB_DIM_GHG.GAS_CODE', 
                     'PUB_FACTS_SECTOR_GHG_EMISSION.CO2E_EMISSION']]
    print(master_df_subsetted.head(10))
    

if __name__== "__main__":
    main()
