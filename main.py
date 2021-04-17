import requests
import pandas as pd
import csv
from datetime import date, timedelta
import numpy as np
from twython import Twython
import time
from bokeh.io import export_png, export_svgs
from bokeh.models import ColumnDataSource, DataTable, TableColumn, HTMLTemplateFormatter
from io import BytesIO
import cloudscraper

def save_df_as_image(df, path):
    source = ColumnDataSource(df)
    df_columns = [df.index.name]
    df_columns.extend(df.columns.values)
    columns_for_table=[]

    template="""                
            <div style="color:<%= 
                (function colorfromint(){
                    if (Variation > 0)
                        {return('green')}
                    else if (Variation < 0)
                        {return('red')}
                    else 
                        {return('blue')}
                    }())%>;"> 
                <%=value%>
            </div>
            """
    formatter =  HTMLTemplateFormatter(template=template)

    for column in df_columns:
        if(column == 'Variation'):
            columns_for_table.append(TableColumn(field=column, title=column, formatter=formatter))
        else:
            columns_for_table.append(TableColumn(field=column, title=column))
        
    full_height=(26*len(df.index))
    data_table = DataTable(source=source, columns=columns_for_table,fit_columns=True,height=full_height,width_policy="auto",index_position=None)
        
    export_png(data_table, filename = path)

urls = ['https://ark-funds.com/wp-content/fundsiteliterature/csv/ARK_INNOVATION_ETF_ARKK_HOLDINGS.csv',
'https://ark-funds.com/wp-content/fundsiteliterature/csv/ARK_AUTONOMOUS_TECHNOLOGY_&_ROBOTICS_ETF_ARKQ_HOLDINGS.csv',
'https://ark-funds.com/wp-content/fundsiteliterature/csv/ARK_NEXT_GENERATION_INTERNET_ETF_ARKW_HOLDINGS.csv',
'https://ark-funds.com/wp-content/fundsiteliterature/csv/ARK_GENOMIC_REVOLUTION_MULTISECTOR_ETF_ARKG_HOLDINGS.csv',
'https://ark-funds.com/wp-content/fundsiteliterature/csv/ARK_FINTECH_INNOVATION_ETF_ARKF_HOLDINGS.csv',
'https://ark-funds.com/wp-content/fundsiteliterature/csv/ARK_SPACE_EXPLORATION_&_INNOVATION_ETF_ARKX_HOLDINGS.csv']

#twitter
api_key = ''
api_secret = ''
access_token = ''
secret_token = ''
twitter = Twython(
    api_key,
    api_secret,
    access_token,
    secret_token
)

#Get current date
today = date.today()
delta = 1
#If today is Monday, we need to compare to Friday
if date.today().weekday() == 0:
    delta = 3
yesterday = date.today() - timedelta(delta)

#Download files
for csv_url in urls:
    scraper = cloudscraper.create_scraper()
    req = scraper.get(csv_url)
    url_content = req.content
    csv_file = open('/tmp/downloaded.csv', 'wb')
    csv_file.write(url_content)
    csv_file.close()

    #Read downloaded csv
    csv_file = open('/tmp/downloaded.csv','r')
    df = pd.read_csv(url_content)
    print(df.fund.iloc[0])
    #Create for saving info
    df.to_csv('/tmp/'+df.fund.iloc[0]+'-'+today.strftime("%b-%d-%Y")+'.csv')

    df_yesterday = pd.read_csv('/tmp/'+df.fund.iloc[0]+'-'+yesterday.strftime("%b-%d-%Y")+'.csv')
    csv_file.close()
    #Merge two dataframes based on ticker
    merged = pd.merge(df_yesterday, df, on='ticker')
    merged['variation'] = np.where(merged['shares_x'] >= merged['shares_y'], merged['shares_y']-merged['shares_x'], merged['shares_y']-merged['shares_x'])
    merged['pct'] = np.where(merged['shares_x'] >= merged['shares_y'], ((merged['variation'] * 100)/merged['shares_x']), ((merged['variation'] * 100)/merged['shares_x']))
    #merged.to_csv('/mnt/c/Users/herna/OneDrive/Documentos/pyhton_ark_investments/ark/test'+df.fund.iloc[0]+'.csv')

    #We create final dataframe for converting to image later
    df_final = pd.DataFrame({
        "Company":[],
        "Ticker":[],
        "Total shares":[],
        "Change":[],
        "Variation":[],
        "%Variation":[]
    })
    hashtags = ' #ARKinsights #ARKstocks #ARKinvest #CathieWood #investing #stock #market'
    for index, row in merged.iterrows():
        try:
            if row.variation > 0:
                msg = row.fund_x + ' increases $' + row.ticker + ' position by ' + str(np.abs(int(row.variation))) + ' shares for a total of ' + str(np.abs(int(merged.shares_y.iloc[index]))) + hashtags
                if (len(row.ticker) >= 1):
                    new_row = pd.DataFrame({
                        "Company":[row.company_x],
                        "Ticker":[row.ticker],
                        "Total shares":[np.abs(int(row.shares_y))],
                        "Change":['BUY'],
                        "Variation":[row.variation],
                        "%Variation":[row.pct]
                    })
                    df_final = df_final.append(new_row)

            elif row.variation < 0:
                msg = row.fund_x + ' decreases $' + row.ticker + ' position by ' + str(np.abs(int(row.variation))) + ' shares for a total of ' + str(np.abs(int(merged.shares_y.iloc[index]))) + hashtags
                if (len(row.ticker) >= 1):
                    new_row = pd.DataFrame({
                        "Company":[row.company_x],
                        "Ticker":[row.ticker],
                        "Total shares":[np.abs(int(row.shares_y))],
                        "Change":['SELL'],
                        "Variation":[row.variation],
                        "%Variation":[row.pct]
                    })
                    df_final = df_final.append(new_row)
        except:
            print('ENDED')
    #Find new or closed positions
    df_closed = df.merge(df_yesterday, on=['ticker'], how = 'outer' ,indicator=True).loc[lambda x : x['_merge']=='right_only']
    #df_closed.to_csv("df_closed.csv")
    df_new = df.merge(df_yesterday, on=['ticker'], how = 'outer' ,indicator=True).loc[lambda x : x['_merge']=='left_only']
    #df_new.to_csv("df_new.csv")
    for index, row in df_closed.iterrows():
        try:
            #msg = row.fund_y + ' closed its position of $' + row.ticker + '. Its position was reported to be  ' + str(np.abs(int(row.shares_y))) + ' shares' + hashtags
            if (len(row.ticker) >= 1):
                new_row = pd.DataFrame({
                        "Company":[row.company_x],
                        "Ticker":[row.ticker],
                        "Total shares":[np.abs(int(row.shares_y))],
                        "Change":['CLOSED POSITION'],
                        "Variation":[row.variation],
                        "%Variation":[row.pct]
                    })
                df_final = df_final.append(new_row)
        except:
            print('ENDED')
    for index, row in df_new.iterrows():
        try:
            #msg = row.fund_x + ' opened a new position in $' + row.ticker + ' with  ' + str(np.abs(int(row.shares_x))) + ' shares' + hashtags
            if (len(row.ticker) >= 1):
                new_row = pd.DataFrame({
                        "Company":[row.company_x],
                        "Ticker":[row.ticker],
                        "Total shares":[np.abs(int(row.shares_x))],
                        "Change":['NEW POSITION'],
                        "Variation":[row.variation],
                        "%Variation":[row.pct]
                    })
                df_final = df_final.append(new_row)
        except:
            print('ENDED')
    try:
        #df_final.to_csv('/mnt/c/Users/herna/OneDrive/Documentos/pyhton_ark_investments/ark/FINAL_'+df.fund.iloc[0]+'.csv')
        df_final.set_index('Ticker', inplace=True)
        save_df_as_image(df_final, '/tmp/'+df.fund.iloc[0]+'.png')
        image = open('/tmp/'+df.fund.iloc[0]+'.png', 'rb')
        image_ids = twitter.upload_media(media=image)
        twitter.update_status(status='Daily report about ' + df.fund.iloc[0] + ' changes. ' + hashtags, media_ids=[image_ids['media_id']])
    except:
        twitter.update_status(status="I didn't detect any changes in " + df.fund.iloc[0] + '. ' + hashtags)
