import json
import pymysql
import pandas as pd
import numpy as np


def classify_customers(sr) :
    sr2 = pd.Series(index=sr.index)
    for idx, col in enumerate(sr) :
        if col  == '444' :
            sr2[idx] = 'Best Customers'
        elif col == '244' :
            sr2[idx] = 'Almost Lost'
        elif col == '144' :
            sr2[idx] = 'Lost Customers'
        elif col == '111' or col in [f'1{s}{d}' for s in range(1,3) for d in range(1,3)] :
            sr2[idx] = 'Lost Cheap Customers'
        elif col in [f'4{s}{d}' for s in range(1,5) for d in range(1,5)] :
            sr2[idx] = "Recent Customers"
        elif col in [f'{s}4{d}' for s in range(1,5) for d in range(1,5)] :
            sr2[idx] = "Loyal Customers"
        elif col in [f'{s}{d}4' for s in range(1,5) for d in range(1,5)] :
            sr2[idx] = "Big Spenders"
        else :
            sr2[idx] = 'Others'
    return sr2
    
    
def lambda_handler(event, context):

    conn = pymysql.connect(user = 'jintae2002', passwd = 'medici0330', host = 'database-2.c1udiifin44z.ap-northeast-2.rds.amazonaws.com',
                            db='kumekume')
                            
    kind = event['queryStringParameters']['kind']

    sql1 = f""" 
           with user_rfm as (
   select Customer_ID,
      max(Order_Date) as Recent_Date,
        datediff(max(Order_Date),(select max(Order_Date) from Final_log1 where kind ='{kind}')) as Recency,
        count(distinct Order_Date) as Frequency,
        round(sum(Sales),2) as Monetary
    from Final_log1
    where kind='{kind}'
    group by Customer_ID
)
, RFM_RANK as (
   select Customer_ID,
         Recent_Date,
            Recency,
         Frequency,
            Monetary,
            NTILE(4) OVER(order by Recency) as R_score,
            NTILE(4) OVER(order by Frequency) as F_score,
            NTILE(4) OVER(order by Monetary) as M_score
   from user_rfm
) , RFM_Final as( 
   select *, (100*R_score + 10*F_score + 1*M_score) as RFM_Class
    from RFM_RANK
)
select *
from RFM_Final
order by Customer_ID;
           """
           
           
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    
    cursor.execute(sql1)
    
    df = cursor.fetchall()
    
    df = pd.DataFrame(df)
    
    df['RFM_Customer']=np.nan
    
    df[['RFM_Class']] = df[['RFM_Class']].astype(str)
    
    df['RFM_Customer'] = classify_customers(df['RFM_Class'])
    
    
    
    
    df1 = df.groupby('RFM_Customer')["Customer_ID"].count().to_frame()
    
    df1['purchase_period'] = df.groupby('RFM_Customer')["Recency"].mean().to_frame()
    
    df1 = round(df1)
    
    
    
    
    df1 = df1.reset_index().rename(columns={"index": "RFM_Customer"})
    
    # df1 = df1.pivot(index="RFM_Customer", columns="Customer_ID")
    
    # df1 = df1['Customer_ID']
    df1.columns = ["rfm_customer","count_c","purchase_period"]
    
    df1 = df1.to_dict("records")
    
    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin":"*",
        },
        "body": json.dumps(df1),
    }