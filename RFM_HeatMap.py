import json
import pymysql
import pandas as pd

def lambda_handler(event, context):

    conn = pymysql.connect(user = 'jintae2002', passwd = 'medici0330', host = 'database-2.c1udiifin44z.ap-northeast-2.rds.amazonaws.com',
                            db='kumekume')
                            
    kind = event['queryStringParameters']['kind']

    sql1 = f""" 
           select Q_First as q_first, Q_Diff as q_diff, count(distinct `Customer_ID`) as customer_id
           from Final_log1
           where kind = '{kind}'
           group by Q_First, Q_Diff;
           """
           
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    
    cursor.execute(sql1)
    
    r1 = cursor.fetchall()
    
    r1 = pd.DataFrame(r1)
    r2 = r1.pivot(index='q_first',columns='q_diff')
    
    r2 = r2.fillna(0)
    r2 = r2['customer_id']
    
    r2 = r2.astype('float')
    
    r2.columns = list(map(lambda x : "q_diff_" + str(x), r2.columns))
    r2 = r2.div(r2.iloc[:,0],axis=0)
    
    r2 = r2*100
    
    r2 = round(r2,2)
    
    r2 = r2.reset_index().rename(columns={"index": "q_first"})
    
    r2.columns.name = None
    
    # print(r2)
    
    r2 = r2.to_dict("records")
    
    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin":"*",
        },
        "body": json.dumps(r2),
    }
    