import json
import pymysql
import pandas as pd

def lambda_handler(event, context):

    conn = pymysql.connect(user = 'jintae2002', passwd = 'medici0330', host = 'database-2.c1udiifin44z.ap-northeast-2.rds.amazonaws.com',
                            db='kumekume')
                            
    kind = event['queryStringParameters']['kind']
    
    sql1 = f"""
           select date_format(Order_Date, '%Y-%m') as orderdate, sum(sales), appearance
            from Final_log1
            where kind = '{kind}'
            group by appearance, orderdate
            order by orderdate, appearance
           """


    cursor = conn.cursor(pymysql.cursors.DictCursor)
    
    cursor.execute(sql1)
    
    r1 = cursor.fetchall()
    
    r1 = pd.DataFrame(r1)
    r2 = r1.pivot(index='orderdate',columns='appearance')
    
    r2 = r2.fillna(0)
    r2 = r2['sum(sales)']
    
    r2 = r2.astype('float')
    
    r2.columns = list(map(lambda x : "app_" + str(x), r2.columns))
    
    r2 = r2.reset_index().rename(columns={"index": "appearance"})
    
    r2.columns.name = None
    
    
    r2 = r2.to_dict("records")
    
    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin":"*",
        },
        "body": json.dumps(r2),
    }