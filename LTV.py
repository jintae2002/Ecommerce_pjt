import json
import pymysql
import pandas as pd


def makeCumsum(df) :
    df2 = pd.DataFrame(index = df.index, columns = df.columns)
    df2.iloc[:,0] = df.iloc[:,0]
    
    for i in range(len(df.columns)-1) : 
        df2.iloc[:,i+1] = df.iloc[:,i+1] + df2.iloc[:,i]
    
    return df2

def lambda_handler(event, context):

    conn = pymysql.connect(user = 'jintae2002', passwd = 'medici0330', host = 'database-2.c1udiifin44z.ap-northeast-2.rds.amazonaws.com',
                            db='kumekume')
                            
    kind = event['queryStringParameters']['kind']


    sql1 = f""" 
           select Q_First as q_first, Q_Diff as q_diff, round(sum(Sales)) as sums
           from Final_log1
           where kind ='{kind}'
           group by Q_First, Q_Diff
           order by Q_First, Q_Diff;
           """
           
           
    sql2 = f"""
           select count(distinct Customer_ID) as first_cohort
           from Final_log1 
           where Q_Diff=0 and kind ='{kind}'
           group by Q_First,Q_Diff;
           """
           
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    
    cursor.execute(sql1)
    
    r1 = cursor.fetchall()
    
    r1 = pd.DataFrame(r1)
    
    r2 = r1.pivot(index='q_first',columns='q_diff')

    r2 = r2['sums']
    r2.columns = list(map(lambda x : "q_diff_" + str(x), r2.columns))

    cursor.execute(sql2)
    r3 = cursor.fetchall()
    r3 = pd.DataFrame(r3, index = r2.index)

    r4 = makeCumsum(r2)
    r4 = r4.fillna(0)
    
    ltv = r4.div(r3.iloc[:,0], axis=0)
    ltv = round(ltv,2)
    ltv = ltv.reset_index().rename(columns={"index": "q_first"})
    
    ltv = ltv.to_dict("records")
    
    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin":"*",
        },
        "body": json.dumps(ltv),
    }