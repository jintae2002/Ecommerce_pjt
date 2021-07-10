import json
import pymysql


def lambda_handler(event, context):

    conn = pymysql.connect(user = 'jintae2002', passwd = 'medici0330', host = 'database-2.c1udiifin44z.ap-northeast-2.rds.amazonaws.com',
                            db='kumekume')
    # kind = event['kind']['G']
    # kind = event['kind']
    kind = event['queryStringParameters']['kind']
    print(event)
    
    sql1 = f""" 
           select date_format(Order_Date, '%Y-%m') as orderdate, SUM(Sales) as sum_s, COUNT(Sales) as count_s
           from Final_log1
           where kind = '{kind}'
           group by orderdate
           order by orderdate
           """
    print(sql1)
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    
    cursor.execute(sql1)
    
    r1 = cursor.fetchall()
    
    # r1 = json.dumps(r1, ensure_ascii = False)
    # print(event['kind']['G'])
    
    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin":"*",
        },
        "body": json.dumps(r1),
    }
        
