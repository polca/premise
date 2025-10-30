import gcamreader 
import os, re

dbpath = r'database'
scenario = 'ssp24p5tol5'

conn = gcamreader.LocalDBConn(dbpath=dbpath, dbfile=scenario)

query_files = [f for f in os.listdir('queries') if f.endswith('.xml') and not re.search('nrel', f)]

print(query_files)

for qf in query_files:
    queries = gcamreader.parse_batch_query(os.path.join('queries', qf))

    for q in queries:
        print(f'Running query: {q.title}')
        db = conn.runQuery(q)
        if not os.path.exists('queries/queryresults/' + scenario):
            os.mkdir('queries/queryresults/' + scenario)
        db.to_csv(os.path.join('queries/queryresults', scenario, q.title + '.csv'), index=False)
