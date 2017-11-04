import sys
import psycopg2
import json

def getCaseTypes(t):
    # Get types for given court system from DB
    if t == 'civil':
        cur.execute("SELECT type FROM cases WHERE LOWER(court_system) LIKE '%civil%' AND LOWER(court_system) NOT LIKE '%civil citation%' GROUP BY type")
    else:
        cur.execute("SELECT type FROM cases WHERE LOWER(court_system) LIKE '%" + t + "%' GROUP BY type")
    # Build list of types from results
    results = cur.fetchall()
    return [r[0] for r in results][1:]

def main():
    global cur, conn

    # Connect to DB
    args = sys.argv[1:]
    conn = psycopg2.connect(host=args[0], database=args[1], user=args[2], password=args[3])
    cur = conn.cursor()

    # Get all case types
    types = {t: getCaseTypes(t) for t in {'civil', 'criminal', 'traffic', 'civil citation'}}

    # Write to file
    with open('types.json', 'w') as f:
        f.write(json.dumps(types))

if __name__ == '__main__': main()
