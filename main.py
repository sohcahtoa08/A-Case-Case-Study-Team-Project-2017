import psycopg2
import sys
from parser import parseCase
from concurrent.futures import ThreadPoolExecutor, as_completed
from psycopg2.pool import ThreadedConnectionPool

# EXAMPLE: python3 main.py postgresql://user:pass@localhost/db 10

# Set default case limit per query
limit = 1000

# Parse params
args = sys.argv[1:]

def main():
    global limit, tcp

    # This is only for testing
    if len(args) > 1 and args[1] == 'test':
        insertCase(open('test.html', 'r').read())
    else:
        # Create conn pool
        tcp = ThreadedConnectionPool(1, 10, args[0])

        # Apply case limit
        if len(args) > 1 and args[1].isdigit():
            limit = int(args[1])

        # Iterate thru cases
        with ThreadPoolExecutor(max_workers=50) as pool:
            for i in range(50):
                pool.submit(doParsing, (limit + 1) * i)

# Field tuples for each table
TABLE_COLS = {
    'cases': ('case_id', 'title', 'court_system', 'type', 'filing_date', 'status', 'disposition', 'disposition_date', 'violation_county', 'violation_date'),
    'parties': ('case_id', 'name', 'type', 'bus_org_name', 'agency_name', 'race', 'sex', 'height', 'weight', 'dob', 'address', 'city', 'state', 'zip'),
    'attorneys': ('case_id', 'name', 'type', 'appearance_date', 'removal_date', 'practice_name', 'address', 'city', 'state', 'zip'),
    'events': ('case_id', 'type', 'date', 'time', 'result', 'result_date'),
    'charges': ('case_id', 'statute_code', 'description', 'offense_date_from', 'offense_date_to', 'class', 'amended_date', 'cjis_code', 'probable_cause', 'victim_age', 'speed_limit', 'recorded_speed', 'location_stopped', 'accident_contribution', 'injuries', 'property_damage', 'seatbelts_used', 'mandatory_court_appearance', 'vehicle_tag', 'state', 'plea', 'plea_date', 'disposition', 'disposition_date', 'jail_extreme_punishment', 'jail_term', 'jail_suspended_term', 'jail_unsuspended_term', 'probation_term', 'probation_supervised_term', 'probation_unsupervised_term', 'fine_amt', 'fine_suspended_amt', 'fine_restitution_amt', 'fine_due', 'fine_first_pmt_due', 'cws_hours', 'cws_deadline', 'cws_location', 'cws_date'),
    'documents': ('case_id', 'name', 'filing_date'),
    'judgements': ('case_id', 'against', 'in_favor_of', 'type', 'date', 'interest', 'amt'),
    'complaints': ('case_id', 'type', 'against', 'status', 'status_date', 'filing_date', 'amt')
}

def doParsing(offset):
    # Connect to DB
    conn = tcp.getconn()
    cur = conn.cursor()

    # Get raw case HTML where we haven't already parsed it
    cur.execute('SELECT rawcases.case_id, html FROM rawcases LEFT OUTER JOIN cases ON rawcases.case_id = cases.case_id WHERE cases.case_id IS NULL LIMIT %s OFFSET %s', (limit, offset))

    # Iterate thru cases
    results = cur.fetchall()
    for i in range(len(results)):
        print('[%s] %s remaining' % (results[i][0], len(results) - i))
        insertCase(cur, conn, *results[i])

    # Disconnect from DB
    tcp.putconn(conn)

# Insert all the data for a case
def insertCase(cur, conn, raw_case_id, html):
    # Parse HTML
    data = parseCase(html)

    # Store case ID
    try:
        case_id = data['cases'][0]['case_id']
    except KeyError:
        # Delete the case if it's nonsense
        cur.execute('DELETE FROM rawcases WHERE case_id = %s', (raw_case_id, ))
        print('[%s] Deleted: nonsense' % raw_case_id)
        conn.commit()
        return

    # Insert data for each section/table
    def insertData(table):
        rows = []
        dataFields = TABLE_COLS[table]
        for entry in data[table]:
            # Get the value for a field
            def getFieldValue(field):
                if field == 'case_id':
                    return case_id
                else:
                    return entry.get(field) or None
            # Build tuple of col values
            dataTuple = tuple(getFieldValue(field) for field in dataFields)
            rows.append(cur.mogrify('(' + '%s, ' * (len(dataFields) - 1) + '%s)', dataTuple).decode('utf-8'))
        # Batch execute query
        insertText = ','.join(rows)
        try:
            cur.execute('INSERT INTO ' + table + ' ' + str(dataFields).replace('\'', '') + ' VALUES ' + insertText)
            print('[%s] Inserted: %s (%s)' % (case_id, table, len(rows)))
        except psycopg2.IntegrityError as error:
            if 'duplicate' in str(error):
                conn.rollback()
                print('[%s] Error inserting %s row: %s' % (case_id, table, str(error)))
                cur.execute('DELETE FROM rawcases WHERE case_id = %s', (raw_case_id,))
                print('[%s] Deleted: duplicate' % raw_case_id)
                conn.commit()
            else:
                raise error

    insertData('cases')
    for table in data:
        if table != 'cases':
            insertData(table)

    # Commit changes to DB
    conn.commit()

if __name__ == '__main__': main()
