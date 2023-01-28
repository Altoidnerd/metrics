


conp = get_pymysql_con()

def get_speeds():
    sdf = pd.read_sql('''SELECT
        d.$user_id, 
        avg(d.response_bytes/d.duration) speed,
        count(distinct(date)) active_days
    FROM (
        SELECT $user_id, date,
        response_bytes,
        CASE WHEN duration_ms='<null>' THEN null ELSE duration_ms END duration
        FROM fpa.events) d
    GROUP BY
        $user_id
    ORDER BY
        active_days, speed DESC;''', conp)

    return sdf