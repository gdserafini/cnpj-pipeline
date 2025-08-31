SELECT * 
FROM read_parquet('/app/data/Cnaes/**/*.parquet')
WHERE year = ? AND month = ?
LIMIT 5;