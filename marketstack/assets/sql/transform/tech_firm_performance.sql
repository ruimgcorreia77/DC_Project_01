select 
	TO_CHAR(TO_TIMESTAMP(timestamp, 'YYYY-MM-DD"T"HH24:MI:SS"+00:00"'), 'YYYY-MM') AS year_month
	,ticker_name
	,stock_exchange
	,max(open) highest_open
	,min(close) lowest_close
	,sum(volume) total_volume
	,avg (volume) average_volume
from public.marketstack_eods_tech
	group by 1,2,3
	order by 1 asc