-- TRUNCATE TABLE AND INSERT

TRUNCATE public.meta_connectivity_test_slider;

INSERT INTO public.meta_connectivity_test_slider (
select event_id, h3_08, date, 

AVG(CASE WHEN data_type='coverage' then value 
ELSE NULL 
end) as "avg_coverage", 

AVG(CASE WHEN data_type='no_coverage' then value 
ELSE NULL 
end) as "avg_no_coverage", 

AVG(CASE WHEN data_type='p_connectivity' then value 
ELSE NULL 
end) as "avg_p_connectivity"

FROM 

(select d_hex.event_id, d.name, d.fromdate, d.todate, d_hex.h3_08 , mch.data_type, mch.date, mch.value, mch.meta_disaster_id,
row_number() over(partition by d_hex.event_id,d_hex.h3_08,mch.data_type order by date desc) as rn

from public.disasters_hex d_hex     

left join public.disasters d 
on d_hex.event_id=d.event_id

left join private.meta_connectivity_hex mch
on d_hex.h3_08=mch.h3_08

where TRUE 
and mch.date<= d.todate + INTERVAL '28 day'
and mch.date >= d.fromdate

) as sub



GROUP by 1,2,3)
;

CREATE INDEX ON public.meta_connectivity_test_slider(h3_08)
