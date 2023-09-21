-- set up public table with adm 0/1/2 levels 

TRUNCATE public.meta_movement;

INSERT INTO public.meta_movement(
SELECT mm.disaster_id
,mm.disaster_name
,mm.country
,mm.date_time
, ah_start.name0 as start_adm0
, ah_start.name1 as start_adm1
, ah_start.name2 as start_adm2

, ah_end.name0 as end_adm0
, ah_end.name1 as end_adm1
, ah_end.name2 as end_adm2
,sum(mm.n_difference) as n_difference

FROM private.meta_movement_h308 mm 

left join  public.adm2_hex ah_start
on ah_start.h3_08= mm.start_h3_08 

left join  public.adm2_hex ah_end
on ah_end.h3_08= mm.end_h3_08 

-- inner join to make sure we only keep movements coming from the disaster zone
inner join public.disasters_hex dh 
on dh.h3_08 = mm.start_h3_08 

group by 1,2,3,4,5,6,7,8,9,10 )
;

CREATE INDEX ON public.meta_movement(start_adm2)
; 

-- set up public table with adm 2 flows 
-- keep only adm 2, data for the first 72hours, and movements >0


TRUNCATE public.meta_movement_formatted;

INSERT INTO public.meta_movement_formatted(

with min_date as (
select disaster_id
, min(date_time) as start_date
from public.meta_movement
group by 1
),

mapping_meta_inter as (
select dh.event_id, d.name,  d.fromdate, d.todate, mmh.disaster_id , mmh.data_type, count(*) as occurences

 ,row_number() over(partition by mmh.disaster_id order by count(*) desc) as rn
from public.disasters_hex dh 

left join public.disasters d 
on dh.event_id=d.event_id

left join private.meta_movement_h308 mmh 
on mmh.start_h3_08=dh.h3_08

where TRUE 
and mmh.date_time<= d.todate + INTERVAL '28 day' 
and mmh.date_time >= d.fromdate - INTERVAL '3 day'
and mmh.disaster_id is not null
group by 1,2,3,4,5,6
), 
  
mapping_meta_final as (select * from mapping_meta_inter where rn=1) ,

grouped_data as 
(select mapping_meta_final.event_id, start_date, disaster_name, start_adm2, end_adm2, sum(mm.n_difference) as total
from public.meta_movement mm

left join min_date md 
on md.disaster_id=mm.disaster_id

left join mapping_meta_final
on mm.disaster_id=mapping_meta_final.disaster_id

where date_time - start_date <= interval '3 days'
and end_adm2 != start_adm2
group by 1,2,3,4,5)

select * from grouped_data
where total >0
)
  
;

CREATE INDEX ON public.meta_movement_formatted(start_adm2)
; 
