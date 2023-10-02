
TRUNCATE public.meta_population_crisis_adm2 ;

-- admin2 mapping + group by 

INSERT INTO public.meta_population_crisis_adm2(
SELECT mpch.disaster_id
,mpch.disaster_name
,mpch.country
,mpch.date

, ah.name0 as adm0
, ah.name1 as adm1
, ah.name2 as adm2

, ah.gid0 as gid0
, ah.gid1 as gid1
, ah.gid2 as gid2

,sum(mpch.n_difference) as n_difference
,sum(mpch.n_baseline) as n_baseline
,sum(mpch.n_crisis) as n_crisis

FROM private.meta_population_crisis_h308 mpch 

left join  public.adm2_hex ah
on ah.h3_08= mpch.h3_08 

group by 1,2,3,4,5,6,7,8,9,10
); 

CREATE INDEX ON public.meta_population_crisis_adm2(amd2) ;

---- mapping meta/gdacs events / keep only the latest date 

TRUNCATE public.meta_population_crisis_formatted;

INSERT INTO public.meta_population_crisis_formatted(

with max_date_table as (
select disaster_id
, max(date) as max_date
from public.meta_population_crisis_adm2
group by 1
),

mapping_meta_inter as (
select dh.event_id, d.name as gdacs_name,  d.fromdate, d.todate, mpch.disaster_id as meta_disaster_id, mpch.disaster_name as meta_disaster_name ,count(*) as occurences

 ,row_number() over(partition by dh.event_id order by count(*) desc) as rn
from public.disasters_hex dh 

left join public.disasters d 
on dh.event_id=d.event_id

left join private.meta_population_crisis_h308 mpch 
on mpch.h3_08=dh.h3_08


where TRUE 
and mpch.date<= d.todate + INTERVAL '28 day' 
and mpch.date >= d.fromdate - INTERVAL '3 day'
and mpch.disaster_id is not null
group by 1,2,3,4,5,6
), 
  
mapping_meta_final as (select * from mapping_meta_inter where rn=1) ,

grouped_data as 
(select mapping_meta_final.event_id, date, gdacs_name,meta_disaster_name , adm2, gid2, n_difference, n_baseline, n_crisis
from public.meta_population_crisis_adm2 mpca

left join max_date_table md 
on md.disaster_id=mpca.disaster_id

left join mapping_meta_final
on mpca.disaster_id=mapping_meta_final.meta_disaster_id


where mpca.date= md.max_date )

select * from grouped_data

);
CREATE INDEX ON public.meta_population_crisis_formatted(adm2)
; 
