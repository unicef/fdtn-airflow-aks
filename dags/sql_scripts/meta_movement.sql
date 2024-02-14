
TRUNCATE public.meta_population_crisis_adm2 ;

-- admin2 mapping + group by 

INSERT INTO public.meta_population_crisis_adm2 ( 

with population_meta_grouped_h308 as (

select disaster_id, disaster_name, country, h3_08, date, date_time, sum(n_baseline) as n_baseline, sum(n_crisis) as n_crisis, sum(n_difference) as n_difference
,sum(case when n_difference > 0
then n_difference 
else 0
end)  as n_difference_positive

,sum(case when n_difference < 0
then n_difference 
else 0
end)  as n_difference_negative

FROM private.meta_population_crisis_h308 mpch
group by 1,2,3,4,5,6

)
,

population_meta_joined_hrsl as ( 
select pmg.*, pcd.general as hrsl_population 
from population_meta_grouped_h308 pmg
left join public.population_crosstab_deduplicated pcd
on pmg.h3_08=pcd.h3_08
)
, 

population_joined_adm as (

SELECT pmjh.disaster_id
,pmjh.disaster_name
,pmjh.country
,pmjh.date
,pmjh.date_time

, ah.name0 as adm0
, ah.name1 as adm1
, ah.name2 as adm2

, ah.gid0 as gid0
, ah.gid1 as gid1
, ah.gid2 as gid2

,sum(pmjh.n_difference) as n_difference
,sum(pmjh.n_baseline) as n_baseline
,sum(pmjh.n_crisis) as n_crisis
,sum(pmjh.hrsl_population) as hrsl_population
,sum(pmjh.n_difference_positive) as n_difference_positive
,sum(pmjh.n_difference_negative) as n_difference_negative



FROM population_meta_joined_hrsl pmjh

left join  public.adm2_hex ah
on ah.h3_08= pmjh.h3_08 
group by 1,2,3,4,5,6,7,8,9,10,11
)
select * from population_joined_adm ); 

CREATE INDEX ON public.meta_population_crisis_adm2(adm2) ;


--- 



---- mapping meta/gdacs events / keep only the latest date 

TRUNCATE public.meta_population_crisis_slider_formatted;

INSERT INTO public.meta_population_crisis_slider_formatted(

with max_time_per_date_table as (

select disaster_id, date, date_time, gid0, gid1, gid2 
,row_number() over( partition by disaster_id, gid0, gid1, gid2 ,date order by date_time desc ) as rn
from public.meta_population_crisis_adm2
group by 1,2,3,4,5,6

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
(select mapping_meta_final.event_id, mpca.date, mpca.date_time, gdacs_name,meta_disaster_name , mpca.adm0, mpca.gid0, mpca.adm1, mpca.gid1,  mpca.adm2, mpca.gid2, n_difference, n_baseline, n_crisis,hrsl_population,

case when hrsl_population<= n_baseline then n_difference
when (n_baseline=0 or n_baseline is null) then n_difference
else  n_difference *( hrsl_population/n_baseline ) 
end 
as n_difference_scaled

,n_difference_positive
,n_difference_negative

from public.meta_population_crisis_adm2 mpca


left join max_time_per_date_table md 
on md.disaster_id=mpca.disaster_id 
and md.gid0 = mpca.gid0
and md.gid1 = mpca.gid1
and md.gid2 = mpca.gid2
and md.date_time = mpca.date_time



left join mapping_meta_final
on mpca.disaster_id=mapping_meta_final.meta_disaster_id


where md.rn=1 ), 

-- make sure we have all days x all adm2 to have a consistent data structure for the slider fin the front end 

all_days as (select event_id, "date" from grouped_data group by 1,2), 

all_adm2 as (select event_id, adm0, gid0, adm1, gid1,  adm2, gid2 from grouped_data group by 1,2,3,4,5,6,7),

all_days_adm2 as (select all_days."date", all_adm2.*  from all_days left join all_adm2 on all_days.event_id = all_adm2.event_id)



select ad.event_id,ad."date", gd.date_time, gd.gdacs_name, gd.meta_disaster_name, ad.adm0, ad.gid0, ad.adm1, ad.gid1, ad.adm2, ad.gid2,  gd.n_difference, gd.n_baseline, gd.n_crisis,gd.hrsl_population,gd.n_difference_scaled, gd.n_difference_positive, gd.n_difference_negative 
from all_days_adm2 ad left join grouped_data gd 
on ad.event_id=gd.event_id and ad.gid2=gd.gid2 and ad.gid1=gd.gid1 and ad.gid0=gd.gid0 and ad."date"=gd."date"
)



;
CREATE INDEX ON public.meta_population_crisis_slider_formatted(adm2)
