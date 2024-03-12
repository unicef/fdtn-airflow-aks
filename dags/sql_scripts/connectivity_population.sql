
TRUNCATE public.population_connectivity_status ;

INSERT INTO public.population_connectivity_status (    
       
with event_days_connectivity as (select event_id, date from public.meta_connectivity_test_slider group by 1,2), 

connectivity_all_days_all_h308 as (select dh.event_id, dh.h3_08, edc.date from disasters_hex dh 
left join event_days_connectivity edc on dh.event_id= edc.event_id),

connectivity_slider_h3_07 as (select mcts.*, hm.h3_07 from meta_connectivity_test_slider mcts
left join public.h3_07_08_mapping hm on mcts.h3_08 = hm.h3_08 ),

connectivity_slider_surroundings_h3_08 as ( select cs7.*, hm.h3_08 as h3_08_surroundings from connectivity_slider_h3_07 cs7
left join public.h3_07_08_mapping hm on cs7.h3_07 = hm.h3_07 ),

deduplicated_connectivity_slider_surroundings_h3_08 as ( select event_id,date,h3_08 as h3_08_meta, h3_08_surroundings, min(avg_p_connectivity) as avg_p_connectivity from connectivity_slider_surroundings_h3_08 cs8
group by 1,2,3,4 )


select cadah.event_id, cadah.date, cs8_dedup.avg_p_connectivity, sum(wp.general) as general from connectivity_all_days_all_h308 cadah

left join deduplicated_connectivity_slider_surroundings_h3_08 cs8_dedup on (cadah.h3_08= cs8_dedup.h3_08_surroundings and cadah.event_id = cs8_dedup.event_id and cadah.date=cs8_dedup.date)
left join public.worldpop_h3_08 wp on  cadah.h3_08 = wp.h3_08 
group by 1,2,3)
