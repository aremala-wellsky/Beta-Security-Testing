CREATE OR REPLACE FUNCTION qlik_build_call_answer_pivot_views(
    _delta_date character varying,
    _call_record_date character varying,
    _types VARCHAR[])
  RETURNS void AS
$BODY$
DECLARE
    _type VARCHAR;
    _dsql TEXT;
    _question_query TEXT;
    _inner_query TEXT;
    _final_query TEXT;
    _call_limit VARCHAR;
BEGIN
    -- Version 20200605-1

    _types := CASE WHEN ($3 IS NULL) THEN ARRAY['call', 'call_followup'] ELSE $3 END;

    FOREACH _type IN ARRAY _types LOOP
        _call_limit := CASE WHEN _type = 'call' THEN 'qaa.date_effective::DATE <= cr.start_date::DATE' ELSE 'cr.actual_followup_date IS NOT NULL AND qaa.date_effective::DATE <= cr.actual_followup_date::DATE' END;
    
        _question_query := 'SELECT DISTINCT virt_field_name FROM qlik_'||_type||'_answers 
        UNION SELECT DISTINCT virt_field_name FROM qlik_answer_access qaa ORDER BY 1';

        _inner_query := 'SELECT DISTINCT ON (tier_link, call_record_id, virt_field_name) tier_link||''''|''''||call_record_id AS sec_key, virt_field_name, answer_val
                 FROM (
                 SELECT DISTINCT cr.call_record_id, tier_link, virt_field_name, answer_val, date_effective 
                 FROM qlik_'||_type||'_answers qea
                 JOIN sp_call_record cr USING (call_record_id)
                 JOIN (SELECT DISTINCT tier_link, provider_id AS provider_creating_id FROM qlik_user_access_tier_view t WHERE t.user_access_tier = 1) uat USING (provider_creating_id)
                 WHERE end_date IS NULL OR end_date::DATE >= '''''||$2||'''''::DATE
                 UNION
                 SELECT DISTINCT cr.call_record_id, uat.tier_link, virt_field_name, answer_val, date_effective
                 FROM qlik_call_answer_access qaa
                 JOIN sp_call_record cr ON (qaa.call_record_id = cr.call_record_id AND '||_call_limit||')
                 JOIN (SELECT DISTINCT tier_link, provider_id AS provider_creating_id FROM qlik_user_access_tier_view t WHERE t.user_access_tier = 1) uat USING (provider_creating_id)
                 WHERE end_date IS NULL OR end_date::DATE >= '''''||$2||'''''::DATE 
                   AND (cr.provider_creating_id = qaa.provider_id
                   OR (qaa.visibility_id IS NOT NULL 
                       AND EXISTS (SELECT 1 FROM qlik_answer_vis_provider qap WHERE qap.visibility_id = qaa.visibility_id AND qap.provider_id = qaa.provider_id)))
                 ) t
                 ORDER BY call_record_id, tier_link, virt_field_name, date_effective DESC';

        _dsql := 'SELECT FORMAT(
        $$
          SELECT * FROM crosstab('''||_inner_query||''', '''||_question_query||''')
        AS
        (
            sec_key VARCHAR,
            %s
        )
        $$,
        string_agg(FORMAT(''%I %s'', upper(virt_field_name)||''_'||_type||''', ''TEXT''), '', '' ORDER BY virt_field_name)
    )
    FROM (
        '||_question_query||'
    )
        AS t';

        RAISE NOTICE 'Creating the pivot query %',clock_timestamp();
        EXECUTE _dsql INTO _final_query;
        RAISE NOTICE 'Finished creating pivot query %: %', _type, clock_timestamp();

        EXECUTE 'DROP MATERIALIZED VIEW IF EXISTS qlik_'||_type||'_answer_pivot_view';
        EXECUTE 'CREATE MATERIALIZED VIEW qlik_'||_type||'_answer_pivot_view AS '||_final_query;
        RAISE NOTICE 'Finished creating pivot view %: %', _type, clock_timestamp();
        
        EXECUTE 'ALTER TABLE qlik_'||_type||'_answer_pivot_view OWNER TO sp5user';
    END LOOP;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;

-- select qlik_build_call_answer_pivot_views('2015-01-01', '2015-01-01', ARRAY['call']);
-- select * from qlik_entry_answer_pivot_view;