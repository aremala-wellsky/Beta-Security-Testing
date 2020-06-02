CREATE OR REPLACE FUNCTION qlik_build_answer_pivot_views(
    _delta_date character varying,
    _entry_exit_date character varying,
    _types VARCHAR[])
  RETURNS void AS
$BODY$
DECLARE
    _type VARCHAR;
    _dsql TEXT;
    _question_query TEXT;
    _inner_query TEXT;
    _final_query TEXT;
    _ee_limit VARCHAR;
BEGIN
    -- Version 20200602-1

    _types := CASE WHEN ($3 IS NULL) THEN ARRAY['entry', 'exit'] ELSE $3 END;

    CREATE TEMP TABLE tmp_relevant_ees AS
    SELECT entry_exit_id, tier_link, client_id, entry_date, exit_date, provider_id
    FROM qlik_ee_user_access_tier_view uat
    WHERE uat.user_access_tier != 1 AND (exit_date IS NULL OR exit_date::DATE >= $2::DATE);

    FOREACH _type IN ARRAY _types LOOP
        _ee_limit := CASE WHEN _type = 'exit' THEN '(ee.exit_date IS NULL OR qaa.date_effective::DATE <= ee.exit_date::DATE)' ELSE 'qaa.date_effective::DATE <= ee.entry_date::DATE' END;
    
        _question_query := 'SELECT DISTINCT virt_field_name FROM qlik_'||_type||'_answers 
        UNION SELECT DISTINCT virt_field_name FROM qlik_answer_access qaa ORDER BY 1';

        _inner_query := 'SELECT DISTINCT ON (tier_link, entry_exit_id, virt_field_name) tier_link||''''|''''||entry_exit_id AS sec_key, virt_field_name, answer_val
                 FROM (
                 SELECT DISTINCT ee.entry_exit_id, tier_link, virt_field_name, answer_val, date_effective 
                 FROM qlik_'||_type||'_answers qea
                 JOIN (SELECT DISTINCT tier_link, entry_exit_id FROM qlik_ee_user_access_tier_view t WHERE t.user_access_tier = 1) ee USING (entry_exit_id)
                 UNION
                 SELECT DISTINCT ee.entry_exit_id, ee.tier_link, virt_field_name, answer_val, date_effective
                 FROM qlik_answer_access qaa 
                 JOIN tmp_relevant_ees ee ON (ee.client_id = qaa.client_id AND '||_ee_limit||')
                 WHERE ee.provider_id = qaa.provider_id
                   OR (qaa.visibility_id IS NOT NULL 
                       AND EXISTS (SELECT 1 FROM qlik_answer_vis_provider qap WHERE qap.visibility_id = qaa.visibility_id AND qap.provider_id = qaa.provider_id))
                 ) t
                 ORDER BY entry_exit_id, tier_link, virt_field_name, date_effective DESC';

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

-- select qlik_build_answer_pivot_views('2015-01-01', '2015-01-01', null);
-- select * from qlik_entry_answer_pivot_view;