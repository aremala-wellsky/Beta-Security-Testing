-- Time: 34 minutes for both

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
    _ee_join VARCHAR;
    _ee_where VARCHAR;
BEGIN
    -- Version 20200609-1

    _types := CASE WHEN ($3 IS NULL) THEN ARRAY['entry', 'exit'] ELSE $3 END;

    CREATE TEMP TABLE tmp_relevant_ees AS
    SELECT entry_exit_id, tier_link, client_id, entry_date, exit_date, provider_id
    FROM qlik_ee_user_access_tier_view uat
    WHERE uat.user_access_tier != 1 AND (exit_date IS NULL OR exit_date::DATE >= $2::DATE);

    CREATE TEMP TABLE tmp_qlik_vis_provider AS
    SELECT visibility_id, vgpt.provider_id
    FROM qlik_answer_vis_array qav 
    JOIN sp_visibility_group_provider_tree vgpt ON (vgpt.visibility_group_id = ANY(allow_ids))
    JOIN (SELECT DISTINCT provider_id FROM qlik_user_access_tier_view WHERE user_access_tier != 1) up ON (vgpt.provider_id = up.provider_id)
    EXCEPT
    SELECT visibility_id, vgpt.provider_id
    FROM qlik_answer_vis_array qav 
    JOIN sp_visibility_group_provider_tree vgpt ON (vgpt.visibility_group_id = ANY(deny_ids))
    JOIN (SELECT DISTINCT provider_id FROM qlik_user_access_tier_view WHERE user_access_tier != 1) up ON (vgpt.provider_id = up.provider_id);

    FOREACH _type IN ARRAY _types LOOP
        _ee_join := CASE WHEN _type = 'exit' 
                         THEN '(ee.exit_date IS NULL OR qaa.date_effective::DATE <= ee.exit_date::DATE)' 
                         ELSE 'qaa.date_effective::DATE <= ee.entry_date::DATE' END;

        _ee_where := CASE WHEN _type = 'exit' 
                          THEN 'exit_date IS NULL OR exit_date::DATE >= '''''||$2||'''''::DATE' 
                          ELSE 'entry_date::DATE >= '''''||$2||'''''::DATE' END;
    
        _question_query := 'SELECT DISTINCT virt_field_name FROM qlik_'||_type||'_answers 
        UNION SELECT DISTINCT virt_field_name FROM qlik_answer_access qaa ORDER BY 1';

        _inner_query := 'SELECT DISTINCT ON (tier_link, entry_exit_id, virt_field_name) tier_link||''''|''''||entry_exit_id AS sec_key, virt_field_name, answer_val
         FROM (
         -- Tier 1 - Top answers
         SELECT DISTINCT ee.entry_exit_id, tier_link, virt_field_name, answer_val, date_effective 
         FROM qlik_'||_type||'_answers qea
         JOIN (SELECT DISTINCT tier_link, entry_exit_id FROM qlik_ee_user_access_tier_view t WHERE t.user_access_tier = 1) ee USING (entry_exit_id)
         UNION
         -- Tier 2/3 - INHERITED
         SELECT DISTINCT ee.entry_exit_id, ee.tier_link, virt_field_name, answer_val, date_effective
         FROM qlik_answer_access qaa 
         JOIN tmp_relevant_ees ee ON (ee.provider_id = qaa.provider_id AND ee.client_id = qaa.client_id AND '||_ee_join||')
         UNION
         -- Tier 2/3 - EXPLICIT
         SELECT DISTINCT ee.entry_exit_id, uat.user_access_tier||''''|''''||qavp.provider_id AS tier_link, virt_field_name, answer_val, date_effective
         FROM qlik_answer_access qaa 
         JOIN tmp_relevant_ees ee ON (ee.client_id = qaa.client_id AND '||_ee_join||')
         JOIN tmp_qlik_vis_provider qavp USING (visibility_id)
         CROSS JOIN (SELECT DISTINCT user_access_tier FROM qlik_user_access_tier_view WHERE user_access_tier != 1) uat
         WHERE '||_ee_where||') t
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

        RAISE NOTICE 'Creating the pivot query %', clock_timestamp();
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