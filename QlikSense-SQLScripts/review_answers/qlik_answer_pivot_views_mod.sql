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
    _lower_tier_join VARCHAR;
    _upper_tier_join VARCHAR := 'JOIN (SELECT DISTINCT tier_link, entry_exit_id FROM qlik_ee_user_access_tier_view t WHERE t.user_access_tier = 1) ee USING (entry_exit_id)';
    _primary_key VARCHAR := 'ee.entry_exit_id';
    _additional_col VARCHAR := '';
    _additional_col_headers VARCHAR := '';
BEGIN
    _types := CASE WHEN ($3 IS NULL) THEN ARRAY['entry', 'exit', 'review'] ELSE $3 END;

    DROP TABLE IF EXISTS tmp_relevant_ees;

    CREATE TEMP TABLE tmp_relevant_ees AS
    SELECT entry_exit_id, tier_link, client_id, entry_date, exit_date, provider_id
    FROM qlik_ee_user_access_tier_view uat
    WHERE uat.user_access_tier != 1 AND (exit_date IS NULL OR exit_date::DATE >= $2::DATE);

    FOREACH _type IN ARRAY _types LOOP
        
        IF _type = 'review' THEN
            _primary_key := 'entry_exit_review_id';
            _additional_col := ', entry_exit_review_pit_type, entry_exit_review_type, entry_exit_review_date ';
            _additional_col_headers := ', entry_exit_review_pit_type VARCHAR, entry_exit_review_type VARCHAR, entry_exit_review_date VARCHAR ';
            _lower_tier_join := 'JOIN (SELECT tee.tier_link, tee.provider_id, client_id, '||_primary_key||', '||
                                      'plv(teer.point_in_time_type_id) as entry_exit_review_pit_type, '||
                                      'plv(teer.review_type_id) AS entry_exit_review_type, teer.review_date AS entry_exit_review_date '||
                                'FROM sp_entry_exit_review teer JOIN tmp_relevant_ees tee USING (entry_exit_id) '||
                                'WHERE teer.active) ee ON (ee.client_id = qaa.client_id AND qaa.date_effective::DATE <= ee.entry_exit_review_date::DATE) ';
        ELSIF _type = 'exit' THEN
            _lower_tier_join := 'JOIN tmp_relevant_ees ee ON (ee.client_id = qaa.client_id AND (ee.exit_date IS NULL OR qaa.date_effective::DATE <= ee.exit_date::DATE)) ';
        ELSE
            _lower_tier_join := 'JOIN tmp_relevant_ees ee ON (ee.client_id = qaa.client_id AND qaa.date_effective::DATE <= ee.entry_date::DATE) ';
        END IF;

        _question_query := 'SELECT DISTINCT virt_field_name FROM qlik_'||_type||'_answers 
        UNION SELECT DISTINCT virt_field_name FROM qlik_answer_access qaa ORDER BY 1';

        _inner_query := 'SELECT DISTINCT ON (tier_link, '||_primary_key||', virt_field_name) tier_link||''''|''''||'||_primary_key||' AS sec_key, virt_field_name, answer_val'||_additional_col||'
                 FROM (
                 SELECT DISTINCT '||_primary_key||', tier_link, virt_field_name, answer_val, date_effective'||_additional_col||'
                 FROM qlik_'||_type||'_answers qea '||_upper_tier_join||'
                 UNION
                 SELECT DISTINCT ON ('||_primary_key||', tier_link, virt_field_name, answer_val, date_effective) '
                 ||_primary_key||', tier_link, virt_field_name, answer_val, date_effective'||_additional_col||'
                 FROM qlik_answer_access qaa '||_lower_tier_join||'
                 WHERE ee.provider_id = qaa.provider_id 
                   OR (qaa.visibility_id IS NOT NULL 
                       AND EXISTS (SELECT 1 FROM qlik_answer_vis_provider qap WHERE qap.visibility_id = qaa.visibility_id AND qap.provider_id = qaa.provider_id))
                 ) t
                 ORDER BY '||_primary_key||', tier_link, virt_field_name, date_effective DESC'||_additional_col;

        _dsql := 'SELECT FORMAT(
        $$
          SELECT * FROM crosstab('''||_inner_query||''', '''||_question_query||''')
        AS
        (
            sec_key VARCHAR'||_additional_col_headers||', %s
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

-- select qlik_build_review_answers_table('2015-01-01', '2015-01-01');
-- select qlik_build_answer_pivot_views('2015-01-01', '2015-01-01', ARRAY['review']);

-- select * from qlik_review_answer_pivot_view