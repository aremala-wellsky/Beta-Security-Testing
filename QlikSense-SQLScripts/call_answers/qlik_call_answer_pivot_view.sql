CREATE OR REPLACE FUNCTION qlik_build_call_answer_pivot_view(
    _delta_date character varying,
    _call_record_date character varying,
    _create_base_table boolean)
  RETURNS void AS
$BODY$
DECLARE
    _dsql TEXT;
    _question_query TEXT;
    _inner_query TEXT;
    _final_query TEXT;
    _sa_query TEXT;
BEGIN

    IF _create_base_table IS NOT DISTINCT FROM TRUE THEN 
        PERFORM qlik_build_call_answers_table($1, $2);
    END IF;

    _question_query := 'SELECT DISTINCT virt_field_name FROM qlik_entry_answers 
	UNION SELECT DISTINCT virt_field_name FROM qlik_answer_access_entry qaa ORDER BY 1';

    _inner_query := 'SELECT DISTINCT ON (tier_link, call_record_id, virt_field_name) tier_link||''''|''''||call_record_id AS sec_key, virt_field_name, answer_val
                     FROM (
                         SELECT ee.call_record_id, tier_link, virt_field_name, answer_val, date_effective 
                         FROM qlik_entry_answers qea
                         -- JOIN ON CALL RECORDS???? 
                         JOIN qlik_user_access_tier_view uat ON (ee.provider_id = uat.provider_id AND uat.user_access_tier = 1)
                         UNION
                         SELECT call_record_id, tier_link, virt_field_name, answer_val, date_effective
                         FROM qlik_call_answer_access qaa 
                         JOIN qlik_user_access_tier_view uat ON (uat.user_access_tier != 1)
                         -- EXPLICIT VIS STUFF??????
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
    string_agg(FORMAT(''%I %s'', upper(virt_field_name)||''_entry'', ''TEXT''), '', '' ORDER BY virt_field_name)
)
FROM (
    '||_question_query||'
)
    AS t';

    RAISE NOTICE 'Creating the pivot query %',clock_timestamp();
    EXECUTE _dsql INTO _final_query;
    RAISE NOTICE 'Finished creating pivot query %',clock_timestamp();

    DROP MATERIALIZED VIEW IF EXISTS qlik_call_answer_pivot_view;
    EXECUTE 'CREATE MATERIALIZED VIEW qlik_call_answer_pivot_view AS '||_final_query;
    RAISE NOTICE 'Finished creating qlik_call_answer_pivot_view %',clock_timestamp();
    
    ALTER TABLE qlik_call_answer_pivot_view OWNER TO sp5user;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;