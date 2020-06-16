CREATE OR REPLACE FUNCTION qlik_build_call_answer_tables(
    _delta_date character varying,
    _call_record_date character varying,
    _types VARCHAR[])
  RETURNS void AS
$BODY$
DECLARE
    _dsql TEXT;
    _date_column TEXT;
    _type VARCHAR;
BEGIN
    -- Version 20200609-1

    _types := CASE WHEN ($3 IS NULL) THEN ARRAY['call', 'call_followup'] ELSE $3 END;

    FOREACH _type IN ARRAY _types LOOP
        _date_column := CASE WHEN _type = 'call_followup' THEN 'actual_followup_date' ELSE 'start_date' END;

        EXECUTE 'DROP TABLE IF EXISTS qlik_'||_type||'_answers';

        _dsql := 'CREATE TABLE qlik_'||_type||'_answers AS 
        SELECT b.call_record_id, b.question_id, dq3.virt_field_name, b.date_effective,
        CASE WHEN b.code = ''lookup'' THEN plv(ca3.val_int)::VARCHAR
         WHEN b.code = ''yes_no'' THEN yn(ca3.val_int)::VARCHAR
         WHEN b.code = ''date'' THEN (ca3.val_date::DATE)::VARCHAR
         WHEN b.code = ''int'' THEN ca3.val_int::VARCHAR
         WHEN b.code = ''textbox'' THEN ca3.val_textfield::VARCHAR
         WHEN b.code = ''textarea'' THEN ca3.val_textfield::VARCHAR
         WHEN b.code = ''money'' THEN ca3.val_float::VARCHAR
         WHEN b.code = ''service_code'' THEN ca3.val_int::VARCHAR
         ELSE '''' 
        END AS answer_val
        FROM
        (
        SELECT a.call_record_id, a.question_id, a.code, a.date_effective,
        (select ca2.call_answer_id FROM call_answer ca2 INNER JOIN da_question dq2 on (dq2.question_id = ca2.question_id)
            WHERE ca2.call_record_id = a.call_record_id AND ca2.question_id = a.question_id AND ca2.date_effective = a.date_effective 
            ORDER BY ca2.call_answer_id DESC LIMIT 1) as answer_id
        FROM 
        (
            SELECT cr.call_record_id, ca.question_id, dq.question_type_code AS code, max(date_effective) as date_effective
            FROM (
            SELECT DISTINCT cr2.call_record_id, cr2.'||_date_column||'
            FROM sp_call_record cr2
            WHERE cr2.active AND cr2.date_updated > '''||_delta_date||'''
            AND '||_date_column||' IS NOT NULL
            AND (cr2.end_date IS NULL OR cr2.end_date >= '''||_call_record_date||''')
            UNION
            SELECT DISTINCT  cr3.call_record_id, cr3.'||_date_column||'
            FROM sp_call_record cr3
            INNER JOIN call_answer ca3 ON (ca3.call_record_id = cr3.call_record_id)
            INNER JOIN qlik_answer_questions dq3 ON (dq3.question_id = ca3.question_id)
            WHERE cr3.active AND ca3.active 
            AND LEAST(ca3.date_inactive, ca3.date_added) > '''||_delta_date||'''
            AND cr3.date_updated > '''||_delta_date||''' 
            AND '||_date_column||' IS NOT NULL AND ca3.date_effective::DATE <= cr3.'||_date_column||'::DATE
            AND (cr3.end_date IS NULL OR (cr3.end_date >= '''||_call_record_date||'''))
            )cr
            INNER JOIN call_answer ca ON (ca.call_record_id = cr.call_record_id)
            INNER JOIN qlik_answer_questions dq ON (dq.question_id = ca.question_id)
            WHERE ca.active AND '||_date_column||' IS NOT NULL AND ca.date_effective::DATE <= cr.'||_date_column||'::DATE
            GROUP BY cr.call_record_id, ca.question_id, dq.question_type_code
        ) a
        ) b
        INNER JOIN call_answer ca3 ON (ca3.call_answer_id = b.answer_id)
        INNER JOIN da_question dq3 ON (dq3.question_id = ca3.question_id)';

        RAISE NOTICE 'Starting the creation of qlik_call_answers %',clock_timestamp();
        EXECUTE _dsql;
        EXECUTE 'ALTER TABLE qlik_'||_type||'_answers OWNER TO sp5user';
        RAISE NOTICE 'Finished creating qlik_call_answers %',clock_timestamp();
    END LOOP;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;

-- SELECT qlik_build_call_answer_tables('2015-01-01', '2015-01-01', NULL);