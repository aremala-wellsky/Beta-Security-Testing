-- Function: qlik_build_exit_answers_table(character varying, character varying)

-- DROP FUNCTION qlik_build_exit_answers_table(character varying, character varying);

CREATE OR REPLACE FUNCTION qlik_build_entry_answers_table(
    _delta_date character varying,
    _entry_exit_date character varying)
  RETURNS void AS
$BODY$
DECLARE
    _dsql TEXT;
BEGIN
    -- Version 20200609-1

DROP TABLE IF EXISTS qlik_entry_answers;

_dsql := '
CREATE TABLE qlik_entry_answers AS 
SELECT
b.entry_exit_id, b.question_id, b.virt_field_name, 0 AS entryexit, da3.date_effective,
CASE WHEN b.code = ''lookup'' THEN plv(da3.val_int)::VARCHAR
     WHEN b.code = ''yes_no'' THEN yn(da3.val_int)::VARCHAR
     WHEN b.code = ''date'' THEN TO_CHAR((da3.val_date)::TIMESTAMP::DATE,''MM/dd/YYYY'')
     WHEN b.code = ''int'' THEN da3.val_int::VARCHAR
     WHEN b.code = ''textbox'' THEN substring(da3.val_textfield::VARCHAR from 1 for 200)
     WHEN b.code = ''textarea'' THEN substring(da3.val_textfield::VARCHAR from 1 for 200)
     WHEN b.code = ''money'' THEN da3.val_float::VARCHAR
     WHEN b.code = ''service_code'' THEN da3.val_int::VARCHAR
     ELSE '''' 
END AS answer_val
FROM (
    SELECT a.entry_exit_id, a.question_id, a.code, a.virt_field_name,
    (select da2.answer_id 
     FROM da_answer da2 JOIN da_question dq2 USING (question_id)
     WHERE da2.client_id = a.client_id AND da2.question_id = a.question_id AND da2.date_effective = a.date_effective 
     ORDER BY da2.answer_id DESC LIMIT 1) as answer_id
    FROM (
        SELECT ee.entry_exit_id, ee.client_id, da.question_id, dq.virt_field_name, dq.question_type_code AS code, max(date_effective) as date_effective
        FROM (
            -- Get Updated EEs
            SELECT DISTINCT ee2.entry_exit_id, ee2.client_id, ee2.provider_id, ee2.entry_date
            FROM sp_entry_exit ee2
            WHERE ee2.active AND ee2.date_updated > '''||_delta_date||'''
              AND (ee2.exit_date IS NULL OR ee2.exit_date >= '''||_entry_exit_date||''')
            UNION
            -- Get Updated Answers
            SELECT DISTINCT ee3.entry_exit_id, ee3.client_id, ee3.provider_id, ee3.entry_date
            FROM sp_entry_exit ee3
            JOIN da_answer da3 USING (client_id)
            JOIN qlik_answer_questions dq3 USING (question_id)
            WHERE ee3.active AND LEAST(da3.date_inactive, da3.date_added) > '''||_delta_date||'''
              AND da3.date_effective::DATE <= ee3.entry_date::DATE
              AND (ee3.exit_date IS NULL OR ee3.exit_date >= '''||_entry_exit_date||''')
        ) ee
        JOIN (select DISTINCT entry_exit_id FROM qlik_ee_user_access_tier_view) eev USING(entry_exit_id)
        JOIN da_answer da USING (client_id)
        JOIN qlik_answer_questions dq ON (dq.question_id = da.question_id)
        LEFT JOIN sp_provider_tree belowtree ON (belowtree.ancestor_provider_id = ee.provider_id)
        LEFT JOIN sp_provider_tree abovetree ON (abovetree.provider_id = ee.provider_id)
        WHERE da.active AND da.date_effective::DATE <= ee.entry_date::DATE
        AND ((belowtree.provider_id IS NOT NULL AND belowtree.provider_id = da.provider_id) 
          OR (abovetree.ancestor_provider_id IS NOT NULL AND abovetree.ancestor_provider_id = da.provider_id)
          OR dq.virt_field_name IN (''SVPPROFGENDER'',''RHYMISTERTIARYRACE'',''RHYMISQUATERNARYRACE'',''RHYMISQUINARYRACE'',''SVPPROFSECONDARYRACE'',''SVPPROFRACE'',''SVPPROFETH'',''SVPPROFDOB'',''SVPPROFDOBTYPE'')
        )
        GROUP BY ee.entry_exit_id, ee.client_id, da.question_id, dq.virt_field_name, dq.question_type_code
    ) a
) b
JOIN da_answer da3 USING (answer_id)';

    RAISE NOTICE 'Starting the creation of qlik_entry_answers %',clock_timestamp();
    EXECUTE _dsql;
    ALTER TABLE qlik_entry_answers OWNER TO sp5user;
    RAISE NOTICE 'Finished creating qlik_entry_answers %',clock_timestamp();
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
