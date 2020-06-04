CREATE OR REPLACE FUNCTION qlik_build_review_answers_table(
    _delta_date character varying,
    _entry_exit_date character varying)
  RETURNS void AS
$BODY$
DECLARE
    _dsql TEXT;
BEGIN
    -- Version 20200604-1

DROP TABLE IF EXISTS qlik_review_answers;

_dsql := ' CREATE TABLE qlik_review_answers AS 
SELECT
b.entry_exit_review_id,
b.review_date AS entry_exit_review_date,
b.exit_date,
b.entry_exit_id,
b.question_id,
dq3.virt_field_name,
da3.date_effective,
CASE WHEN b.code = ''lookup'' THEN plv(da3.val_int)::VARCHAR
     WHEN b.code = ''yes_no'' THEN yn(da3.val_int)::VARCHAR
     WHEN b.code = ''date'' THEN (da3.val_date::DATE)::VARCHAR
     WHEN b.code = ''int'' THEN da3.val_int::VARCHAR
     WHEN b.code = ''textbox'' THEN left(da3.val_textfield::VARCHAR,200)
     WHEN b.code = ''textarea'' THEN left(da3.val_textfield::VARCHAR,200)
     WHEN b.code = ''money'' THEN da3.val_float::VARCHAR
     WHEN b.code = ''service_code'' THEN da3.val_int::VARCHAR
     ELSE '''' 
END AS answer_val
FROM
(
    SELECT 
            a.entry_exit_review_id,
            a.entry_exit_id,
            a.question_id,
            a.code,
            a.review_type_id,
            a.review_date,
            a.exit_date,
    (SELECT da2.answer_id FROM da_answer da2 INNER JOIN da_question dq2 on (dq2.question_id = da2.question_id)
            WHERE (da2.client_id = a.client_id) AND (da2.question_id = a.question_id) AND (da2.date_effective = a.date_effective) 
            ORDER BY da2.answer_id DESC LIMIT 1) as answer_id
    FROM 
    (
        SELECT 
        ee.entry_exit_review_id, ee.review_date, ee.point_in_time_type_id, ee.review_type_id,
        ee.entry_exit_id, ee.client_id, da.question_id, dq.question_type_code AS code, max(date_effective) as date_effective, ee.exit_date    
        FROM (
            SELECT DISTINCT eer2.entry_exit_review_id, ee2.entry_exit_id, ee2.client_id, ee2.provider_id, eer2.review_date, eer2.point_in_time_type_id, eer2.review_type_id, ee2.exit_date
            FROM sp_entry_exit_review eer2
            INNER JOIN sp_entry_exit ee2 ON (ee2.entry_exit_id = eer2.entry_exit_id)
            INNER JOIN sp_picklist_value pv2 ON (pv2.picklist_value_id = eer2.point_in_time_type_id)
            WHERE (eer2.active = TRUE) AND (eer2.date_updated > '''||_delta_date||''')
            --AND (eer2.review_date >= '''||_entry_exit_date||''')
            AND (ee2.active = true) AND ((ee2.exit_date IS NULL) OR (ee2.exit_date >= '''||_entry_exit_date||'''))
                        AND pv2.code NOT IN (''EEPOINTINTIME_EXIT'',''EEPOINTINTIME_ENTRY'')
            UNION
            SELECT DISTINCT eer3.entry_exit_review_id, ee3.entry_exit_id, ee3.client_id, ee3.provider_id, eer3.review_date, eer3.point_in_time_type_id, eer3.review_type_id, ee3.exit_date
            FROM sp_entry_exit_review eer3
            INNER JOIN sp_entry_exit ee3 ON (ee3.entry_exit_id = eer3.entry_exit_id)
            INNER JOIN da_answer da3 ON (da3.client_id = ee3.client_id)
            INNER JOIN qlik_answer_questions dq3 ON (dq3.question_id = da3.question_id)
            INNER JOIN sp_picklist_value pv3 ON (pv3.picklist_value_id = eer3.point_in_time_type_id)
            WHERE (eer3.active = TRUE) AND (ee3.active = true) AND pv3.code NOT IN (''EEPOINTINTIME_EXIT'',''EEPOINTINTIME_ENTRY'')
            AND (LEAST(da3.date_inactive, da3.date_added) > '''||_delta_date||''')
            AND ((da3.date_effective::DATE <= eer3.review_date::DATE) OR (ee3.exit_date IS NULL))
            AND ((ee3.exit_date IS NULL) OR (ee3.exit_date >= '''||_entry_exit_date||'''))
            --AND (eer3.review_date >= '''||_entry_exit_date||''')
                     ) ee
        INNER JOIN da_answer da ON (da.client_id = ee.client_id)
        INNER JOIN qlik_answer_questions dq ON (dq.question_id = da.question_id)

            LEFT OUTER JOIN sp_provider_tree belowtree ON (belowtree.ancestor_provider_id = ee.provider_id)
            LEFT OUTER JOIN sp_provider_tree abovetree ON (abovetree.provider_id = ee.provider_id)
        
        WHERE (da.active = true)
        --AND (da.date_effective >= '''||_entry_exit_date||''')
        AND ((da.date_effective::DATE <= ee.review_date::DATE) OR (ee.exit_date IS NULL))

        AND (((belowtree.provider_id IS NOT NULL) AND (belowtree.provider_id = da.provider_id)) OR ((abovetree.ancestor_provider_id IS NOT NULL) AND (abovetree.ancestor_provider_id = da.provider_id))
        OR (dq.virt_field_name IN (''SVPPROFGENDER'',''RHYMISTERTIARYRACE'',''RHYMISQUATERNARYRACE'',''RHYMISQUINARYRACE'',''SVPPROFSECONDARYRACE'',''SVPPROFRACE'',''SVPPROFETH'',''SVPPROFDOB'',''SVPPROFDOBTYPE'')))    
        GROUP BY ee.entry_exit_review_id, ee.review_date, ee.point_in_time_type_id, ee.review_type_id, ee.entry_exit_id, ee.client_id, da.question_id, dq.question_type_code, ee.exit_date
    ) a
) b
INNER JOIN da_answer da3 ON (da3.answer_id = b.answer_id)
INNER JOIN da_question dq3 ON (dq3.question_id = da3.question_id)';

    RAISE NOTICE 'Starting the creation of qlik_review_answers %',clock_timestamp();
    EXECUTE _dsql;
    ALTER TABLE qlik_review_answers OWNER TO sp5user;
    RAISE NOTICE 'Finished creating qlik_review_answers %',clock_timestamp();
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;

-- select qlik_build_review_answers_table('2015-01-01', '2015-01-01');