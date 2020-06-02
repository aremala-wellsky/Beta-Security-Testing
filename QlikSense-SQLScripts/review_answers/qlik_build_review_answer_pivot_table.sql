	CREATE OR REPLACE FUNCTION qlik_build_review_answer_pivot_table(
		_delta_date character varying,
		_entry_exit_date character varying)
	  RETURNS void AS
	$BODY$
	DECLARE
		_dsql TEXT;
		_question_query TEXT;
		_inner_query TEXT;
		_final_query TEXT;
	BEGIN
		DROP TABLE IF EXISTS tmp_relevant_ees;

		CREATE TEMP TABLE tmp_relevant_ees AS
		SELECT entry_exit_id, tier_link, client_id, entry_date, exit_date, provider_id
		FROM qlik_ee_user_access_tier_view uat
		WHERE uat.user_access_tier != 1 AND (exit_date IS NULL OR exit_date::DATE >= $2::DATE);

			_question_query := 'SELECT DISTINCT virt_field_name FROM qlik_review_answers 
			UNION SELECT DISTINCT virt_field_name FROM qlik_answer_access qaa ORDER BY 1';

			_inner_query := 'SELECT DISTINCT ON (tier_link, entry_exit_review_id, virt_field_name) tier_link||''''|''''||entry_exit_review_id AS sec_key, virt_field_name, answer_val '||
					'FROM (
					 SELECT DISTINCT entry_exit_review_id, tier_link, virt_field_name, answer_val, date_effective 
					 FROM qlik_review_answers qea 
					 JOIN (SELECT DISTINCT tier_link, entry_exit_id FROM qlik_ee_user_access_tier_view t WHERE t.user_access_tier = 1) ee USING (entry_exit_id)
					 UNION
					 SELECT DISTINCT ON (entry_exit_review_id, tier_link, virt_field_name, answer_val, date_effective) 
					 entry_exit_review_id, tier_link, virt_field_name, answer_val, date_effective
					 FROM qlik_answer_access qaa 
					 JOIN (SELECT tee.tier_link, tee.provider_id, client_id, entry_exit_review_id, entry_exit_id, teer.review_date AS entry_exit_review_date 
						   FROM sp_entry_exit_review teer JOIN tmp_relevant_ees tee USING (entry_exit_id)
						   WHERE teer.active) ee ON (ee.client_id = qaa.client_id AND qaa.date_effective::DATE <= ee.entry_exit_review_date::DATE)
					 WHERE ee.provider_id = qaa.provider_id 
					   OR (qaa.visibility_id IS NOT NULL 
						   AND EXISTS (SELECT 1 FROM qlik_answer_vis_provider qap WHERE qap.visibility_id = qaa.visibility_id AND qap.provider_id = qaa.provider_id))
					 ) t
					 ORDER BY entry_exit_review_id, tier_link, virt_field_name, date_effective DESC';

			_dsql := 'SELECT FORMAT(
			$$
			  SELECT * FROM crosstab('''||_inner_query||''', '''||_question_query||''')
			AS
			(
				sec_key VARCHAR, %s
			)
			$$,
			string_agg(FORMAT(''%I %s'', upper(virt_field_name)||''_review'', ''TEXT''), '', '' ORDER BY virt_field_name)
		)
		FROM (
			'||_question_query||'
		)
			AS t';

		RAISE NOTICE 'Creating the crosstab query %',clock_timestamp();
		EXECUTE _dsql INTO _final_query;
		RAISE NOTICE 'Finished creating crosstab query: %', clock_timestamp();

		-- We have to create a separate table to define the column structure for the crosstab
		DROP TABLE IF EXISTS tmp_ee_review_crosstab;
		EXECUTE 'CREATE TEMP TABLE tmp_ee_review_crosstab AS '||_final_query;

		-- Create the reviews pivot view like this since we have additional data needed besides 
		-- the sec_key and cross tab doesn't like us adding multiple columns to it
		DROP TABLE IF EXISTS qlik_review_answer_pivot;
		CREATE TABLE qlik_review_answer_pivot AS
		SELECT split_part(sec_key, '|', 1)||'|'||split_part(sec_key, '|', 2)||'|'||eer.entry_exit_id AS ee_sec_key, 
			   plv(eer.point_in_time_type_id) as entry_exit_review_pit_type, entry_exit_review_id,
			   plv(eer.review_type_id) AS entry_exit_review_type, eer.review_date AS entry_exit_review_date, t.*
		FROM tmp_ee_review_crosstab t JOIN sp_entry_exit_review eer ON (split_part(sec_key, '|', 3)::VARCHAR = eer.entry_exit_review_id::VARCHAR);

		RAISE NOTICE 'Finished creating pivot table: %', clock_timestamp();

		ALTER TABLE qlik_review_answer_pivot OWNER TO sp5user;
	END;
	$BODY$
	  LANGUAGE plpgsql VOLATILE
	  COST 100;

	-- select qlik_build_review_answers_table('2015-01-01', '2015-01-01');
	-- select qlik_build_review_answer_pivot_table('2015-01-01', '2015-01-01');
	-- select * from qlik_review_answer_pivot