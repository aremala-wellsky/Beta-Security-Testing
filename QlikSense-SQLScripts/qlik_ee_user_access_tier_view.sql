-- DROP VIEW qlik_ee_user_access_tier_view;

CREATE OR REPLACE VIEW qlik_ee_user_access_tier_view AS 
SELECT DISTINCT (sec.user_access_tier || '|' || sec.user_provider_id) AS tier_link, entry_date, exit_date, user_access_tier, user_provider_id, client_id, entry_exit_id, sec.provider_id AS provider_id
FROM sp_entry_exit ee
JOIN qlik_user_access_tier_view AS sec ON ((ee.provider_id = sec.provider_id) OR (ee.provider_creating_id = sec.provider_id))
WHERE ee.active;
