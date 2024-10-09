SELECT a.*, 
b.us_resident_flag, 
b.age, 
c.afternoon_activity_perc,
c.evening_activity_perc,
c.email_activity_perc,
c.morning_activity_perc,
c.social_activity_perc, 
c.google_activity_perc,
c.search_activity_perc,
c.banner_activity_perc,
c.cpc_activity_perc,
c.affiliate_activity_perc,
c.referral_activity_perc
FROM ml_custom_attr a 
LEFT JOIN gldn.customer b on a.canonical_id=b.canonical_id
LEFT JOIN td_ml_dev.nba_combined_metrics_final c on a.canonical_id=c.canonical_id