SELECT column_name  
FROM SELFANALYSIS_DB.INFORMATION_SCHEMA.COLUMNS  
WHERE table_catalog = 'SELFANALYSIS_DB'  
  AND table_schema  = 'GP_GPM_LOW'  
  AND table_name    = 'V_GPM_AGREED_GOODS'  
ORDER BY ordinal_position;  