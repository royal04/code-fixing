create or replace package INTG_PURGE_DATA_RET IS
   -----------------------------------------------------------------------------
   -- Author  : Praveen Kumar
   -- Created : 12/11/2020
   -- Purpose : This package has the purpose of supporting functions
   --           related with PSM INTG purge tables.
---------------------------------------------------------------------------------
  
   
-----------------------------------------------------------------------------------------------------------
  -- Name:     DEL_INT_RETICKET_CTRL_AUDIT -1
  -- Purpose:   This function will perform all the delete logic
  --            associated with deleting a Audit records FROM INT_RETICKET_CTRL_AUDIT based on retention.
------------------------------------------------------------------------------------------------------------
 FUNCTION DEL_INT_RETICKET_CTRL_AUDIT(O_error_message IN OUT ERRORS.MESSAGE%TYPE)
   RETURN BOOLEAN;
   
-----------------------------------------------------------------------------------------------------------
  -- Name:      DEL_INT_STAGED_MESSAGE_AUDIT -2
  -- Purpose:   This function will perform all the delete logic
  --            associated with deleting a Audit records FROM INT_STAGED_MESSAGE_AUDIT based on retention.
------------------------------------------------------------------------------------------------------------
FUNCTION DEL_INT_STAGED_MESSAGE_AUDIT(O_error_message IN OUT ERRORS.MESSAGE%TYPE)
     RETURN BOOLEAN;
	 
------------------------------------------------------------------------------------------------------------
  -- Name:      DEL_INT_STKADJ_CTRL_AUDIT -3
  -- Purpose:   This function will perform all the delete logic
  --            associated with deleting a Audit records FROM INT_STKADJ_CTRL_AUDIT based on retention.
------------------------------------------------------------------------------------------------------------
FUNCTION DEL_INT_STKADJ_CTRL_AUDIT(O_error_message IN OUT ERRORS.MESSAGE%TYPE)
     RETURN BOOLEAN;
	 
------------------------------------------------------------------------------------------------------------
  -- Name:      DEL_INT_STKCNT_CTRL_AUDIT -4
  -- Purpose:   This function will perform all the delete logic
  --            associated with deleting a Audit records FROM INT_STKCNT_CTRL_AUDIT based on retention.
------------------------------------------------------------------------------------------------------------
FUNCTION DEL_INT_STKCNT_CTRL_AUDIT(O_error_message IN OUT ERRORS.MESSAGE%TYPE)
     RETURN BOOLEAN;

------------------------------------------------------------------------------------------------------------
  -- Name:      DEL_INT_TSFIN_AUDIT -5
  -- Purpose:   This function will perform all the delete logic
  --            associated with deleting a Audit records FROM INT_TSFIN_AUDIT based on retention.
------------------------------------------------------------------------------------------------------------
FUNCTION DEL_INT_TSFIN_AUDIT(O_error_message IN OUT ERRORS.MESSAGE%TYPE)
     RETURN BOOLEAN;

------------------------------------------------------------------------------------------------------------
  -- Name:      DEL_INT_TSFIN_CTRL_AUDIT -6
  -- Purpose:   This function will perform all the delete logic
  --            associated with deleting a Audit records FROM INT_TSFIN_CTRL_AUDIT based on retention.
------------------------------------------------------------------------------------------------------------
FUNCTION DEL_INT_TSFIN_CTRL_AUDIT(O_error_message IN OUT ERRORS.MESSAGE%TYPE)
     RETURN BOOLEAN;	

------------------------------------------------------------------------------------------------------------
  -- Name:      DEL_INT_SALES_WEEK_SUM -7
  -- Purpose:   This function will perform all the delete logic
  --            associated with deleting records FROM INT_SALES_WEEK_SUM based on retention.
------------------------------------------------------------------------------------------------------------
FUNCTION DEL_INT_SALES_WEEK_SUM(O_error_message IN OUT ERRORS.MESSAGE%TYPE)
     RETURN BOOLEAN;	
	 
------------------------------------------------------------------------------------------------------------
  -- Name:      DEL_INT_STORE_BATCH_CTRL -8
  -- Purpose:   This function will perform all the delete logic
  --            associated with deleting  records FROM INT_STORE_BATCH_CTRL based on retention.
------------------------------------------------------------------------------------------------------------
FUNCTION DEL_INT_STORE_BATCH_CTRL(O_error_message IN OUT ERRORS.MESSAGE%TYPE)
     RETURN BOOLEAN;

------------------------------------------------------------------------------------------------------------
  -- Name:      DEL_INTG_DSD_UPLOAD_CTRL -9
  -- Purpose:   This function will perform all the delete logic
  --            associated with deleting  records FROM INTG_DSD_UPLOAD_CTRL based on retention.
------------------------------------------------------------------------------------------------------------
FUNCTION DEL_INTG_DSD_UPLOAD_CTRL(O_error_message IN OUT ERRORS.MESSAGE%TYPE)
     RETURN BOOLEAN;	 
     
------------------------------------------------------------------------------------------------------------
  -- Name:      DEL_INTG_PRICE_EVENT_CTRL -10
  -- Purpose:   This function will perform all the delete logic
  --            associated with deleting records FROM INTG_PRICE_EVENT_CTRL based on retention.
------------------------------------------------------------------------------------------------------------
FUNCTION DEL_INTG_PRICE_EVENT_CTRL(O_error_message IN OUT ERRORS.MESSAGE%TYPE)
     RETURN BOOLEAN; 
	 
END INTG_PURGE_DATA_RET;




create or replace package BODY INTG_PURGE_DATA_RET IS
-----------------------------------------------------------------------------------------------------------
  -- Name:     DEL_INT_RETICKET_CTRL_AUDIT -1
  -- Purpose:   This function will perform all the delete logic
  --            associated with deleting a Audit records FROM INT_RETICKET_CTRL_AUDIT based on retention.
------------------------------------------------------------------------------------------------------------
FUNCTION DEL_INT_RETICKET_CTRL_AUDIT(O_error_message IN OUT ERRORS.MESSAGE%TYPE)
     RETURN BOOLEAN IS

      L_program             VARCHAR2(64) := 'DEL_INT_RETICKET_CTRL_AUDIT';
      L_purge_days          system_config.config_value%TYPE;
      L_language_code       LANGUAGE.ISO_CODE%TYPE;
      L_key                 system_config.config_key%TYPE := 'DAYS_TO_HOLD_AUDIT_CTRL_TABLES';
     

    BEGIN

       L_purge_days := PSM_SQL.Get_config_value(L_key);

    delete  from INT_RETICKET_CTRL_AUDIT  --1
       where  NVL(last_update_date, create_date) < (SYSDATE - L_purge_days);
   
     RETURN TRUE;

    EXCEPTION
      WHEN OTHERS THEN
        O_error_message := PSM_SQL.GET_ERROR_MESSAGE(I_error_id      => 'IN_PARAM_MAND',
                                                     I_lang_iso_code => L_language_code,
                                                     I_par_1         => L_key,
                                                     I_par_2         => L_program,
                                                     I_par_3         => null);
        RETURN FALSE;

    END DEL_INT_RETICKET_CTRL_AUDIT;
	
-----------------------------------------------------------------------------------------------------------
  -- Name:      DEL_INT_STAGED_MESSAGE_AUDIT -2
  -- Purpose:   This function will perform all the delete logic
  --            associated with deleting a Audit records FROM INT_STAGED_MESSAGE_AUDIT based on retention.
------------------------------------------------------------------------------------------------------------
FUNCTION DEL_INT_STAGED_MESSAGE_AUDIT(O_error_message IN OUT ERRORS.MESSAGE%TYPE)
     RETURN BOOLEAN IS

      L_program             VARCHAR2(64) := 'DEL_INT_STAGED_MESSAGE_AUDIT';
      L_purge_days          system_config.config_value%TYPE;
      L_language_code       LANGUAGE.ISO_CODE%TYPE;
      L_key                 system_config.config_key%TYPE := 'DAYS_TO_HOLD_AUDIT_CTRL_TABLES';
     

    BEGIN

       L_purge_days := PSM_SQL.Get_config_value(L_key);

      delete from INT_STAGED_MESSAGE_AUDIT  --2
       where NVL(last_update_date, create_date) < (SYSDATE - L_purge_days);
	      
      RETURN TRUE;

    EXCEPTION
      WHEN OTHERS THEN
        O_error_message := PSM_SQL.GET_ERROR_MESSAGE(I_error_id      => 'IN_PARAM_MAND',
                                                     I_lang_iso_code => L_language_code,
                                                     I_par_1         => L_key,
                                                     I_par_2         => L_program,
                                                     I_par_3         => null);
        RETURN FALSE;

    END DEL_INT_STAGED_MESSAGE_AUDIT;
	
------------------------------------------------------------------------------------------------------------
  -- Name:      DEL_INT_STKADJ_CTRL_AUDIT -3
  -- Purpose:   This function will perform all the delete logic
  --            associated with deleting a Audit records FROM INT_STKADJ_CTRL_AUDIT based on retention.
------------------------------------------------------------------------------------------------------------
FUNCTION DEL_INT_STKADJ_CTRL_AUDIT(O_error_message IN OUT ERRORS.MESSAGE%TYPE)
     RETURN BOOLEAN IS

      L_program             VARCHAR2(64) := 'DEL_INT_STKADJ_CTRL_AUDIT';
      L_purge_days          system_config.config_value%TYPE;
      L_language_code       LANGUAGE.ISO_CODE%TYPE;
      L_key                 system_config.config_key%TYPE := 'DAYS_TO_HOLD_AUDIT_CTRL_TABLES';
     

    BEGIN

       L_purge_days := PSM_SQL.Get_config_value(L_key);

     
      delete from INT_STKADJ_CTRL_AUDIT --3
       where NVL(last_update_date, create_date) < (SYSDATE - L_purge_days);
	  
      RETURN TRUE;

    EXCEPTION
      WHEN OTHERS THEN
        O_error_message := PSM_SQL.GET_ERROR_MESSAGE(I_error_id      => 'IN_PARAM_MAND',
                                                     I_lang_iso_code => L_language_code,
                                                     I_par_1         => L_key,
                                                     I_par_2         => L_program,
                                                     I_par_3         => null);
        RETURN FALSE;

    END DEL_INT_STKADJ_CTRL_AUDIT;
	
------------------------------------------------------------------------------------------------------------
  -- Name:      DEL_INT_STKCNT_CTRL_AUDIT -4
  -- Purpose:   This function will perform all the delete logic
  --            associated with deleting a Audit records FROM INT_STKCNT_CTRL_AUDIT based on retention.
------------------------------------------------------------------------------------------------------------
FUNCTION DEL_INT_STKCNT_CTRL_AUDIT(O_error_message IN OUT ERRORS.MESSAGE%TYPE)
     RETURN BOOLEAN IS

      L_program             VARCHAR2(64) := 'DEL_INT_STKCNT_CTRL_AUDIT';
      L_purge_days          system_config.config_value%TYPE;
      L_language_code       LANGUAGE.ISO_CODE%TYPE;
      L_key                 system_config.config_key%TYPE := 'DAYS_TO_HOLD_AUDIT_CTRL_TABLES';
     

    BEGIN

       L_purge_days := PSM_SQL.Get_config_value(L_key);

     delete from INT_STKCNT_CTRL_AUDIT --4
       where insert_date < (SYSDATE - L_purge_days);
    
      RETURN TRUE;

    EXCEPTION
      WHEN OTHERS THEN
        O_error_message := PSM_SQL.GET_ERROR_MESSAGE(I_error_id      => 'IN_PARAM_MAND',
                                                     I_lang_iso_code => L_language_code,
                                                     I_par_1         => L_key,
                                                     I_par_2         => L_program,
                                                     I_par_3         => null);
        RETURN FALSE;

    END DEL_INT_STKCNT_CTRL_AUDIT;
	
------------------------------------------------------------------------------------------------------------
  -- Name:      DEL_INT_TSFIN_AUDIT -5
  -- Purpose:   This function will perform all the delete logic
  --            associated with deleting a Audit records FROM INT_TSFIN_AUDIT based on retention.
------------------------------------------------------------------------------------------------------------
FUNCTION DEL_INT_TSFIN_AUDIT(O_error_message IN OUT ERRORS.MESSAGE%TYPE)
     RETURN BOOLEAN IS

      L_program             VARCHAR2(64) := 'DEL_INT_TSFIN_AUDIT';
      L_purge_days          system_config.config_value%TYPE;
      L_language_code       LANGUAGE.ISO_CODE%TYPE;
      L_key                 system_config.config_key%TYPE := 'DAYS_TO_HOLD_AUDIT_CTRL_TABLES';
     

    BEGIN

       L_purge_days := PSM_SQL.Get_config_value(L_key);
 

      delete from INT_TSFIN_AUDIT --5
       where NVL(last_update_date, create_date) < (SYSDATE - L_purge_days);
    
      RETURN TRUE;

    EXCEPTION
      WHEN OTHERS THEN
        O_error_message := PSM_SQL.GET_ERROR_MESSAGE(I_error_id      => 'IN_PARAM_MAND',
                                                     I_lang_iso_code => L_language_code,
                                                     I_par_1         => L_key,
                                                     I_par_2         => L_program,
                                                     I_par_3         => null);
        RETURN FALSE;

    END DEL_INT_TSFIN_AUDIT;

------------------------------------------------------------------------------------------------------------
  -- Name:      DEL_INT_TSFIN_CTRL_AUDIT -6
  -- Purpose:   This function will perform all the delete logic
  --            associated with deleting a Audit records FROM INT_TSFIN_CTRL_AUDIT based on retention.
------------------------------------------------------------------------------------------------------------
FUNCTION DEL_INT_TSFIN_CTRL_AUDIT(O_error_message IN OUT ERRORS.MESSAGE%TYPE)
     RETURN BOOLEAN IS

      L_program             VARCHAR2(64) := 'DEL_INT_TSFIN_CTRL_AUDIT';
      L_purge_days          system_config.config_value%TYPE;
      L_language_code       LANGUAGE.ISO_CODE%TYPE;
      L_key                 system_config.config_key%TYPE := 'DAYS_TO_HOLD_AUDIT_CTRL_TABLES';
     

    BEGIN

       L_purge_days := PSM_SQL.Get_config_value(L_key);
	   
    delete from INT_TSFIN_CTRL_AUDIT --6
       where NVL(last_update_date, create_date) < (SYSDATE - L_purge_days); 
	
    
      RETURN TRUE;

    EXCEPTION
      WHEN OTHERS THEN
        O_error_message := PSM_SQL.GET_ERROR_MESSAGE(I_error_id      => 'IN_PARAM_MAND',
                                                     I_lang_iso_code => L_language_code,
                                                     I_par_1         => L_key,
                                                     I_par_2         => L_program,
                                                     I_par_3         => null);
        RETURN FALSE;

    END DEL_INT_TSFIN_CTRL_AUDIT;	


------------------------------------------------------------------------------------------------------------
  -- Name:      DEL_INT_SALES_WEEK_SUM -7
  -- Purpose:   This function will perform all the delete logic
  --            associated with deleting records FROM INT_SALES_WEEK_SUM based on retention.
------------------------------------------------------------------------------------------------------------
FUNCTION DEL_INT_SALES_WEEK_SUM(O_error_message IN OUT ERRORS.MESSAGE%TYPE)
     RETURN BOOLEAN IS

      L_program             VARCHAR2(64) := 'DEL_INT_SALES_WEEK_SUM';
      L_purge_days          system_config.config_value%TYPE;
      L_language_code       LANGUAGE.ISO_CODE%TYPE;
      L_key                 system_config.config_key%TYPE := 'DAYS_TO_HOLD_AUDIT_CTRL_TABLES';
     

    BEGIN

       L_purge_days := PSM_SQL.Get_config_value(L_key);
	   
    delete from INT_SALES_WEEK_SUM --7
       where NVL(last_update_date, create_date) < (SYSDATE - L_purge_days); 
	
    
      RETURN TRUE;

    EXCEPTION
      WHEN OTHERS THEN
        O_error_message := PSM_SQL.GET_ERROR_MESSAGE(I_error_id      => 'IN_PARAM_MAND',
                                                     I_lang_iso_code => L_language_code,
                                                     I_par_1         => L_key,
                                                     I_par_2         => L_program,
                                                     I_par_3         => null);
        RETURN FALSE;

    END DEL_INT_SALES_WEEK_SUM;
	
------------------------------------------------------------------------------------------------------------
  -- Name:      DEL_INT_STORE_BATCH_CTRL -8
  -- Purpose:   This function will perform all the delete logic
  --            associated with deleting records FROM INT_STORE_BATCH_CTRL based on retention.
------------------------------------------------------------------------------------------------------------
FUNCTION DEL_INT_STORE_BATCH_CTRL(O_error_message IN OUT ERRORS.MESSAGE%TYPE)
     RETURN BOOLEAN IS

      L_program             VARCHAR2(64) := 'DEL_INT_STORE_BATCH_CTRL';
      L_purge_days          system_config.config_value%TYPE;
      L_language_code       LANGUAGE.ISO_CODE%TYPE;
      L_key                 system_config.config_key%TYPE := 'DAYS_TO_HOLD_AUDIT_CTRL_TABLES';
     

    BEGIN

       L_purge_days := PSM_SQL.Get_config_value(L_key);
	   
    delete from INT_STORE_BATCH_CTRL --8
       where vdate < (SYSDATE - L_purge_days); 
	
    
      RETURN TRUE;

    EXCEPTION
      WHEN OTHERS THEN
        O_error_message := PSM_SQL.GET_ERROR_MESSAGE(I_error_id      => 'IN_PARAM_MAND',
                                                     I_lang_iso_code => L_language_code,
                                                     I_par_1         => L_key,
                                                     I_par_2         => L_program,
                                                     I_par_3         => null);
        RETURN FALSE;

    END DEL_INT_STORE_BATCH_CTRL;

------------------------------------------------------------------------------------------------------------
  -- Name:      DEL_INTG_DSD_UPLOAD_CTRL -9
  -- Purpose:   This function will perform all the delete logic
  --            associated with deleting records FROM INT_STORE_BATCH_CTRL based on retention.
------------------------------------------------------------------------------------------------------------
FUNCTION DEL_INTG_DSD_UPLOAD_CTRL(O_error_message IN OUT ERRORS.MESSAGE%TYPE)
     RETURN BOOLEAN IS

      L_program             VARCHAR2(64) := 'DEL_INTG_DSD_UPLOAD_CTRL';
      L_purge_days          system_config.config_value%TYPE;
      L_language_code       LANGUAGE.ISO_CODE%TYPE;
      L_key                 system_config.config_key%TYPE := 'DAYS_TO_HOLD_AUDIT_CTRL_TABLES';
     

    BEGIN

       L_purge_days := PSM_SQL.Get_config_value(L_key);
	   
    delete from INTG_DSD_UPLOAD_CTRL --9
       where DATE_OF_UPLOAD < (SYSDATE - L_purge_days); 
	
    
      RETURN TRUE;

    EXCEPTION
      WHEN OTHERS THEN
        O_error_message := PSM_SQL.GET_ERROR_MESSAGE(I_error_id      => 'IN_PARAM_MAND',
                                                     I_lang_iso_code => L_language_code,
                                                     I_par_1         => L_key,
                                                     I_par_2         => L_program,
                                                     I_par_3         => null);
        RETURN FALSE;

    END DEL_INTG_DSD_UPLOAD_CTRL;	
    
------------------------------------------------------------------------------------------------------------
  -- Name:      DEL_INTG_PRICE_EVENT_CTRL -10
  -- Purpose:   This function will perform all the delete logic
  --            associated with deleting records FROM INTG_PRICE_EVENT_CTRL based on retention.
------------------------------------------------------------------------------------------------------------
FUNCTION DEL_INTG_PRICE_EVENT_CTRL(O_error_message IN OUT ERRORS.MESSAGE%TYPE)
     RETURN BOOLEAN IS

      L_program             VARCHAR2(64) := 'DEL_INTG_PRICE_EVENT_CTRL';
      L_purge_days          system_config.config_value%TYPE;
      L_language_code       LANGUAGE.ISO_CODE%TYPE;
      L_key                 system_config.config_key%TYPE := 'DAYS_TO_HOLD_AUDIT_CTRL_TABLES';
     

    BEGIN

       L_purge_days := PSM_SQL.Get_config_value(L_key);
	   
    delete from INTG_PRICE_EVENT_CTRL --10
       where NVL(last_update_date, create_date) < (SYSDATE - L_purge_days);  
	
    
      RETURN TRUE;

    EXCEPTION
      WHEN OTHERS THEN
        O_error_message := PSM_SQL.GET_ERROR_MESSAGE(I_error_id      => 'IN_PARAM_MAND',
                                                     I_lang_iso_code => L_language_code,
                                                     I_par_1         => L_key,
                                                     I_par_2         => L_program,
                                                     I_par_3         => null);
        RETURN FALSE;

    END DEL_INTG_PRICE_EVENT_CTRL;	
    
END INTG_PURGE_DATA_RET;
