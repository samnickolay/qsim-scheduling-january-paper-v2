-- --JOB TABLES

-- SET CURRENT SCHEMA COBALT_LOG_DB

-- CREATE TABLE JOB_DATA ( \
--     ID INTEGER GENERATED BY DEFAULT AS IDENTITY (START WITH 1, INCREMENT BY 1, CACHE 10 ORDER) NOT NULL PRIMARY KEY, \
--     JOBID INTEGER NOT NULL, \
--     UMASK INTEGER NOT NULL, \
--     JOBNAME VARCHAR(256) NOT NULL, \
--     JOB_TYPE VARCHAR(32) NOT NULL, \
--     JOB_USER VARCHAR(16) NOT NULL, \
--     WALLTIME INTEGER NOT NULL, \
--     PROCS INTEGER NOT NULL, \
--     NODES INTEGER NOT NULL, \
--     ARGS VARCHAR(1024), \
--     COMMAND CLOB NOT NULL, \
--     PROJECT VARCHAR(32) NOT NULL, \
--     LIENID INTEGER NULL, \
--     HOST VARCHAR(128), \
--     PORT INTEGER, \
--     INPUTFILE CLOB, \
--     KERNEL VARCHAR(128) NOT NULL, \
--     KERNELOPTIONS CLOB, \
--     NOTIFY VARCHAR(1024), \
--     ADMINEMAIL VARCHAR(80), \
--     LOCATION VARCHAR(128), \
--     OUTPUTPATH CLOB, \
--     OUTPUTDIR CLOB, \
--     ERRORPATH CLOB, \
--     PATH CLOB NOT NULL, \
--     MODE VARCHAR(4) NOT NULL, \
--     ENVS CLOB, \
--     QUEUE VARCHAR(128) NOT NULL, \
--     PRIORITY_CORE_HOURS INTEGER, \
--     FORCE_KILL_DELAY INTEGER NOT NULL, \
--     ATTRIBUTE VARCHAR(32) NOT NULL, \
--     PREEMPTABLE INTEGER NOT NULL, \
--     ISDUMMY INTEGER NOT NULL DEFAULT 0, \
--     COMMENT VARCHAR(1024) NULL \
-- )


-- CREATE TABLE JOB_EVENTS ( \
--     ID INTEGER GENERATED BY DEFAULT AS IDENTITY (START WITH 1, INCREMENT BY 1, CACHE 10 ORDER) NOT NULL PRIMARY KEY, \
--     NAME VARCHAR(32) NOT NULL, \
--     CLASS CHAR NOT NULL \
-- )

-- --EVENT TABLE ENTRIES:
-- INSERT INTO JOB_EVENTS (NAME, CLASS) VALUES( 'creating', 'C')
-- INSERT INTO JOB_EVENTS (NAME, CLASS) VALUES( 'starting', 'S')
-- INSERT INTO JOB_EVENTS (NAME, CLASS) VALUES( 'running', 'R')
-- INSERT INTO JOB_EVENTS (NAME, CLASS) VALUES( 'resource_epilogue_start', 'S')
-- INSERT INTO JOB_EVENTS (NAME, CLASS) VALUES( 'resource_epilogue_finished', 'S')
-- INSERT INTO JOB_EVENTS (NAME, CLASS) VALUES( 'job_epilogue_start', 'S')
-- INSERT INTO JOB_EVENTS (NAME, CLASS) VALUES( 'job_epilogue_finished', 'T')
-- INSERT INTO JOB_EVENTS (NAME, CLASS) VALUES( 'killing', 'T')
-- INSERT INTO JOB_EVENTS (NAME, CLASS) VALUES( 'modifying', 'M')
-- INSERT INTO JOB_EVENTS (NAME, CLASS) VALUES( 'user_hold', 'H')
-- INSERT INTO JOB_EVENTS (NAME, CLASS) VALUES( 'admin_hold', 'H')
-- INSERT INTO JOB_EVENTS (NAME, CLASS) VALUES( 'dep_hold', 'H')
-- INSERT INTO JOB_EVENTS (NAME, CLASS) VALUES( 'maxrun_hold', 'H')
-- INSERT INTO JOB_EVENTS (NAME, CLASS) VALUES( 'user_hold_release', 'E')
-- INSERT INTO JOB_EVENTS (NAME, CLASS) VALUES( 'admin_hold_release', 'E')
-- INSERT INTO JOB_EVENTS (NAME, CLASS) VALUES( 'dep_hold_release', 'E')
-- INSERT INTO JOB_EVENTS (NAME, CLASS) VALUES( 'maxrun_hold_release', 'E')
-- INSERT INTO JOB_EVENTS (NAME, CLASS) VALUES( 'all_holds_clear', 'S')
-- INSERT INTO JOB_EVENTS (NAME, CLASS) VALUES( 'dep_fail', 'S')
-- INSERT INTO JOB_EVENTS (NAME, CLASS) VALUES( 'failed', 'S')
-- INSERT INTO JOB_EVENTS (NAME, CLASS) VALUES( 'run_retrying', 'S')
-- INSERT INTO JOB_EVENTS (NAME, CLASS) VALUES( 'terminated', 'T')

-- CREATE TABLE JOB_COBALT_STATES ( \
--     ID INTEGER GENERATED BY DEFAULT AS IDENTITY (START WITH 1, INCREMENT BY 1, CACHE 10 ORDER) NOT NULL PRIMARY KEY, \
--     NAME VARCHAR(32) NOT NULL \
)      

--Cobalt State Machine States
-- INSERT INTO JOB_COBALT_STATES (NAME) VALUES( 'Ready' )
-- INSERT INTO JOB_COBALT_STATES (NAME) VALUES( 'Hold' )
-- INSERT INTO JOB_COBALT_STATES (NAME) VALUES( 'Prologue' )
-- INSERT INTO JOB_COBALT_STATES (NAME) VALUES( 'Release_Resources_Retry' )
-- INSERT INTO JOB_COBALT_STATES (NAME) VALUES( 'Run_Retry' )
-- INSERT INTO JOB_COBALT_STATES (NAME) VALUES( 'Running' )
-- INSERT INTO JOB_COBALT_STATES (NAME) VALUES( 'Kill_Retry' )
-- INSERT INTO JOB_COBALT_STATES (NAME) VALUES( 'Killing' )
-- INSERT INTO JOB_COBALT_STATES (NAME) VALUES( 'Preempt_Retry' )
-- INSERT INTO JOB_COBALT_STATES (NAME) VALUES( 'Preempting' )
-- INSERT INTO JOB_COBALT_STATES (NAME) VALUES( 'Preempt_Finalize_Retry' ) 
-- INSERT INTO JOB_COBALT_STATES (NAME) VALUES( 'Preempt_Epilogue' )
-- INSERT INTO JOB_COBALT_STATES (NAME) VALUES( 'Preempted' )
-- INSERT INTO JOB_COBALT_STATES (NAME) VALUES( 'Preempted_Hold' )
-- INSERT INTO JOB_COBALT_STATES (NAME) VALUES( 'Finalize_Retry' )
-- INSERT INTO JOB_COBALT_STATES (NAME) VALUES( 'Resource_Epilogue' )
-- INSERT INTO JOB_COBALT_STATES (NAME) VALUES( 'Job_Epilogue' )
-- INSERT INTO JOB_COBALT_STATES (NAME) VALUES( 'Terminal' )

-- CREATE TABLE JOB_PROG ( \
--      ID INTEGER GENERATED BY DEFAULT AS IDENTITY (START WITH 1, INCREMENT BY 1, CACHE 10 ORDER) NOT NULL PRIMARY KEY, \
--      JOB_DATA_ID INTEGER NOT NULL REFERENCES JOB_DATA("ID"), \
--      EVENT_TYPE INTEGER NOT NULL REFERENCES JOB_EVENTS("ID"), \
--      COBALT_STATE INTEGER NOT NULL REFERENCES JOB_COBALT_STATES("ID"), \
--      EXEC_USER VARCHAR(16), \
--      ENTRY_TIME TIMESTAMP NOT NULL, \
--      SCORE DECIMAL(18,3) NOT NULL \      
-- )

CREATE TABLE JOB_ATTR ( \
     ID INTEGER GENERATED BY DEFAULT AS IDENTITY (START WITH 1, INCREMENT BY 1, CACHE 10 ORDER) NOT NULL PRIMARY KEY, \
     JOB_DATA_ID INTEGER NOT NULL REFERENCES JOB_DATA("ID"), \
     KEY VARCHAR(128) NOT NULL, \
     VALUE VARCHAR (512) NOT NULL \
)

--DEP_ON_ID is really a the cobalt jobid this job depends on
--SATISFIED is supposed to be a boolean value for whether the dep is satisfied.
--Not sure what will be most useful here.  It almost seems like a dep
--satisfied timestamp would be better. If null not satisfied.
--A prog entry will be created for an update to this.

CREATE TABLE JOB_DEPS( \
     ID INTEGER GENERATED BY DEFAULT AS IDENTITY (START WITH 1, INCREMENT BY 1, CACHE 10 ORDER) NOT NULL PRIMARY KEY, \
     JOB_DATA_ID INTEGER NOT NULL REFERENCES JOB_DATA("ID"), \
     DEP_ON_ID INTEGER NOT NULL, \
     SATISFIED INTEGER NOT NULL \
)