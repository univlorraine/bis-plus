-- Script d'initialisation de la base de données PostgreSQL
-- Crée le schéma splus et les schémas Blue/Green avec les permissions nécessaires

-- Connexion à la base business_data


-- ============================================================================
-- SCHÉMA PRINCIPAL (vues publiques)
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS splus;

-- ============================================================================
-- SCHÉMAS BLUE/GREEN (tables de données)
-- ============================================================================
-- Architecture Blue/Green pour imports atomiques avec rollback
-- - splus_blue  : Tables blue
-- - splus_green : Tables green (identiques)
-- - splus       : Vues pointant vers le schéma actif
CREATE SCHEMA IF NOT EXISTS splus_blue;
CREATE SCHEMA IF NOT EXISTS splus_green;

-- ============================================================================
-- PERMISSIONS DATAUSER
-- ============================================================================

-- Permissions sur le schéma principal (vues)
GRANT ALL PRIVILEGES ON SCHEMA splus TO datauser;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA splus TO datauser;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA splus TO datauser;

-- Permissions sur splus_blue
GRANT ALL PRIVILEGES ON SCHEMA splus_blue TO datauser;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA splus_blue TO datauser;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA splus_blue TO datauser;

-- Permissions sur splus_green
GRANT ALL PRIVILEGES ON SCHEMA splus_green TO datauser;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA splus_green TO datauser;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA splus_green TO datauser;

-- Permissions par défaut pour les futures tables
ALTER DEFAULT PRIVILEGES IN SCHEMA splus
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO datauser;
ALTER DEFAULT PRIVILEGES IN SCHEMA splus_blue
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO datauser;
ALTER DEFAULT PRIVILEGES IN SCHEMA splus_green
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO datauser;

-- Définir le search_path par défaut (inclut les schémas blue/green)
ALTER ROLE datauser SET search_path TO splus, splus_blue, splus_green, public;

-- ============================================================================
-- SCHÉMA ADMIN (état centralisé - remplace variables Airflow d'état)
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS splus_admin;

-- ============================================================================
-- TABLE D'ÉTAT CENTRALISÉE
-- Remplace les variables Airflow :
--   - amue_last_finish_timestamp
--   - amue_last_successful_run
--   - amue_bluegreen_state (tous les champs)
-- ============================================================================
CREATE TABLE IF NOT EXISTS splus_admin.amue_state (
    id                    INTEGER PRIMARY KEY DEFAULT 1,

    -- Timestamps de synchro AMUE (polling)
    last_finish_timestamp TIMESTAMPTZ,
    last_successful_run   TIMESTAMPTZ,
    last_report_start     TIMESTAMPTZ,

    -- État blue/green
    active_schema         VARCHAR(20),
    last_switch_timestamp TIMESTAMPTZ,
    last_sync_timestamp   TIMESTAMPTZ,
    import_in_progress    BOOLEAN NOT NULL DEFAULT FALSE,
    import_started_at     TIMESTAMPTZ,
    import_correlation_id VARCHAR(255),

    -- Audit
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT single_row CHECK (id = 1)
);

-- Ligne unique (si elle n'existe pas encore)
INSERT INTO splus_admin.amue_state (id)
VALUES (1)
ON CONFLICT DO NOTHING;

-- ============================================================================
-- TABLE DE CONFIGURATION DES TABLES À IMPORTER
-- Remplace la variable Airflow 'amue_tables_to_import'
-- ============================================================================
CREATE TABLE IF NOT EXISTS splus_admin.amue_tables (
    table_name      VARCHAR(100) PRIMARY KEY,
    enabled         BOOLEAN      NOT NULL DEFAULT TRUE,
    primary_key     TEXT         NOT NULL DEFAULT '',
    delta           TEXT         NOT NULL DEFAULT '',
    fingerprint_api TEXT         NOT NULL DEFAULT '',
    fingerprint_ul  TEXT         NOT NULL DEFAULT '',
    ecc_query       TEXT,          -- NULL = table AMUE pure ; non-NULL = requête Oracle ECC
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Config initiale (ON CONFLICT DO NOTHING = idempotent au redémarrage)
INSERT INTO splus_admin.amue_tables (table_name, enabled, primary_key, delta) VALUES
    ('AGR_AGRS',  true,  'AGR_NAME,CHILD_AGR',                                             ''),
    ('AGR_USERS', true,  'AGR_NAME,UNAME,FROM_DAT,TO_DAT',                                 ''),
    ('BKPF',      true,  'BUKRS,BELNR,GJAHR',                                              'cpudt'),
    ('CEPC',      true,  'PRCTR,DATBI,KOKRS',                                              ''),
    ('COBK',      true,  'KOKRS,BELNR',                                                    ''),
    ('COVP',      true,  'KOKRS,BELNR,BUZEI',                                              ''),
    ('CSKS',      true,  'KOKRS,KOSTL,DATBI',                                              ''),
    ('CSKU',      false, 'SPRAS,KTOPL,KSTAR',                                              ''),
    ('FM01H',     true,  'FIKRS,GJAHR',                                                    ''),
    ('FMBDT',     true,  'RLDNR,RRCTY,RVERS,RYEAR,ROBJNR,COBJNR,SOBJNR,RTCUR,DRCRK,RPMAX', ''),
    ('FMBH',      true,  'DOCNR,FM_AREA,DOCYEAR',                                          'crtdate'),
    ('FMBL',      true,  'FM_AREA,DOCYEAR,DOCNR,DOCLN,RPMAX',                              ''),
    ('FMFCTR',    true,  'FIKRS,FICTR,DATBIS',                                             ''),
    ('FMIFHD',    false, 'FMBELNR,FIKRS',                                                  ''),
    ('FMIFIIT',   true,  'FIKRS,BTART,RLDNR,GJAHR,STUNR',                                  ''),
    ('FMFINCODE', true,  'FIKRS,FINCODE',                                                  ''),
    ('FMHISV',    true,  'FIKRS,HIVARNT,FISTL',                                            ''),
    ('FMMEASURE', true,  'FMAREA,MEASURE',                                                 ''),
    ('KBLK',      true,  'BELNR',                                                          ''),
    ('KNA1',      true,  'KUNNR',                                                          ''),
    ('LFA1',      true,  'LIFNR',                                                          ''),
    ('LFB1',      true,  'LIFNR,BUKRS',                                                    ''),
    ('PA0000',    false, 'PERNR,SUBTY,OBJPS,SPRPS,ENDDA,BEGDA,SEQNR',                      ''),
    ('PA0001',    true,  'PERNR,SUBTY,OBJPS,SPRPS,ENDDA,BEGDA,SEQNR',                      ''),
    ('PA0002',    true,  'PERNR,SUBTY,OBJPS,SPRPS,ENDDA,BEGDA,SEQNR',                      ''),
    ('PA0105',    true,  'PERNR,SUBTY,OBJPS,SPRPS,ENDDA,BEGDA,SEQNR',                      ''),
    ('PRPS',      false, 'POSID',                                                          ''),
    ('PRPS_RH',   false, 'POSID',                                                          ''),
    ('SOOD',      false, '',                                                               ''),
    ('SRGBTBREL', true,  'BRELGUID',                                                       ''),
    ('TFKB',      true,  'FKBER',                                                          ''),
    ('TVARVC',    false, 'NAME,TYPE,NUMB',                                                 ''),
    ('USR02',     true,  'BNAME',                                                          ''),
    ('UST04',     false, 'BNAME,PROFILE',                                                  ''),
    ('UST10S',    false, 'PROFN,AKTPS,OBJCT,AUTH',                                         ''),
    ('UST12',     true,  'OBJCT,AUTH,AKTPS,FIELD,VON,BIS',                                 ''),
    ('ZSIFACTAFM',false, 'FKBER',                                                          '')
ON CONFLICT (table_name) DO NOTHING;

-- Lignes ECC (noms minuscules, distincts des tables AMUE en majuscules — PKs VARCHAR sensibles à la casse)
INSERT INTO splus_admin.amue_tables (table_name, enabled, primary_key, delta, ecc_query) VALUES
    ('lfa1',   true, 'lifnr',                                        '', $ecc$SELECT lfa1.lifnr,lfa1.land1,lfa1.name1,lfa1.name2,lfa1.name3,lfa1.name4,lfa1.ort01,lfa1.ort02,lfa1.pfach,lfa1.pstl2,lfa1.pstlz,lfa1.regio,lfa1.sortl,lfa1.stras,lfa1.adrnr,lfa1.mcod1,lfa1.mcod2,lfa1.mcod3,lfa1.anred,lfa1.bahns,lfa1.bbbnr,lfa1.bbsnr,lfa1.begru,lfa1.brsch,lfa1.bubkz,lfa1.datlt,lfa1.dtams,lfa1.dtaws,lfa1.erdat,lfa1.ernam,lfa1.esrnr,lfa1.konzs,lfa1.ktokk,lfa1.kunnr,lfa1.lnrza,Decode(lfa1.loevm,'X', 1,0) AS LOEVM,Decode(lfa1.sperr,'X', 1,0) AS SPERR,Decode(lfa1.sperm,'X', 1,0) AS SPERM,lfa1.spras,lfa1.stcd1,lfa1.stcd2,lfa1.stkza,Decode(lfa1.stkzu,'X', 1,0) AS STKZU,lfa1.telbx,lfa1.telf1,lfa1.telf2,lfa1.telfx,lfa1.teltx,lfa1.telx1,Decode(lfa1.xcpdk,'X', 1,0) AS XCPDK,Decode(lfa1.xzemp,'X', 1,0) AS XZEMP,lfa1.vbund,lfa1.fiskn,lfa1.stceg,lfa1.stkzn,lfa1.sperq,lfa1.gbort,lfa1.gbdat,lfa1.sexkz,lfa1.kraus,lfa1.revdb,lfa1.qssys,lfa1.ktock,lfa1.pfort,lfa1.werks,Decode(lfa1.ltsna,'X', 1,0) AS LTSNA,Decode(lfa1.werkr,'X', 1,0) AS WERKR,lfa1.plkal,lfa1.duefl,lfa1.txjcd,Decode(lfa1.sperz,'X', 1,0) AS SPERZ,lfa1.scacd,lfa1.sfrgr,lfa1.lzone,Decode(lfa1.xlfza,'X', 1,0) AS XLFZA,lfa1.dlgrp,lfa1.fityp,lfa1.stcdt,Decode(lfa1.regss,'X', 1,0) AS REGSS,lfa1.actss,lfa1.stcd3,lfa1.stcd4,lfa1.stcd5,lfa1.stcd6,Decode(lfa1.ipisp,'X', 1,0) AS IPISP,lfa1.taxbs,lfa1.profs,lfa1.stgdl,lfa1.emnfr,lfa1.lfurl,lfa1.j_1kfrepre,lfa1.j_1kftbus,lfa1.j_1kftind,lfa1.confs,lfa1.updat,lfa1.uptim,Decode(lfa1.nodel,'X', 1,0) AS NODEL,lfa1.qssysdat,lfa1.podkzb,lfa1.fisku,lfa1.stenr,lfa1.carrier_conf,' ' AS CVP_XBLCK,0 AS WEORA,lfa1.rgdate,lfa1.rnedate,' ' AS PAYTRSN,' ' AS LFA1_EEW_SUPP,lfa1.alc,lfa1.pmt_office,lfa1.psofg,lfa1.psois,lfa1.pson1,lfa1.pson2,lfa1.pson3,lfa1.psovn,lfa1.psotl,lfa1.psohs,lfa1.psost,' ' AS BORGR_DATUN,0 AS BORGR_YEAUN,' ' AS ADDR2_STREET,' ' AS ADDR2_HOUSE_NUM,' ' AS ADDR2_POST,' ' AS ADDR2_CITY,' ' AS ADDR2_COUNTRY,' ' AS CATEG,' ' AS PARTNER_NAME,' ' AS PARTNER_UTR,' ' AS STATUS,' ' AS VFNUM,' ' AS VFNID,' ' AS CRN,' ' AS FR_OCCUPATION,' ' AS AEDAT,' ' AS J_1IPANVALDT,' ' AS DVALSS,lfa1.transport_chain,lfa1.staging_time,lfa1.scheduling_type,Decode(lfa1.submi_relevant, 'X', 1, 0) AS SUBMI_RELEVANT FROM   sapsr3.lfa1 WHERE  lfa1.mandt = 330 AND lfa1.loevm = ' '$ecc$),
    ('lfb1',   true, 'lifnr,bukrs',                                  '', $ecc$SELECT lfb1.lifnr,lfb1.bukrs,lfb1.pernr,lfb1.erdat,lfb1.ernam,Decode(lfb1.sperr, 'X', 1,0)  AS SPERR,Decode(lfb1.loevm, 'X', 1,0)  AS LOEVM,lfb1.zuawa,lfb1.akont,lfb1.begru,lfb1.vzskz,lfb1.zwels,Decode(lfb1.xverr, 'X', 1,0)  AS XVERR,lfb1.zahls,lfb1.zterm,lfb1.eikto,lfb1.zsabe,lfb1.kverm,lfb1.fdgrv,lfb1.busab,lfb1.lnrze,lfb1.lnrzb,lfb1.zindt,lfb1.zinrt,lfb1.datlz,Decode(lfb1.xdezv, 'X', 1,0)  AS XDEZV,lfb1.webtr,lfb1.kultg,Decode(lfb1.reprf, 'X', 1,0)  AS REPRF,lfb1.togru,lfb1.hbkid,Decode(lfb1.xpore, 'X', 1,0)  AS XPORE,lfb1.qsznr,lfb1.qszdt,lfb1.qsskz,lfb1.blnkz,lfb1.mindk,lfb1.altkn,lfb1.zgrup,lfb1.mgrup,lfb1.uzawe,lfb1.qsrec,lfb1.qsbgr,lfb1.qland,Decode(lfb1.xedip, 'X', 1,0)  AS XEDIP,lfb1.frgrp,lfb1.togrr,lfb1.tlfxs,lfb1.intad,Decode(lfb1.xlfzb, 'X', 1,0)  AS XLFZB,lfb1.guzte,lfb1.gricd,lfb1.gridt,lfb1.xausz,lfb1.cerdt,lfb1.confs,lfb1.updat,lfb1.uptim,Decode(lfb1.nodel, 'X', 1,0)  AS NODEL,lfb1.tlfns,lfb1.avsnd,lfb1.ad_hash,' 'AS CVP_XBLCK_B,lfb1.ciiucode,' 'AS PAYMENTCLEARINGGRPID,' 'AS PAYTRSN,' 'LFB1_EEW_CC,' 'AS ZBOKD,' 'AS ZQSZDT,' 'AS ZMINDAT,Decode(lfb1.gmvkzk, 'X', 1,0) AS GMVKZK,' 'AS BRSCH,' 'AS WAERS,' 'AS US_REC_COUNTRY,' 'AS US_REC_DOB,' 'AS US_W8_RECVDATE,' 'AS US_W9_RECVDATE,lfb1.prepay_relevant,lfb1.assign_test FROM   sapsr3.lfb1 WHERE  lfb1.mandt = 330 AND lfb1.loevm = ' '$ecc$),
    ('pa0001', true, 'pernr,subty,objps,sprps,endda,begda,seqnr',    '', $ecc$SELECT pernr,subty,objps,sprps,endda,begda,seqnr,aedtm,uname,histo,Decode(itxex,'X',1,0) itxex,Decode(refex,'X',1,0) refex,Decode(ordex,'X',1,0) ordex,itbld,preas,flag1,flag2,flag3,flag4,rese1,rese2,grpvl,bukrs,werks,persg,persk,vdsk1,gsber,btrtl,juper,abkrs,ansvh,kostl,orgeh,plans,stell,mstbr,sacha,sachp,sachz,sname,ename,otype,sbmod,kokrs,fistl,geber,fkber,grant_nbr,sgmnt,budget_pd FROM   sapsr3.pa0001 WHERE  pa0001.mandt = '330' AND pa0001.endda >= '20260224'$ecc$),
    ('pa0002', true, 'pernr,subty,objps,sprps,endda,begda,seqnr',    '', $ecc$SELECT pernr, subty,objps,sprps,endda,begda,seqnr,aedtm,uname,histo,Decode(itxex, 'X', 1,0) itxex,Decode(refex, 'X', 1, 0) refex,Decode(ordex, 'X', 1,0) ordex,itbld,preas,flag1,flag2,flag3,flag4,rese1,rese2,grpvl,inits,nachn,name2,nach2,vorna,cname,titel,titl2,namzu,vorsw,vors2,rufnm,midnm,knznm,anred,gesch,gbdat,gblnd,gbdep,gbort,natio,nati2,nati3,sprsl,konfe,famst,famdt,anzkd,nacon,permo,perid,gbpas,fnamk,lnamk,fnamr,lnamr,nabik,nabir,nickk,nickr,gbjhr,gbmon,gbtag,nchmc,vnamc,namz2,'' AS gender_si,zzid_chorus,lifnr FROM   sapsr3.pa0002 WHERE  pa0002.mandt = '330' AND pa0002.endda >= '20260224'$ecc$)
ON CONFLICT (table_name) DO NOTHING;

-- ============================================================================
-- PERMISSIONS SIFACPLUS SUR SPLUS_ADMIN
-- ============================================================================
GRANT SELECT, INSERT, UPDATE ON splus_admin.amue_tables TO sifacplus;

GRANT ALL PRIVILEGES ON SCHEMA splus_admin TO sifacplus;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA splus_admin TO sifacplus;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA splus_admin TO sifacplus;
ALTER DEFAULT PRIVILEGES IN SCHEMA splus_admin
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO sifacplus;

-- ============================================================================
-- LOG DE CONFIRMATION
-- ============================================================================
SELECT 'Database initialized successfully' AS status;
SELECT 'Schema splus created (views)' AS info;
SELECT 'Schema splus_blue created (blue tables)' AS info;
SELECT 'Schema splus_green created (green tables)' AS info;
SELECT 'Blue/Green architecture ready' AS info;
SELECT 'Permissions granted to datauser' AS info;
SELECT 'Schema splus_admin created' AS info;
SELECT 'Table splus_admin.amue_state ready' AS info;
SELECT 'Table splus_admin.amue_tables ready' AS info;
